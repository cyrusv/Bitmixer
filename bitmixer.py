import uuid
import logging
import requests
import threading
from decimal import Decimal
from time import sleep

from flask import Flask, request
from flask_restplus import Api, Resource, abort, reqparse


app = Flask(__name__)
api = Api(
    app, version='0.0', title='Bitmixer',
    description='A Mixer for Jobcoins', default='Mixer',
)

log = logging.getLogger('Bitmixer')


ACCOUNTS = {}

TXNS_URL = 'http://jobcoin.gemini.com/sustained/api/transactions'
ADDR_INFO_URL = 'http://jobcoin.gemini.com/sustained/api/addresses/'
CORE_WALLET_ADDR = 'CORE_WALLET_ADDR'


def transfer(fromAddress, toAddress, amount):
    return requests.post(TXNS_URL, {
        'fromAddress': fromAddress,
        'toAddress': toAddress,
        'amount': amount,
    })


def pollAddress(address):
    while True:
        url = ADDR_INFO_URL + address
        resp = requests.get(url).json()
        incoming = [txn for txn in resp['transactions']
                    if txn['toAddress'] == address]
        if incoming:
            # Pick the earliest to keep it simple and robust
            firstIncoming = min(incoming, key=lambda t: t['timestamp'])

            # Transfer that amount from this temp addr to the core wallet
            amt = Decimal(firstIncoming['amount'])
            resp = transfer(address, CORE_WALLET_ADDR, amt)

            # When that finishes, Core Wallet will transfer to the user's addrs
            outputAddresses = ACCOUNTS[address]

            # TODO Split randomly instead of evenly
            subAmt = amt/len(outputAddresses)
            for addr in outputAddresses:
                resp = transfer(CORE_WALLET_ADDR, addr, str(subAmt))

            return

        sleep(5)


parser = reqparse.RequestParser()
parser.add_argument('listOfAddresses', required=True,
                    help='Provide list of Addresses to mix to', location='json')


@api.route('/provide-list/', endpoint='provide-list')
@api.response(400, 'Invalid input.')
class Mixer(Resource):

    @api.expect(parser)
    def post(self):
        """
        Accepts list of addresses to mix into, returns a temp wallet

        Makes a new address for user to deposit coins in, and
        stores it as a k-v pair with a listOfAddresses
        we should be mixing into.
        """
        listOfAddresses = request.json

        # Make a new address. In BTC would use pub-priv keypair
        newAddress = uuid.uuid4().hex
        # Validate
        if not listOfAddresses or not isinstance(listOfAddresses, list):
            abort(400, 'Illegal value: listOfAddresses')

        # Store new address with provided addresses so we can poll
        # TODO make this persistent
        ACCOUNTS[newAddress] = listOfAddresses

        # TODO A better polling scheme would use celery beat
        threading.Thread(target=pollAddress, args=(newAddress,)).start()

        log.info('Provided list of wallets `{}`, returning throwaway wallet '
                 '`{}`'.format(listOfAddresses, newAddress))
        # For user to know where to deposit
        return newAddress


if __name__ == '__main__':
    app.run(debug=True)
