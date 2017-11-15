import uuid
import json
import logging
import requests
import threading
from decimal import Decimal
from time import sleep

from flask import Flask
from flask import request
from flask import Response


app = Flask(__name__)

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


@app.route('/provide-list', methods=['POST'])
def provideList():
    """
    Makes a new address for user to deposit coins in, and
    stores it as a k-v pair with a listOfAddresses
    we should be mixing into.
    :param listOfAddresses: List of bitcoin wallets to mix into
    :return: New Wallet Address for user to put money in to
        start mixing
    """
    listOfAddresses = request.json['listOfAddresses']
    # Make a new address. In BTC would use pub-priv keypair
    newAddress = uuid.uuid4().hex

    if not listOfAddresses:
        # TODO write an exception handler class
        errmsg = {'error': 'Provide listOfAddresses'}
        return Response(
            json.dumps(errmsg),
            status=400,
            mimetype='application/json'
        )

    # Store new address with provided addresses so we can poll
    # TODO make this persistent
    ACCOUNTS[newAddress] = listOfAddresses

    # TODO A better polling scheme would use celery beat
    threading.Thread(target=pollAddress, args=(newAddress,)).start()

    log.info('Provided list of wallets `{}`, returning throwaway wallet `{}`'
             ''.format(listOfAddresses, newAddress))
    # For user to know where to deposit
    return newAddress


if __name__ == '__main__':
    app.run()
