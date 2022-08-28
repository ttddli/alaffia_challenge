import requests
from data_model import db, Coin
from datetime import datetime
from sqlalchemy import exc
import logging



def extract(coin):
    url = f'https://api.coingecko.com/api/v3/coins/{coin}/tickers'
    response = requests.get(url)
    data = response.json()

    if "error" in data and data['error'] == "Could not find coin with the given id":
        return None, 404

    if 'tickers' in data:
        tickers = data['tickers']
        identifiers = [x['market']['identifier'] for x in tickers]
        return identifiers, 200

    if 'status' in data and 'error_code' in data['status']:
        return None, int(data['status']['error_code'])

    return None, None


def transform(id, identifiers, task_run):
    exchanges = ','.join(list(set(identifiers)))
    return id, exchanges, task_run

def load(id, exchanges, taskRun):
    db.session.add(Coin(id, exchanges, taskRun))
    db.session.commit()

def ingest_data(body):
    ids = body['coins']

    # How to get task_run: Use the timestamp as the task_run.
    # This number can be easily collected, and also show the sequence of each request without duplicates.
    task_run = int(datetime.now().timestamp() * 1000000)

    status = []
    for id in ids:
        identifiers, status_code = extract(id)

        if identifiers:
            # Transform data based on the requirement
            id, exchanges, taskRun = transform(id, identifiers, task_run)

            # Load data into destination
            try:
                load(id, exchanges, taskRun)
            except exc.SQLAlchemyError as e:
                print("Ignore duplicated records")

        status.append(status_code)

    return status

