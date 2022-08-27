import requests
from data_model import db, Coin
from datetime import datetime
from sqlalchemy import exc
import logging

def extract(coin):
    response = requests.get(f'https://api.coingecko.com/api/v3/coins/{coin}/tickers')
    data = response.json()
    if "error" in data and data['error'] == "Could not find coin with the given id":
        return None

    try:
        tickers = data['tickers']
        identifiers = [x['market']['identifier'] for x in tickers]
    except Exception as e:
        logging.error(f'Failed to parse API result due to {e}')

    return identifiers

def transform(id, identifiers, task_run):
    exchanges = str(list(set(identifiers)))
    return id, exchanges, task_run

def load(id, exchanges, taskRun):
    db.session.add(Coin(id, exchanges, taskRun))
    db.session.commit()

def ingest_data(body):
    ids = body['coins']
    task_run = int(datetime.now().timestamp() * 1000000)

    new_records = 0
    for id in ids:
        identifiers = extract(id)

        if identifiers:
            # Transform data based on the requirement
            id, exchanges, taskRun = transform(id, identifiers, task_run)

            # Load data into destination
            try:
                load(id, exchanges, taskRun)
                new_records += 1
            except exc.SQLAlchemyError as e:
                logging.warning("Ignore duplicated records")

    return new_records

