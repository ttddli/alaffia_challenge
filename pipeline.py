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
        return None

    try:
        tickers = data['tickers']
        identifiers = [x['market']['identifier'] for x in tickers]
        return identifiers
    except Exception as e:
        print(url + '\n' + str(data))
        raise Exception

def transform(id, identifiers, task_run):
    exchanges = ','.join(list(set(identifiers)))
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
                print("Ignore duplicated records")

    return new_records

