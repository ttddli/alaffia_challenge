import csv
import sys
import requests
import time
import logging

def get_coins(filename):
    """
    Use generator to get the coins from file
    """
    with open(filename, "r") as csvfile:
        datareader = csv.DictReader(csvfile)
        for row in datareader:
            yield row["coin"]

def call_api(lst):
    """
    Make URL and call API
    """
    API_ENDPOINT = "http://localhost:80/coins"
    headers = {'Content-Type': 'application/json'}
    coin_job = {'coins': lst}

    response = requests.post(url=API_ENDPOINT, json=coin_job, headers=headers)
    status = response.status_code
    print (f"Status: {str(status)} ")

def schedule_job(request_limit, task_limit, delay=400):
    """
    Schedule API call by using request_list, task_limit, and delay
    """
    print (f"request_limit: {str(request_limit)}, task_limit: {str(task_limit)}, delay: {str(delay)}")
    group = []
    groups = 0
    coins = get_coins(coin_file)
    for index, coin in enumerate(coins):
        if groups >= int(request_limit):
            break

        if index % int(task_limit) == 0 :
            if group:
                call_api(group)
                groups += 1
                time.sleep(int(delay)/1000)
            group = []
        group.append(coin)


if __name__ == '__main__':
    coin_file = "./coin_file.csv"
    schedule_job(*sys.argv[1:])
