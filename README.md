# Alaffia Software Engineering Challenge
### Author: Zhongyuan Li
### Date: 2022/08/27

## Overview
This is the code for Alaffia API challenge.
In this repo, a pipeline is created to receive POST request, and then retrieve exchanges for a coin, save all the result to a postgres DB.


## Code Files
- app.py: API file running with FLASK, which receives POST request
- pipeline.py: Business logic of this pipeline, including 4 functions: ingest_data(), extract(), transform(), load()
- data_model.py: Define the data model (Columns and types)

## Configuration Files
- Dockerfile: Make image for container
- docker-compose.yml: Define services and their configurations (pythonapp and DB)
- requirement.txt: List all dependencies of this project (python libs)

## How to run it?

### Prerequisites
- Make sure you have Docker installed in your computer
- Make sure you have Git installed in your computer

### Set local environment for pipeline
1. Create new project folder
```
mkdir testing_zhongyuan_li
```
```
cd testing_zhongyuan_li 
```
2. Clone code to your local
```
git clone https://github.com/ttddli/alaffia_challenge.git
```
```
cd alaffia_challenge 
```

3. Run your docker app
4. Run the container (with image)

```
docker compose up
```

### Schedule pipeline
To send a POST request to the API, the blow command needs to be sent to API:
```
curl --location --request POST 'http://localhost:80/coins' \
--header 'Content-Type: application/json' \
--data-raw '{"coins":["bitcoin", "pascalcoin"]}'
```
Job scheduler:
1. Airflow (BashOperator)
2. crontab 
```
* * * * * curl --location --request POST 'http://localhost:80/coins' --header 'Content-Type: application/json' --data-raw '{"coins":["bitcoin", "pascalcoin"]}'
```
3. etc.

### SQL: Check the result:
1. Run Postgres
````
docker exec -it db psql -U postgres
````

2. List all tables
```
\dt
```

3. For each exchange, get the count of distinct coins
```
select UNNEST(string_to_array(exchanges, ',')) as exchange, count(distinct id) as cnt from coin group by 1;
```

4. For each exchange, get the median task_run of those coins
```
select UNNEST(string_to_array(exchanges, ',')) as exchange, (max(task_run) + min(task_run))/2 as median_run from coin group by 1;
```

5. Show all exchanges containing the letter 'x'
```
select * from (select UNNEST(string_to_array(exchanges, ',')) as exchange from coin) as temp where exchange like '%x%'
```
