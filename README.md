## DE Assignment - Github ETL Process:

This assignment consists of creating a data pipeline for:
- Extract GitHub organizations 
- Extract organizations repositories
- Stores the data in a Postgres DB
- Clone stored organizations repositories


# Architecture

Adhock version:
![Architecture](architecture.png?raw=true)
![](graph-view.png?raw=true)

Apache Airflow version:
![Architecture](architecture-airflow.png?raw=true)
![](graph-airflow-view.png?raw=true)

A preliminary version of repository cloning:

![](graph-airflow-clone-repo-view.png?raw=true)



## Configuration

There are some env variables you must set to the application configs:

NAME                      | DESCRIPTION
--------------------------|------------
POSTGRES_USER           | Postgres user
POSTGRES_PASSWORD       | Postgres password
POSTGRES_HOST           | Postgres host
POSTGRES_PORT           | Postgres port
POSTGRES_NAME           | Postgres database name


## Run
Docking postgres:
`https://hub.docker.com/_/postgres/`

Creating DB:
`db.sql`

Install application dependencies:
`pip install -r requirements.txt`

Running on adhoc mode:
```
export POSTGRES_USER=XXX
export POSTGRES_PASSWORD=XXX
export POSTGRES_HOST=XXX
export POSTGRES_PORT=XXX
export POSTGRES_NAME=XXX
```
`python main.py`

Running on Apache Airflow:

Add DAG `airflow-dag\github_to_sql.py` to DAGs folder `/Users/username/airflow/dags` 

Add DAG `airflow-dag\clone_repo.py` to DAGs folder `/Users/username/airflow/dags` 


## Python version:
Python 3.6.8 :: Anaconda, Inc.

