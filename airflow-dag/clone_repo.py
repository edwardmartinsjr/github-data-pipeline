from __future__ import print_function

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from pprint import pprint
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import git
import os

# Postgres
db_user = 'postgres'
db_password = 'postgres'
db_host = 'localhost'
db_port = '5432'
db_name = 'analytics'

# Set Postgres
engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (db_user, db_password, db_host, db_port, db_name))

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id='clone_repo',
    max_active_runs=1,
    default_args=args,
    schedule_interval=timedelta(minutes=1),
)

def get_all_repos(ds, **kwargs):
    repos = []
    try:
        # Get unprocessed organization from DB
        values = engine.execute('''
            SELECT git_url FROM repos 
            WHERE archived = False
            AND forks_count > 0;
        ''').fetchall()
        for value in values:
            repos.append(value['git_url'])
    except BaseException as e:
        print(e)

    return repos

def clone_repos(ds, **kwargs):
    repos = kwargs['task_instance'].xcom_pull(task_ids='get_all_repos')

    # Clone repo
    for row in repos:
        try:
            git.Git(os.getcwd() + "/repos").clone(row)
        except BaseException as e:
            print(e)

get_all_repos = PythonOperator(
    task_id='get_all_repos',
    provide_context=True,
    python_callable=get_all_repos,
    dag=dag,
)

clone_repos = PythonOperator(
    task_id='clone_repos',
    provide_context=True,
    python_callable=clone_repos,
    dag=dag,
)

get_all_repos >> clone_repos
