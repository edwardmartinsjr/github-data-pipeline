from __future__ import print_function

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from pprint import pprint
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
from dateutil import parser
import requests
import time

get_organizations_url = 'https://api.github.com/organizations?since='
get_repos_url = 'https://api.github.com/orgs/%(login)s/repos'

headers = {'Content-Type': 'application/json'}

# https://developer.github.com/v3/#rate-limiting
# For unauthenticated requests, the rate limit allows for up to 60 requests per hour.
task_delay = 60

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
    dag_id='github_to_sql',
    max_active_runs=1,
    default_args=args,
    schedule_interval=timedelta(minutes=1),
)

def get_organizations(ds, **kwargs):
    organization_id = kwargs['task_instance'].xcom_pull(task_ids='get_last_organization_id')
    
    organizations_columns = ['id', 'login']
    organizations = pd.DataFrame(columns = organizations_columns)    

    # Extract the organizations JSON string to pandas object (since pagination).
    resp = requests.get(get_organizations_url + str(organization_id), headers = headers)

    if resp.status_code == 403:
        print('API rate limit exceeded')
        pass
    elif resp.status_code == 200:
        # Read data
        organizations=pd.read_json(resp.content)[organizations_columns]
    else:
        print(resp.content)
        pass

    # Transform data
    organizations['id'] = pd.to_numeric(organizations['id'])
    organizations['login'] = organizations['login'].astype('str')

    return organizations

def get_repos(ds, **kwargs):
    organizations = kwargs['task_instance'].xcom_pull(task_ids='get_unprocessed_organizations')
    
    repos_columns = ['git_url', 'language', 'archived', 'forks_count', 'open_issues_count', 'watchers_count']
    repos = pd.DataFrame(columns = repos_columns)
    
    # Get repos from unprocessed organizations
    for row in organizations:
        resp = requests.get(get_repos_url % {'login': row}, headers = headers)
        
        if resp.status_code == 403:
            print('API rate limit exceeded')
            break
        elif resp.status_code == 200:
            # Read data
            if len(resp.json()) > 0:
                print('Getting %s repos' %(row))
                repos = repos.append(pd.read_json(resp.content)[repos_columns])
        else:
            print(resp.content)
            break

        # Task delay
        print('Task delay:', str(task_delay))
        time.sleep(task_delay)

    # Transform data
    repos['git_url'] = repos['git_url'].astype('str')
    repos['language'] = repos['language'].astype('str')
    repos['forks_count'] = pd.to_numeric(repos['forks_count'])
    repos['open_issues_count'] = pd.to_numeric(repos['open_issues_count'])
    repos['watchers_count'] = pd.to_numeric(repos['watchers_count'])
    repos['archived'] = repos['archived'].astype('bool')

    return repos

def save_organizations(ds, **kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='get_organizations')
    try:
        # Stores the data into DB (append mode)
        df.to_sql(con=engine, name='organizations', if_exists='append', index=False)
        return True
    except BaseException as e:
        print(e)
        return False

def save_repos(ds, **kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='get_repos')
    try:
        # Stores the data into DB (append mode)
        df.to_sql(con=engine, name='repos', if_exists='append', index=False)
        return True
    except BaseException as e:
        print(e)
        return False

def get_unprocessed_organizations(ds, **kwargs):
    organizations = []
    try:
        # Get unprocessed organization from DB
        values = engine.execute('''
        SELECT login FROM organizations WHERE login NOT IN
            (SELECT DISTINCT SUBSTRING(git_url FROM 18 FOR POSITION('/' IN SUBSTRING(git_url, 18)) -1) AS git_url 
                FROM repos GROUP BY git_url)
        ''').fetchall()
        for value in values:
            organizations.append(value['login'])
    except BaseException as e:
        print(e)      

    return organizations

def get_last_organization_id(ds, **kwargs):
    id = 0
    try:
        # Read last organization id on DB
        values = engine.execute('SELECT id FROM organizations ORDER BY 1 DESC LIMIT 1').fetchall()
        for value in values:
            id = int(value['id'])
    except BaseException as e:
        print(e)      

    return id    

get_last_organization_id = PythonOperator(
    task_id='get_last_organization_id',
    provide_context=True,
    python_callable=get_last_organization_id,
    dag=dag,
)

get_organizations = PythonOperator(
    task_id='get_organizations',
    provide_context=True,
    python_callable=get_organizations,
    dag=dag,
)

save_organizations = PythonOperator(
    task_id='save_organizations',
    provide_context=True,
    python_callable=save_organizations,
    dag=dag,
)

get_unprocessed_organizations = PythonOperator(
    task_id='get_unprocessed_organizations',
    provide_context=True,
    python_callable=get_unprocessed_organizations,
    dag=dag,
)

save_repos = PythonOperator(
    task_id='save_repos',
    provide_context=True,
    python_callable=save_repos,
    dag=dag,
)

get_repos = PythonOperator(
    task_id='get_repos',
    provide_context=True,
    python_callable=get_repos,
    dag=dag,
)

get_last_organization_id >> get_organizations >> save_organizations >> get_unprocessed_organizations >> get_repos >> save_repos

