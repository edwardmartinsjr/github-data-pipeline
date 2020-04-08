from sqlalchemy import create_engine
import pandas as pd
import os

# Postgres
db_user = os.environ['POSTGRES_USER']
db_password = os.environ['POSTGRES_PASSWORD']
db_host = os.environ['POSTGRES_HOST']
db_port = os.environ['POSTGRES_PORT']
db_name = os.environ['POSTGRES_NAME']

# Set Postgres
engine = create_engine('postgresql://%s:%s@%s:%s/%s' % (db_user, db_password, db_host, db_port, db_name))

def save_data(df, db_name):
    try:
        # Stores the data into DB (append mode)
        df.to_sql(con=engine, name=db_name, if_exists='append', index=False)
        return True
    except BaseException as e:
        print(e)
        return False

def get_last_organization_id():
    id = 0
    try:
        # Read last organization id on DB
        values = engine.execute('SELECT id FROM organizations ORDER BY 1 DESC LIMIT 1').fetchall()
        for value in values:
            id = int(value['id'])

        return id
    except BaseException as e:
        print(e)       
        return id
