from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator #DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from datetime import datetime
import os
import numpy as np
import pandas as pd
import logging
import json
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine

DB_HOST = Variable.get(key="DB_HOST", default_var="")
DB_PORT = Variable.get(key="DB_PORT", default_var="")
DB_USERNAME = Variable.get(key="DB_USERNAME", default_var="")
DB_PASSWORD = Variable.get(key="DB_PASSWORD", default_var="")
DB_NAME = Variable.get(key="DB_NAME", default_var="")

# WD = os.path.dirname(os.path.abspath(__file__))
data = '/opt/airflow/data/yelp_academic_dataset_business.json'
# data = '/data/yelp_academic_dataset_business.json'

DAG_ID = 'raw_business'

# CONNECT to DB Postgres
def connect_db():
    try:
        conn = psycopg2.connect(f"host={DB_HOST} dbname={DB_NAME} user={DB_USERNAME} password={DB_USERNAME} port={DB_PORT}")
        logging.info(f'Connection to {conn} Success')
    except (Exception, psycopg2.DatabaseError) as error:
        logging.info(f"Error: {error}")
        conn.rollback()

# EXTRACT data from local --> LOAD to DB Postgres
def extract_load():
    with open(data, 'r') as f1:
        ll = [json.loads(line.strip()) for line in f1.readlines()]
        logging.info(f'Total Length Data : {len(ll)}')

    chunks = 10000
    total = len(ll) // chunks
    
    for i in range(total+1):
        df = pd.DataFrame(ll[i * chunks:(i+1) * chunks]).astype('string')
        df = df.fillna('NA')
        tuples = [tuple(x) for x in df.to_numpy()]
        # print(tuples)
        
        cols = ','.join(list(df.columns))
        
        try:
            conn = psycopg2.connect(f"host={DB_HOST} dbname={DB_NAME} user={DB_USERNAME} password={DB_USERNAME} port={DB_PORT}")
            print(f'Connection to {conn} Success')
        except ValueError:
            print('Connection Failed')

        # query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)" % ('tip_stg', cols)
        # print(query)

    
        cursor = conn.cursor()
        try:
            # execute_batch(cursor, query, tuples)
            execute_batch(cursor, "INSERT INTO raw.business_stg VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuples)
            conn.commit()
            print("execute_batch() done")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()

        
        cursor.close()




with DAG(dag_id=DAG_ID, start_date=datetime(2023, 3, 18), 
        schedule_interval='@once', catchup=False) as dag:
    
    start = EmptyOperator(
        task_id=f"start_{DAG_ID}"
    )
 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS raw.business_stg(
                business_id text,
                name text null,
                address text null,
                city text null,
                state text null,
                postal_code text null,
                latitide text null,
                longitude text null,
                stars text null,
                review_count text null,
                is_open text null,
                attributes text null,
                categories text null,
                hours text null
            );
        '''
    )

    truncate_table = PostgresOperator(
        task_id='truncate_table',
        postgres_conn_id='postgres',
        sql='''
            TRUNCATE TABLE raw.business_stg;
        '''
    )

    connect_db = PythonOperator(
        task_id = 'connect_db',
        python_callable=connect_db
    )

    extract_load = PythonOperator(
        task_id = 'extract_load_data',
        python_callable=extract_load
    )

    end = EmptyOperator(
        task_id=f"end_{DAG_ID}"
    )

    start >> create_table >> truncate_table >> connect_db >> extract_load >>end