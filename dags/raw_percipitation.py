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
data = '/opt/airflow/data/USW00023169-LAS_VEGAS_MCCARRAN_INTL_AP-precipitation-inch.csv'

DAG_ID = 'raw_percipitation'



# EXTRACT data from local --> LOAD to DB Postgres
def extract_load():
    try:
        engine = create_engine(f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        df = pd.read_csv(data)
        df['precipitation'] = df['precipitation'].replace('T', None)
        df.to_sql('percipitation_stg', engine, if_exists='replace', schema='raw', index=False)
        print(f'Success')
    except ValueError:
        print('Failed')




with DAG(dag_id=DAG_ID, start_date=datetime(2023, 3, 18), 
        schedule_interval='@once', catchup=False,
        tags=['raw', 'digitalskola']) as dag:
    
    start = EmptyOperator(
        task_id=f"start_{DAG_ID}"
    )

    extract_load = PythonOperator(
        task_id = 'extract_load_data',
        python_callable=extract_load
    )

    end = EmptyOperator(
        task_id=f"end_{DAG_ID}"
    )

    start  >> extract_load >> end