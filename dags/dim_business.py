from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator #DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.variable import Variable
from datetime import datetime
import os
import logging

DAG_ID = 'dwh_dim_business'

with DAG(dag_id=DAG_ID, start_date=datetime(2023, 3, 18), 
        schedule_interval='@once', catchup=False,
        tags=['dwh', 'digitalskola']) as dag:
    
    start = EmptyOperator(
        task_id=f"start_{DAG_ID}"
    )
    wait_extract_done = ExternalTaskSensor(
        task_id = 'wait_extract_done',
        external_task_id='end_raw_business',
        external_dag_id='raw_business'

    )

    drop_table = PostgresOperator(
        task_id='drop_table',
        postgres_conn_id='postgres',
        sql='''
            DROP TABLE IF EXISTS dwh.dim_business
        '''
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS dwh.dim_business(
                db_business_id VARCHAR(256) NOT NULL PRIMARY KEY,
                name VARCHAR(1024),
                address VARCHAR(1024),
                latitude DECIMAL,
                longitude DECIMAL,
                is_open INTEGER,
                stars DECIMAL,
                categories text
            );
        '''
    )

    populate_table = PostgresOperator(
        task_id='populate_table',
        postgres_conn_id='postgres',
        sql='sql/dim_business.sql'
    )

    end = EmptyOperator(
        task_id=f"end_{DAG_ID}"
    )

    start >> wait_extract_done >> drop_table >> create_table >> populate_table >> end