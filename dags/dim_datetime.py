from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator #DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.variable import Variable
from datetime import datetime
import os
import logging

DAG_ID = 'dwh_dim_datetime'

with DAG(dag_id=DAG_ID, start_date=datetime(2023, 3, 18), 
        schedule_interval='@once', catchup=False) as dag:
    
    start = EmptyOperator(
        task_id=f"start_{DAG_ID}"
    )
    # sensoring raw_user
    wait_extract_raw_user = ExternalTaskSensor(
        task_id = 'wait_extract_raw_user',
        external_task_id='end_raw_user',
        external_dag_id='raw_user'

    )
    # sensoring raw_review
    wait_extract_raw_review = ExternalTaskSensor(
        task_id = 'wait_extract_raw_review',
        external_task_id='end_raw_review',
        external_dag_id='raw_review'

    )

    # sensoring raw_tip
    wait_extract_raw_tip = ExternalTaskSensor(
        task_id = 'wait_extract_rraw_tip',
        external_task_id='end_raw_tip',
        external_dag_id='raw_tip'

    )

    drop_table = PostgresOperator(
        task_id='drop_table',
        postgres_conn_id='postgres',
        sql='''
            DROP TABLE IF EXISTS dwh.dim_datetime
        '''
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS dwh.dim_datetime(
                dd_datetime_id TIMESTAMP NOT NULL PRIMARY KEY,
                full_date DATE,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                month_name VARCHAR(50),
                day_name VARCHAR(50)
            );
        '''
    )

    populate_table = PostgresOperator(
        task_id='populate_table',
        postgres_conn_id='postgres',
        sql='sql/dim_datetime.sql'
    )

    end = EmptyOperator(
        task_id=f"end_{DAG_ID}"
    )

    start >> [wait_extract_raw_review, 
              wait_extract_raw_tip, 
              wait_extract_raw_user] >> drop_table >> create_table >> populate_table >> end