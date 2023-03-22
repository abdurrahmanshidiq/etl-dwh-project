from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator #DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.variable import Variable
from datetime import datetime
import os
import logging

DAG_ID = 'dwh_fact_tip'

with DAG(dag_id=DAG_ID, start_date=datetime(2023, 3, 18), 
        schedule_interval='@once', catchup=False,
        tags=['dwh', 'digitalskola']) as dag:
    
    start = EmptyOperator(
        task_id=f"start_{DAG_ID}"
    )
    wait_extract_raw_tip = ExternalTaskSensor(
        task_id = 'wait_extract_raw_tip',
        external_task_id='end_raw_tip',
        external_dag_id='raw_tip'

    )

    wait_extract_raw_business = ExternalTaskSensor(
        task_id = 'wait_extract_raw_business',
        external_task_id='end_raw_business',
        external_dag_id='raw_business'

    )

    wait_extract_dwh_dim_location = ExternalTaskSensor(
        task_id = 'wait_extract_dwh_dim_location',
        external_task_id='end_dwh_dim_location',
        external_dag_id='dwh_dim_location'

    )

    drop_table = PostgresOperator(
        task_id='drop_table',
        postgres_conn_id='postgres',
        sql='''
            DROP TABLE IF EXISTS dwh.fact_tip
        '''
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS dwh.fact_tip(
                ft_tip_id VARCHAR(256) NOT NULL PRIMARY KEY,
                ft_user_id VARCHAR(256) NOT NULL,
                ft_business_id VARCHAR(256) NOT NULL,
                ft_location_id VARCHAR(256) NOT NULL,
                text TEXT,
                date_tip TIMESTAMP,
                compliment_count INTEGER
            );
        '''
    )

    populate_table = PostgresOperator(
        task_id='populate_table',
        postgres_conn_id='postgres',
        sql='sql/fact_tip.sql'
    )

    end = EmptyOperator(
        task_id=f"end_{DAG_ID}"
    )

    start >> [wait_extract_raw_tip,
              wait_extract_raw_business,
              wait_extract_dwh_dim_location] >> drop_table >> create_table >> populate_table >> end