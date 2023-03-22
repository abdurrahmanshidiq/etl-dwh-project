from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator #DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.variable import Variable
from datetime import datetime
import os
import logging

DAG_ID = 'dwh_fact_review'

with DAG(dag_id=DAG_ID, start_date=datetime(2023, 3, 18), 
        schedule_interval='@once', catchup=False, 
        tags=['dwh', 'digitalskola']) as dag:
    
    start = EmptyOperator(
        task_id=f"start_{DAG_ID}"
    )
    wait_extract_raw_review = ExternalTaskSensor(
        task_id = 'wait_extract_raw_review',
        external_task_id='end_raw_review',
        external_dag_id='raw_review'

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
            DROP TABLE IF EXISTS dwh.fact_review
        '''
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS dwh.fact_review(
                fr_review_id VARCHAR(256) NOT NULL PRIMARY KEY,
                fr_user_id VARCHAR(256) NOT NULL,
                fr_business_id VARCHAR(256) NOT NULL,
                fr_location_id VARCHAR(256) NOT NULL,
                stars DOUBLE PRECISION,
                useful INTEGER,
                funny INTEGER,
                cool INTEGER,
                text TEXT,
                date_review TIMESTAMP
            );
        '''
    )

    populate_table = PostgresOperator(
        task_id='populate_table',
        postgres_conn_id='postgres',
        sql='sql/fact_review.sql'
    )

    end = EmptyOperator(
        task_id=f"end_{DAG_ID}"
    )

    start >> [wait_extract_raw_review,
              wait_extract_raw_business,
              wait_extract_dwh_dim_location] >> drop_table >> create_table >> populate_table >> end