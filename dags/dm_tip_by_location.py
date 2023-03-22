from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator #DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.variable import Variable
from datetime import datetime
import os
import logging

DAG_ID = 'dm_tip_by_location'

with DAG(dag_id=DAG_ID, start_date=datetime(2023, 3, 18), 
        schedule_interval='@once', catchup=False, 
        tags=['datamart', 'digitalskola']) as dag:
    
    start = EmptyOperator(
        task_id=f"start_{DAG_ID}"
    )

    # ------------------------- SENSORING ---------------------------------------------
    # sensor fac_tip
    wait_extract_dwh_fact_tip = ExternalTaskSensor(
        task_id = 'wait_extract_dwh_fact_tip',
        external_task_id='end_dwh_fact_tip',
        external_dag_id='dwh_fact_tip'
    )

    # sensor dim_business
    wait_extract_dwh_dim_business = ExternalTaskSensor(
        task_id = 'wait_extract_dwh_dim_business',
        external_task_id='end_dwh_dim_business',
        external_dag_id='dwh_dim_business'
    )

    # sensor dim_location
    wait_extract_dwh_dim_location = ExternalTaskSensor(
        task_id = 'wait_extract_dwh_dim_location',
        external_task_id='end_dwh_dim_location',
        external_dag_id='dwh_dim_location'
    )

    # sensor dim_datetime
    wait_extract_dwh_dim_datetime = ExternalTaskSensor(
        task_id = 'wait_extract_dwh_dim_datetime',
        external_task_id='end_dwh_dim_datetime',
        external_dag_id='dwh_dim_datetime'
    )

    # sensor ref_percipitation ref_temperature
    wait_extract_dwh_ref_precipitation_temperature = ExternalTaskSensor(
        task_id = 'wait_extract_dwh_ref_precipitation_temperature',
        external_task_id='end_dwh_ref_precipitation_temperature',
        external_dag_id='dwh_ref_precipitation_temperature'
    )


    # ------------------ DROP TABLE --------------------------------
    drop_table = PostgresOperator(
        task_id='drop_table',
        postgres_conn_id='postgres',
        sql='''
            DROP TABLE IF EXISTS dm.tip_by_location
        '''
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS datamart.tip_by_location(
                location_id VARCHAR(256) NOT NULL,
                vanue_name VARCHAR(256),
                is_venue_open INTEGER,
                categories TEXT,
                city VARCHAR(256),
                state VARCHAR(15),
                date DATE,
                month_name VARCHAR(20),
                day_name VARCHAR(20),
                compliment_count INTEGER,
                min_temperature FLOAT,
                max_temperature FLOAT,
                precipitation FLOAT
            );
        '''
    )

    populate_table = PostgresOperator(
        task_id='populate_table',
        postgres_conn_id='postgres',
        sql='sql/dm_tip_by_location.sql'
    )

    end = EmptyOperator(
        task_id=f"end_{DAG_ID}"
    )

    start >> [wait_extract_dwh_fact_tip, wait_extract_dwh_dim_business,
              wait_extract_dwh_dim_location, wait_extract_dwh_dim_datetime,
              wait_extract_dwh_ref_precipitation_temperature] >> drop_table >> create_table >> populate_table >> end