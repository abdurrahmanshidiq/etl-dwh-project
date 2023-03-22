from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator #DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.variable import Variable
from datetime import datetime
import os
import logging

DAG_ID = 'dwh_ref_precipitation_temperature'

with DAG(dag_id=DAG_ID, start_date=datetime(2023, 3, 18), 
        schedule_interval='@once', catchup=False,
        tags=['dwh', 'digitalskola']) as dag:
    
    start = EmptyOperator(
        task_id=f"start_{DAG_ID}"
    )
    wait_extract_raw_percipitation = ExternalTaskSensor(
        task_id = 'wait_extract_raw_percipitation',
        external_task_id='end_raw_percipitation',
        external_dag_id='raw_percipitation'

    )

    wait_extract_raw_temperature = ExternalTaskSensor(
        task_id = 'wait_extract_raw_temperature',
        external_task_id='end_raw_temperature',
        external_dag_id='raw_temperature'

    )


    drop_table_percipitation = PostgresOperator(
        task_id='drop_table_percipitation',
        postgres_conn_id='postgres',
        sql='''
            DROP TABLE IF EXISTS dwh.ref_percipitation
        '''
    )


    drop_table_temperature = PostgresOperator(
        task_id='drop_table_temperature',
        postgres_conn_id='postgres',
        sql='''
            DROP TABLE IF EXISTS dwh.ref_temperature
        '''
    )

    create_table_percipitation = PostgresOperator(
        task_id='create_table_percipitation',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS dwh.ref_percipitation(
                date DATE,
                precipitation FLOAT,
                precipitation_normal FLOAT
            );
        '''
    )

    create_table_temperature = PostgresOperator(
        task_id='create_table_temperature',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS dwh.ref_temperature(
                date DATE,
                min FLOAT,
                max FLOAT,
                normal_min FLOAT,
                normal_max FLOAT
            );
        '''
    )

    populate_table_percipitation = PostgresOperator(
        task_id='populate_table_percipitation',
        postgres_conn_id='postgres',
        sql='sql/ref_percipitation.sql'
    )

    populate_table_temperature = PostgresOperator(
        task_id='populate_table_temperature',
        postgres_conn_id='postgres',
        sql='sql/ref_temperature.sql'
    )

    end = EmptyOperator(
        task_id=f"end_{DAG_ID}"
    )

    #Percipitation
    start >> wait_extract_raw_percipitation >> drop_table_percipitation >> create_table_percipitation >> populate_table_percipitation >> end
    #Temperature
    start >> wait_extract_raw_temperature >> drop_table_temperature >> create_table_temperature >> populate_table_temperature >> end
