# etl-dwh-project

1. create python virtual emvironment:
  - python -m venv env-project
  - source env-project/bin/activate
  - pip3 install --upgrade pip
  - pip3 install -r requirements.txt
2. run command `docker-compose up -d`
3. copy local file (data) to container airflow-webserver by run command :
  - `docker cp [YOUR FILE PATH] [container name]:/opt/asirflow`
  - on my case : `docker cp /data/yelp_academic_dataset_business.json digitalskola_final_project_airflow-webserver_1:/opt/airflow`
4. Data Source :
  - https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset?resource=download&select=yelp_academic_dataset_business.json
  - https://github.com/rizqinugroho/sample-data.git
5. Architecure high-level
  - Layers on data warehouse environment
    - raw : landing zone 
    - dwh : cleaned, standardized data. with data modelling star schema
    - datamart : data with spesific business case
    ![architect-highlevel](https://github.com/abdurrahmanshidiq/etl-dwh-project/blob/master/img/architect-highlevel.png "architect-highlevel")<br>

6. Entity Relationship Diagram
  - data source
    ![yelp-erd-source](https://github.com/abdurrahmanshidiq/etl-dwh-project/blob/master/img/yelp-erd-source.png "yelp-erd-source")<br>
  - data warehouse (starschema)
    ![yelp-erd-dwh](https://github.com/abdurrahmanshidiq/etl-dwh-project/blob/master/img/yelp-erd-dwh.png "yelp-erd-dwh")<br>
7. sample DAG airflow
  - Raw layer
    ![raw](https://github.com/abdurrahmanshidiq/etl-dwh-project/blob/master/img/raw.png "raw")<br>
  - DWH layer
    ![dwh](https://github.com/abdurrahmanshidiq/etl-dwh-project/blob/master/img/dwh.png "dwh")<br>
  - Datamart layer
    ![datamart](https://github.com/abdurrahmanshidiq/etl-dwh-project/blob/master/img/datamart.png "datamart")<br>
  - load csv
    ![load_csv](https://github.com/abdurrahmanshidiq/etl-dwh-project/blob/master/img/load_csv.png "load_csv")<br>
