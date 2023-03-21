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
