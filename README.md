# etl-dwh-project

- run command `docker-compose up -d`
- copy local file (data) to container airflow-webserver by run command :
  - `docker cp [YOUR FILE PATH] [container name]:/opt/asirflow`
  - on my case : `docker cp /data/yelp_academic_dataset_business.json digitalskola_final_project_airflow-webserver_1:/opt/airflow`
