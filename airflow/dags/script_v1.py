from airflow import DAG 
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from ingest_data_v1 import ingest_data_to_postgres

# 'conn_id' connect to postgreSQL
conn_id = 'postgres_local'

url_prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
url_yellow_taxi = url_prefix + "{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
yellow_taxi_file = "/opt/airflow/" + "yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
yellow_taxi_table_name = "yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}"

url_location = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
location_file = "/opt/airflow/taxi_zone_lookup.csv"
location_table_name = "taxi_zone_lookup"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 2, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False   # set to False to prevent backfilling
}

with DAG(
    dag_id="ingest_data_v1",
    default_args=default_args,
    schedule_interval="0 0 3 * *",
) as dag:

    # Task download data
    with TaskGroup("download_data") as download_data_group:
        download_yellow_trip_data_task = BashOperator(
            task_id="download_yellow_taxi_data",
            bash_command=f"curl {url_yellow_taxi} > {yellow_taxi_file}"
        )

        download_location_data_task = BashOperator(
            task_id="download_location_data",
            bash_command=f"curl {url_location} > {location_file}",    
        )

    # Task ingest data into postgres
    with TaskGroup("ingest_data_into_postgres") as ingest_data_group:
        ingest_location_data_task = PythonOperator(
            task_id="ingest_location_data_to_postgres",
            python_callable=ingest_data_to_postgres,
            op_kwargs=dict(
                parquet_file=location_file,
                conn_id=conn_id,
                table_name=location_table_name
            )
        )

        ingest_yellow_taxi_data_task = PythonOperator(
            task_id="ingest_yellow_taxi_data_to_postgres",
            python_callable=ingest_data_to_postgres,
            op_kwargs=dict(
                parquet_file=yellow_taxi_file,
                conn_id=conn_id,
                table_name=yellow_taxi_table_name
            )
        )

    download_data_group >> ingest_data_group