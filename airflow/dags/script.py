from airflow import DAG 
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import os
from datetime import datetime, timedelta
from ingest_data import ingest_data_to_postgres
from dotenv import load_dotenv

load_dotenv()

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")

# Ensure all environment variables are set
if not all([user, password, host, port, database]):
    raise ValueError("One or more environment variables are not set")

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
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False   # set to False to prevent backfilling
}

with DAG(
    dag_id="ingest_data",
    default_args=default_args,
    schedule_interval="@once",
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
                user=user,
                password=password,
                host=host,
                port=port,
                database=database,
                table_name=location_table_name
            )
        )

        ingest_yellow_taxi_data_task = PythonOperator(
            task_id="ingest_yellow_taxi_data_to_postgres",
            python_callable=ingest_data_to_postgres,
            op_kwargs=dict(
                parquet_file=yellow_taxi_file,
                user=user,
                password=password,
                host=host,
                port=port,
                database=database,
                table_name=yellow_taxi_table_name
            )
        )
    
    download_data_group >> ingest_data_group