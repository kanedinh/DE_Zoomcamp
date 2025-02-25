import pandas as pd
from postgres_operator import PostgresOperators

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        raise ValueError('Only CSV files are supported')
    else:
        df = pd.read_csv(src_file)
        parquet_file = src_file.replace('.csv', '.parquet')
        df.to_parquet(parquet_file, engine="pyarrow", index=False)
        print('File converted from csv to parquet successfully!')
        return parquet_file

def ingest_data_to_postgres(parquet_file, conn_id, table_name):
    if not parquet_file.endswith('.parquet'):
        parquet_file = format_to_parquet(parquet_file)

    df = pd.read_parquet(parquet_file)
    postgres_operator = PostgresOperators(conn_id=conn_id)
    print("Connecting to PostgreSQL successfully!")

    # Create schema staging and warehouse
    query = """
    CREATE SCHEMA IF NOT EXISTS staging;
    """
    postgres_operator.execute_sql(query)
    query = """
    CREATE SCHEMA IF NOT EXISTS warehouse;
    """
    postgres_operator.execute_sql(query)
    print("Schema staging and warehouse had created.")

    # Ingest raw data to PostgreSQL in schema staging
    postgres_operator.save_data_to_postgres(df=df, table_name=table_name, schema='staging', if_exists='replace')