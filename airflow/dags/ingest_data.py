import pandas as pd
import os
from sqlalchemy import create_engine
from time import time

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        raise ValueError('Only CSV files are supported')
    else:
        df = pd.read_csv(src_file)
        parquet_file = src_file.replace('.csv', '.parquet')
        df.to_parquet(parquet_file, engine="pyarrow", index=False)
        print('File converted from csv to parquet successfully!')
        return parquet_file

def ingest_data_to_postgres(parquet_file, user, password, host, port, database, table_name):
    if not parquet_file.endswith('.parquet'):
        parquet_file = format_to_parquet(parquet_file)

    df = pd.read_parquet(parquet_file)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    engine.connect()
    print("Connection established to postgres database!")

    # Create schema staging and warehouse
    with engine.connect() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")

    df.head(5).to_sql(table_name, engine, schema= 'staging',if_exists='replace', index=False)
    print("Inserted first 5 rows into postgres database successfully!")

    chunk_size = 100000

    if (len(df) < chunk_size):
        t_start = time()
        df.to_sql(table_name, engine, schema= 'staging', if_exists='append', index=False)
        t_end = time()
        print('Data inserted successfully. Time to write:', t_end - t_start)
        return
    else:
        chunks = [df.iloc[i:i+chunk_size] for i in range(5, df.shape[0], chunk_size)]

        total_time = 0
        for i, chunk in enumerate(chunks):
            t_start = time()
            chunk.to_sql(table_name, engine, schema= 'staging', if_exists='append', index=False)
            t_end = time()
            print(f'Inserted chunk number {i}. Time to write: {t_end - t_start}')
            total_time += t_end - t_start

        print('Data inserted successfully. Total time:', total_time)