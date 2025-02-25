from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
from time import time
from math import ceil

class PostgresOperators:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.engine = create_engine(self.hook.get_uri())

    # Method conn
    def get_connection(self):
        return self.hook.get_conn()
    
    # Medthod return a pandas dataframe from a sql query to the database
    def get_data_to_df(self, sql):
        return self.hook.get_pandas_df(sql)

    # Method save a pandas dataframe to a table in the database
    def save_data_to_postgres(self, df, table_name, schema='public', if_exists='replace'):
        chunk_size = 100000

        if (len(df) < chunk_size):
            t_start = time()
            df.to_sql(table_name, self.engine, schema=schema, if_exists=if_exists, index=False)
            t_end = time()
            print('Data inserted successfully. Total time:', t_end - t_start)
        else:
            chunks = [df.iloc[i:i+chunk_size] for i in range(0, df.shape[0], chunk_size)]
            print(f"With chunk_size = {chunk_size}, divide data into {ceil(len(df)/chunk_size)} chunks")
            total_time = 0
            for i, chunk in enumerate(chunks):
                t_start = time()
                if (i == 0 and if_exists == 'replace'):
                    chunk.to_sql(table_name, self.engine, schema=schema, if_exists='replace', index=False)
                else:
                    chunk.to_sql(table_name, self.engine, schema=schema, if_exists='append', index=False)
                t_end = time()
                print(f'Inserted chunk number {i + 1}. Time to write: {t_end - t_start}')
                total_time += t_end - t_start

            print('Data inserted successfully. Total time:', total_time)
        
    # Method execute a sql query in the database
    def execute_sql(self, sql):
        self.hook.run(sql)