#!python3
import os
import sqlparse
import pandas as pd
import connection

if __name__ == "__main__":
    #initiate a connection to database --> "finalproject"
    conf = connection.config('dbconfig')
    conn, engine = connection.db_conn(conf)
    cursor = conn.cursor()

    #initiate a connection to data warehouse --> "dwh_finalproject"
    conf_dwh = connection.config('dwhconfig')
    conn_dwh, engine_dwh = connection.dwh_conn(conf_dwh)
    cursor_dwh = conn.cursor()

    #extract mapreduce output data to data warehouse
    list_filename = ['Customer', 'Product', 'Transaction']
    for file in list_filename:
        pd.read_csv(f"mapreduce_transform/output/mr{file}Trend.csv").to_sql(f"mr{file}Trend", con=engine_dwh, index=False)
        print(f"----[mr{file}Trend EXTRACTED TO DWH]----")

    #extract spark processing output data to data warehouse
    list_filename = ['ProductSales', 'TransactionCountByCountry', 'AgeGenderDistribution']
    for file in list_filename:
        pd.read_csv(f"spark_transform/spark{file}.csv").to_sql(f"spark{file}", con=engine_dwh, index=False)
        print(f"----[spark{file} EXTRACTED TO DWH]----")

