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

    #extract source data to database
    list_filename = ['customer', 'product', 'transaction']
    for file in list_filename:
        pd.read_csv(f"data/bigdata_{file}.csv").to_sql(f"bigdata_{file}", con=engine, index=False)
        print(f"----[bigdata_{file} EXTRACTED TO DB]----")

    #initiate query
    pathQuery = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            pathQuery+'bigdata_joined.sql','r'
        ).read(), strip_comments=True
    ).strip()

    df = pd.read_sql(query, engine)

    #extract query result to local directory
    path = os.getcwd()
    directory = path+'/'+'data'+'/'
    if not os.path.exists(directory):
        os.makedirs(directory)
    df.to_csv(f"{directory}bigdata_joined.csv", index=False)
    print(f"----[bigdata_joined.csv EXTRACTED TO /data]----")

    #extract query result to data warehouse
    cursor.execute(query)
    conn_dwh.commit()
    df.to_sql('bigdata_joined', engine_dwh, if_exists='append', index=False)
    print(f"----[bigdata_joined EXTRACTED TO DWH]----")