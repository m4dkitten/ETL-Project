#!python3

import os
import json
import pandas
from kafka import KafkaConsumer
import connection

if __name__ == '__main__':
    print(f"----[LOG CONSUMER STARTING]----")
    path = os.getcwd()+"/"

    #initiate a connection to database --> "finalproject"
    conf = connection.config('dbconfig')
    conn, engine = connection.db_conn(conf)
    cursor = conn.cursor()

    #initiate a connection to data warehouse --> "dwh_finalproject"
    conf_dwh = connection.config('dwhconfig')
    conn_dwh, engine_dwh = connection.dwh_conn(conf_dwh)
    cursor_dwh = conn.cursor()
    
    #initiate a connection to kafka server with topic final-project
    try:
        consumer = KafkaConsumer("final-project", bootstrap_servers='localhost')
        print(f"----[CONNECTED TO KAFKA SERVER]----")
    except:
        print(f"%%%%[ERROR CONNECTING TO KAFKA SERVER]%%%%")

    #read message from topic kafka server
    for msg in consumer:
        data = json.loads(msg.value)
        print(f"Records = {json.loads(msg.value)}")

        #insert database   
        df = pandas.DataFrame(data, index=[0])
        df.to_sql('bigdata_log', engine_dwh, if_exists='append', index=False)