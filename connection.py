import os
import json
import psycopg2
from sqlalchemy import create_engine
from pyspark.sql import SparkSession

def config(param):
    path = os.getcwd()
    with open(path+'/'+'config.json') as file:
        conf = json.load(file)[param]
    return conf

def db_conn(conf):
    try:
        conn = psycopg2.connect(host=conf['host'], 
                                database=conf['db'], 
                                user=conf['user'], 
                                password=conf['pwd'],
                                port=conf['port']
                                )
        print(f"----[CONNECTED TO DATABASE]----")
        engine = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['pwd']}@{conf['host']}:{conf['port']}/{conf['db']}")
        return conn, engine
    except:
        print(f"%%%%[ERROR CONNECTING TO DATABASE]%%%%")

def dwh_conn(conf):
    try:
        conn_dwh = psycopg2.connect(host=conf['host'], 
                                database=conf['db'], 
                                user=conf['user'], 
                                password=conf['pwd'],
                                port=conf['port']
                                )
        print(f"----[CONNECTED TO DATA WAREHOUSE]----")
        engine_dwh = create_engine(f"postgresql+psycopg2://{conf['user']}:{conf['pwd']}@{conf['host']}:{conf['port']}/{conf['db']}")
        return conn_dwh, engine_dwh
    except:
        print(f"%%%%[ERROR CONNECTING TO DATA WAREHOUSE]%%%%")

def spark_conn(app, config):
    master = config['ip']
    try:
        spark = SparkSession.builder \
            .master(master) \
                .appName(app) \
                    .getOrCreate()
                    
        print(f"----[CONNECTED TO SPARK ENGINE]----")
        return spark
    except:
        print(f"%%%%[ERROR CONNECTING TO SPARK ENGINE]%%%%")