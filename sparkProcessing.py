#!python3

import os
import sqlparse
import pandas as pd
import pyspark
from pyspark.sql.functions import col, asc, desc, count
import connection

if __name__ == '__main__':
    print("----[STARTING SPARK PROCESSING]----")

    #initiate a connection to database --> "finalproject"
    conf = connection.config('dbconfig')
    conn, engine = connection.db_conn(conf)
    cursor = conn.cursor()

    #initiate a connection to data warehouse --> "dwh_finalproject"
    conf_dwh = connection.config('dwhconfig')
    conn_dwh, engine_dwh = connection.dwh_conn(conf_dwh)
    cursor_dwh = conn.cursor()

    #initiate a connection to spark
    conf_spark = connection.config('spark')
    spark = connection.spark_conn(app="final_project", config=conf_spark)

    #initiate query
    pathQuery = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(
            pathQuery+'bigdata_joined.sql','r'
        ).read(), strip_comments=True
    ).strip()

    df = pd.read_sql(query, engine)

    SparkDF = spark.createDataFrame(df)
    SparkDF.groupBy("product_transaction").sum("amount_transaction").sort(col("sum(amount_transaction)").desc()) \
        .toPandas() \
        .to_csv(f"spark_transform/sparkProductSales.csv", index=False)

    SparkDF = spark.createDataFrame(df)
    SparkDF.groupBy("country_customer").agg({"id_transaction":"count"}).sort(col("count(id_transaction)").desc()) \
        .toPandas() \
        .to_csv(f"spark_transform/sparkTransactionCountByCountry.csv", index=False)

    SparkDF = spark.createDataFrame(df)
    SparkDF.groupBy("age", "gender_customer").agg({"id_customer":"count"}).sort(col("age")) \
        .toPandas() \
        .to_csv(f"spark_transform/sparkAgeGenderDistribution.csv", index=False)
    
    print(f"----[SPARK PROCESSING FINISHED]----")