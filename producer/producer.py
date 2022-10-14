#!python3

import pandas as pd
import numpy as np
import json
import time
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

if __name__ == "__main__":

    #read data
    file = pd.read_csv('bigdata_log.csv').to_dict(orient='records')

    #connect kafka server
    producer = KafkaProducer(bootstrap_servers=['localhost'], 
                             value_serializer=json_serializer)
    
    #push data to kafka server with topic "final_project"
    while True:
        for data in file:
            print(data)
            producer.send("final-project", data)
            # time.sleep(2)
