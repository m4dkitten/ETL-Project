#!python3

import os
import csv
import pandas as pd
from mrjob.job import MRJob
from mrjob.step import MRStep

col = 'id_customer,birthdate_customer,age,country_customer,gender_customer,Type,id_transaction,date_transaction,product_transaction,amount_transaction'.split(',')

def csv_readline(line):
    for row in csv.reader([line]):
        return row

class mrCustomerTrend(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

    def mapper(self, _, line):
        row = dict(zip(col, csv_readline(line)))
        if row['id_customer'] != 'id_customer':
            yield row['gender_customer'], 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    mrCustomerTrend.run()
    df = pd.read_csv("mapreduce_transform/output/mrCustomerTrend.txt", delimiter="\t")
    df.to_csv("mapreduce_transform/output/mrCustomerTrend.csv", encoding='utf-8', index=False)
    os.remove("mapreduce_transform/output/mrCustomerTrend.txt")