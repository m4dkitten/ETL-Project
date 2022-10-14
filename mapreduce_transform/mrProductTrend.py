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

class mrProductTrend(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
            , MRStep(reducer=self.sort)
        ]

    def mapper(self, _, line):
        row = dict(zip(col, csv_readline(line)))
        if row['id_transaction'] != 'id_transaction':
            yield row['product_transaction'], 1

    def reducer(self, key, values):
        yield None, (key, sum(values))

    def sort(self, key, values):
        data = []
        for x, y in values:
            data.append((x, y))
            data.sort(reverse=True)

        for x, y in data:
            yield x, y

if __name__ == '__main__':
    mrProductTrend.run()
    df = pd.read_csv("mapreduce_transform/output/mrProductTrend.txt", delimiter="\t")
    df.to_csv("mapreduce_transform/output/mrProductTrend.csv", encoding='utf-8', index=False)
    os.remove("mapreduce_transform/output/mrProductTrend.txt")