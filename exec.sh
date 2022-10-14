#extract source data to be processed
python dataExtract.py

#mapreduce 1
python mapreduce_transform/mrCustomerTrend.py data/bigdata_joined.csv > mapreduce_transform/output/mrCustomerTrend.txt

#mapreduce 2
python mapreduce_transform/mrTransactionTrend.py data/bigdata_joined.csv > mapreduce_transform/output/mrTransactionTrend.txt

#mapreduce 3
python mapreduce_transform/mrProductTrend.py data/bigdata_joined.csv > mapreduce_transform/output/mrProductTrend.txt

#spark processing
python sparkProcessing.py

#load output data to data warehouse
python dataLoad.py