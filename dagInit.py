#!python3

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id = 'final-project',
    start_date = datetime(2022, 10, 14),
    catchup = False,
    tags = ['project'],
) as dag:
    t1 = BashOperator(
        task_id = 'dataExtract',
        bash_command = 'python /mnt/d/Files/Digitalskola-DE/Project/final-project/dataExtract.py',
    )

    t2 = BashOperator(
        task_id = 'mapreduceCustomerTrend',
        bash_command = 'python /mnt/d/Files/Digitalskola-DE/Project/final-project/mapreduce_transform/customerTrend.py',
    )

    t3 = BashOperator(
        task_id = 'mapreduceTransactionTrend',
        bash_command = 'python /mnt/d/Files/Digitalskola-DE/Project/final-project/mapreduce_transform/transactionTrend.py',
    )

    t4 = BashOperator(
        task_id = 'mapreduceProductTrend',
        bash_command = 'python /mnt/d/Files/Digitalskola-DE/Project/final-project/mapreduce_transform/productTrend.py',
    )

    t5 = BashOperator(
        task_id = 'sparkProcessing',
        bash_command = 'python /mnt/d/Files/Digitalskola-DE/Project/final-project/sparkProcessing.py',
    )

    t1 >> t2 >> t3 >> t4 >> t5