from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

def print_message():
    print("Dell Demo Pyspark Program running")

default_args = {
    'owner' : 'datamaking',
    'start_date' : datetime(2022, 10, 23),
    'retries' : 0,
    'catchup' : False,
    'retry_delay' : timedelta(minutes=5)
}

#everyday once midnigh will run
dag = DAG('Dell Demo Practice PySpark',
          default_args=default_args,
          schedule_interval='0 0 * * *')

python_operator = PythonOperator(task_id='print_message',
                                 python_callable=print_message, dag=dag)

spark_config = {
    'conf': {
        "spark.yarn.maxAppAttempts": "1",
        "spark.yarn.executor.memoryOverhead": "512"
    },
    'conn_id': 'spark_local',
    'application': '/c/Users/sridh/airflow/DellDemoPracticeWithPySpark.py',
    'driver_memory': '1g',
    'executor_cores': 1,
    'num_executors': 1,
    'executor_memory': '1g'
}

spark_operator = SparkSubmitOperator(taskid='spark_submit_task', dag=dag, **spark_config)

python_operator.set_downstream(spark_operator)

if __name__ == "__main__":
    dag.cli()