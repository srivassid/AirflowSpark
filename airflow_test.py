import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession, functions
from airflow import DAG

def sparkSession():
    # Create SparkSession
    spark = SparkSession.builder \
      .master("local[1]") \
      .appName("tbcov") \
      .getOrCreate()
    return spark

def countries_tweets():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("tbcov") \
        .getOrCreate()
    # spark = sparkSession()

    df = spark.read.format('csv').option("sep", "\t").option('header', 'true'). \
	  load('airflow/dags/april*.*')

    print(df.head())

    df = df.groupBy('user_loc_country_code').count().orderBy("count", ascending=False)

    print(df.show())

    #df.coalesce(1).write.csv("/home/sid/Documents/PythonProjects/SparkTwitter/coronaTwitter/question_2.csv", header=True)

default_args = {
    'owner': 'sid',
    'start_date': datetime(2024, 1, 1),
    'retries': 10,
	  'retry_delay': timedelta(hours=1)
}
my_dag = DAG('spark_dag',
                  default_args=default_args,
                  schedule_interval='0 1 * * *')

python_task_1 = PythonOperator(
        task_id='spark_task',
        python_callable=countries_tweets,
dag=my_dag
    )

