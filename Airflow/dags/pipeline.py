import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from ETL.create_database import createDB  
from ETL.extract import ExtractData 
from ETL.refined import DataRefinement
from ETL.load import *

default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'execution_timeout': datetime.timedelta(seconds=300)
    }

with DAG(
    "orchestrator",
    start_date=datetime.datetime(2022,1,1),
    default_args= default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:


    create_database = PythonOperator(
        task_id = 'create_database',
        python_callable=createDB
    )
    

    extract = PythonOperator(
        task_id = 'extract_data',
        python_callable=ExtractData,
        op_args={"yts_movies":"TableName"},
        dag=dag
    )

    refined = PythonOperator(
        task_id = 'refined_data',
        python_callable=DataRefinement,
        op_args={"yts_movies":"TableName"},
    )

    load = PythonOperator(
        task_id = 'load_data',
        python_callable=LoadStartSchema,
    )


    create_database >> extract >> refined >> load