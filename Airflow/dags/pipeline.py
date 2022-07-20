import datetime
from airflow import DAG
from airflow.operators.etl_plugin import runSql,extractRawData,refinedData,starSchemaModel

default_args={
        'start_date': datetime.datetime(2022,1,1),
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'execution_timeout': datetime.timedelta(seconds=300),
        'catchup': False
    }

with DAG(
    dag_id="Orchestrator",
    description='A sample orchestrator for ETL process.',
    default_args= default_args,
    schedule_interval="@daily"
) as dag:


    create_database = runSql(
        task_id = 'create_database_and_tables'
        # python_callable=createDB
    )

    extract = extractRawData(
        task_id = 'extract_data',
        # python_callable=ExtractData,
        tableName="yts_movies"
        # dag=dag
    )

    refined = refinedData(
        task_id = 'refined_data',
        # python_callable=DataRefinement,
        tableName="yts_movies"
    )

    load = starSchemaModel(
        task_id = 'load_data'
        # python_callable=LoadStartSchema,
    )


    create_database >> extract >> refined >> load