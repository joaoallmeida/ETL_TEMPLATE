import datetime
from ssl import create_default_context
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


    create = runSql(
        task_id = 'create_database_and_tables'
    )

    extract = extractRawData(
        task_id = 'extract_data',
        tableName="yts_movies"
        # dag=dag
    )

    refined = refinedData(
        task_id = 'refined_data',
        tableName="yts_movies"
    )

    loadDimTorrent = starSchemaModel(
        task_id = 'load_dim_torrent',
        tableId = 'DimTorrent'
    )
    loadDimGenres = starSchemaModel(
        task_id = 'load_dim_genres',
        tableId = 'DimGenres'
    )
    loadDimMovie = starSchemaModel(
        task_id = 'load_dim_movie',
        tableId = 'DimMovie'
    )
    loadFatFilms= starSchemaModel(
        task_id = 'load_fat_films',
        tableId = 'FatFilms'
    )


    create >> extract >> refined >> [loadDimTorrent >> loadDimGenres >> loadDimMovie] >> loadFatFilms