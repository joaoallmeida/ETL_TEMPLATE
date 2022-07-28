import datetime
from airflow import DAG
from etl_operators.etlPlugin import Create,Extract,Refined,Load, DataQuality

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
    schedule_interval=None
) as dag:


    create = Create(
        task_id = 'create_database_and_tables'
    )

    extract = Extract(
        task_id = 'extract_data',
        tableName="yts_movies"
    )

    refined = Refined(
        task_id = 'refined_data',
        tableName="yts_movies"
    )

    loadDimTorrent = Load(
        task_id = 'load_dim_torrent',
        tableId = 'DimTorrent'
    )

    loadDimCalendar = Load(
        task_id = 'load_dim_calendar',
        tableId = 'DimCalendar'
    )

    loadDimGenres = Load(
        task_id = 'load_dim_genres',
        tableId = 'DimGenres'
    )

    loadDimMovie = Load(
        task_id = 'load_dim_movie',
        tableId = 'DimMovie'
    )

    loadFatFilms= Load(
        task_id = 'load_fat_films',
        tableId = 'FatFilm'
    )

    dataQuality = DataQuality(
        task_id = 'data_quality',
        tableName = [
            'DimCalendar',
            'DimTorrent',
            'DimGenres',
            'DimMovie',
            'FatFilm'
        ]
    )

    create >> extract >> refined >> [ loadDimCalendar, loadDimTorrent, loadDimGenres, loadDimMovie ] >> loadFatFilms >> dataQuality