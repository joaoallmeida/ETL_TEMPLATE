from .Connections.db_connection import engineSqlAlchemy, mysqlconnection
from .Functions.utils_functions import *
from configparser import ConfigParser

import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

# Function responsible for refined the raw data.
def DataRefinement():

    dt_now = datetime.datetime.now(pytz.timezone('UTC'))
    user = f'{getpass.getuser()}@{socket.gethostname()}'

    config = ConfigParser()
    config.read('ETL/Connections/credencials.ini')

    HOST=config['MySql']['host']
    USER=config['MySql']['user']
    PASSWORD=config['MySql']['pass']
    PORT = 3306
    DB_READ='db_movies_bronze'
    DB_WRITE='db_movies_silver'

    logging.info(f'Starting the data refinement process')
    
    try:

        conn_read = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_READ)
        conn_write = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_WRITE)
        dbconn = mysqlconnection(HOST,USER,PASSWORD,PORT,DB_WRITE)

        df_movie = pd.read_sql_table('yts_movies',conn_read)

        drop_columns = ['title_english','title_long','slug','description_full','peers',
                        'synopsis','mpa_rating','background_image','seeds','url_tt',
                        'background_image_original','small_cover_image','date_uploaded_unix_tt',
                        'state','date_uploaded_unix','medium_cover_image','hash','movie_sk']

        rename_columns = {
            "url":"url_yts",
            "date_uploaded_tt":"uploaded_torrent_at",
            "date_uploaded":"uploaded_content_at",
            "large_cover_image":"banner_image"
            }

        df_movie = df_movie.drop(drop_columns,axis=1)
        df_movie = df_movie.drop_duplicates().reset_index(drop=True)
        df_movie = df_movie.rename(rename_columns,axis=1)

        df_movie['title'] = df_movie['title'].str.upper()
        df_movie['language'] = df_movie['language'].str.upper()
        df_movie['type'] = df_movie['type'].str.upper()
        df_movie['genre_0'] = df_movie['genre_0'].str.upper()
        df_movie['genre_1'] = df_movie['genre_1'].str.upper()
        df_movie['genre_2'] = df_movie['genre_2'].str.upper()
        df_movie['genre_3'] = df_movie['genre_3'].str.upper()

        df_movie['uploaded_torrent_at'] = pd.to_datetime(df_movie['uploaded_torrent_at'],errors='coerce')
        df_movie['uploaded_content_at'] = pd.to_datetime(df_movie['uploaded_content_at'],errors='coerce')

        df_movie['loaded_at'] = pd.to_datetime(dt_now)
        df_movie['loaded_by'] = user

        truncateTable('yts_movies',dbconn)
        df_movie.to_sql(name='yts_movies',con=conn_write,if_exists='append',index=False)

        lines_number = len(df_movie.index)

    except Exception as e:
        conn_write.close()
        logging.error(f'Error to refinement data: {e}')
        raise TypeError(e)
    
    finally:
        logging.info('Ending the data refinement process')
    
    logging.info(f'Refined lines {lines_number}')
    # return df_movie
