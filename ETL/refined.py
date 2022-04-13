
from .Functions.transform_functions import addNewColumnToDF
from .Connections.db_connection import engineSqlAlchemy
from configparser import ConfigParser

import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

# Function responsible for refined the raw data.
def DataRefinement():

    dt_now = datetime.datetime.now(pytz.timezone('UTC'))
    user = f'{getpass.getuser()}@{socket.gethostname()}'

    config = ConfigParser()
    config.read('ETL/Connections/credencials.ini')

    HOST=config['MySql']['host']
    USER=config['MySql']['user']
    PASSWORD=config['MySql']['pass']
    DB_READ='db_movies_bronze'
    DB_WRITE='db_movies_silver'

    logging.info(f'Starting the data refinement process')
    
    try:

        conn_read = engineSqlAlchemy(HOST,USER,PASSWORD,3306,DB_READ)
        conn_write = engineSqlAlchemy(HOST,USER,PASSWORD,3306,DB_WRITE)

        df_movies = pd.read_sql_table('yts_movies',conn_read)

        drop_columns = ['title_english','title_long','slug','description_full','peers',
                        'synopsis','mpa_rating','background_image','seeds','url_y',
                        'background_image_original','small_cover_image','date_uploaded_unix_x',
                        'state','date_uploaded_unix_y','medium_cover_image','hash']

        rename_columns = {
            "url_x":"url_yts",
            "date_uploaded_y":"uploaded_torrent_at",
            "date_uploaded_x":"uploaded_content_at",
            "large_cover_image":"banner_image"
            }

        # Getting the maximum quality from each movie 
        df_aux = df_movies.groupby(['id']).agg({'size_bytes':'max'})
        df_movie = df_movies.merge(df_aux, left_on=['id','size_bytes'], right_on=['id','size_bytes'],how='inner')

        df_movie = df_movie.drop(drop_columns,axis=1)
        df_movie = df_movie.drop_duplicates().reset_index(drop=True)
        df_movie = df_movie.rename(rename_columns,axis=1)

        df_movie['title'] = df_movie['title'].str.upper()
        df_movie['language'] = df_movie['language'].str.upper()
        df_movie['type'] = df_movie['type'].str.upper()

        df_movie['uploaded_torrent_at'] = pd.to_datetime(df_movie['uploaded_torrent_at'],errors='coerce')
        df_movie['uploaded_content_at'] = pd.to_datetime(df_movie['uploaded_content_at'],errors='coerce')

        df_movie['loaded_at'] = pd.to_datetime(dt_now)
        df_movie['loaded_by'] = user

        df_movie.to_sql(name='yts_movies',con=conn_write,if_exists='replace',index=False)

        return df_movie

    except Exception as e:
        logging.error(f'Error to refinement data: {e}')
        raise TypeError(e)
    
    finally:
        logging.info('Ending the data refinement process')
