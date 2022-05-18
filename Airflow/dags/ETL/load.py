from ETL.Connections.db_connection import engineSqlAlchemy
from ETL.Functions.etl_monitor import InsertLog
from airflow.hooks.base import BaseHook

import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

def LoadStartSchema():

    logging.info('Starting process load star schema')

    dt_now = datetime.datetime.now(pytz.timezone('UTC'))
    user = f'{getpass.getuser()}@{socket.gethostname()}'

    conn = BaseHook.get_connection('MySql Localhost')
    HOST=conn.host
    USER=conn.login
    PASSWORD=conn.password
    PORT=3306
    DB_READ='silver'
    DB_WRITE='gold'

    dbcon_read = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_READ)
    dbcon_write = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_WRITE)

    try:
        
        df = pd.read_sql_table('yts_movies',dbcon_read).drop(['loaded_at','loaded_by','movie_sk'],axis=1)

        ## ----- ## -----## ----- ## -----## ----- ## -----
        # ## Dim Torrent
        logging.info('Creating Dim Torrent')

        InsertLog(4,'DimTorrent','InProgress')


        torrent_columns = ["url_torrent","size","size_bytes"
                        ,"type","quality","language"
                        ,"uploaded_torrent_at",'created_at','updated_at'
                        ,'loaded_at','loaded_by']

        df_torrent = df.copy()
        df_torrent = df_torrent[torrent_columns[:7]]
        df_torrent['created_at'] = pd.to_datetime(dt_now)
        df_torrent['updated_at'] = pd.to_datetime(dt_now)
        df_torrent['loaded_at'] = pd.to_datetime(dt_now)
        df_torrent['loaded_by'] = user
        df_torrent.insert(0, 'torrent_id' , (df_torrent.index+1) )

        df_torrent.to_sql('DimTorrent',dbcon_write,if_exists='replace',index=False)
        
        lines = len(df_torrent.index)
        InsertLog(4,'DimTorrent','Complete',lines)

        logging.info(f'Insert lines in Dim Torrent { lines }')
        logging.info('Completed creation Dim Torrent')

    except Exception as e:
        logging.error(f'Error to load start schema: {e}')
        InsertLog(4,'DimTorrent','Error',0,e)
        raise TypeError(e)

    try:
        ## ----- ## -----## ----- ## -----## ----- ## -----
        # ## Dim Genres

        logging.info('Creating Dim Genres')

        InsertLog(4,'DimGenres','InProgress')

        genres_columns = ["genres",'created_at'
                        ,'updated_at','loaded_at','loaded_by']

        df_genres = df.copy()
        df_genres = df_genres[genres_columns[:1]]
        df_genres = df_genres.drop_duplicates().reset_index(drop=True)
        df_genres['created_at'] = pd.to_datetime(dt_now)
        df_genres['updated_at'] = pd.to_datetime(dt_now)
        df_genres['loaded_at'] = pd.to_datetime(dt_now)
        df_genres['loaded_by'] = user
        df_genres.insert(0, 'genre_id' , (df_genres.index+1))

        df_genres.to_sql('DimGenres',dbcon_write,if_exists='replace',index=False)
        
        lines = len(df_genres.index)
        InsertLog(4,'DimGenres','Complete',lines)

        logging.info(f'Insert lines in Dim Genres { lines }')
        logging.info('Completed creation Dim Genres')

    except Exception as e:
        logging.error(f'Error to load start schema: {e}')
        InsertLog(4,'DimGenres','Error',0,e)
        raise TypeError(e)

    try:
        ## ----- ## -----## ----- ## -----## ----- ## -----
        # ## Dim Movie

        logging.info('Creating Dim Movie')

        InsertLog(4,'DimMovie','InProgress')

        movie_columns = ['id','url_yts', 'title', 'summary', 'banner_image',"imdb_code",	"year",	"rating"
                        ,"runtime" ,"yt_trailer_code",'uploaded_content_at','updated_at','loaded_at','loaded_by']

        movie_rename = {
                    'id':'movie_id'
                    }

        df_movie = df.copy()
        df_movie = df_movie[movie_columns[:10]]
        df_movie = df_movie.drop_duplicates().reset_index(drop=True)
        df_movie = df_movie.rename(movie_rename,axis=1)
        df_movie['created_at'] = pd.to_datetime(dt_now)
        df_movie['updated_at'] = pd.to_datetime(dt_now)
        df_movie['loaded_at'] = pd.to_datetime(dt_now)
        df_movie['loaded_by'] = user

        df_movie.to_sql('DimMovie',dbcon_write,if_exists='replace',index=False)
        
        lines = len(df_movie.index)
        InsertLog(4,'DimMovie','Complete',lines)

        logging.info(f'Insert lines in Dim Movie { lines }')
        logging.info('Completed creation Dim Movie')

    except Exception as e:
        logging.error(f'Error to load start schema: {e}')
        InsertLog(4,'DimMovie','Error',0,e)
        raise TypeError(e)

    try:
        ## ----- ## -----## ----- ## -----## ----- ## -----
        # ## Fat Movies

        logging.info('Creating Fat Movies')
        InsertLog(4,'FatMovies','InProgress')

        df_fat = pd.merge(df ,df_torrent ,how='inner' , on=torrent_columns[:7]).drop(torrent_columns ,axis=1)
        df_fat = pd.merge(df_fat ,df_genres ,how='inner' ,on=genres_columns[:1]).drop(genres_columns ,axis=1)
        df_fat = pd.merge(df_fat ,df_movie ,how='inner' ,on=movie_columns[1:10]).drop(movie_columns ,axis=1)
        df_fat['created_at'] = pd.to_datetime(dt_now)
        df_fat['updated_at'] = pd.to_datetime(dt_now)
        df_fat['loaded_at'] = pd.to_datetime(dt_now)
        df_fat['loaded_by'] = user
        df_fat = df_fat[['movie_id', 'torrent_id', 'genre_id', 'created_at', 'updated_at', 'extraction_at', 'extraction_by', 'loaded_at','loaded_by']]
        
        df_fat.to_sql('FatMovies',dbcon_write,if_exists='replace',index=False)

        lines = len(df_fat.index)
        InsertLog(4,'FatMovies','Complete',lines)

        logging.info(f'Insert lines in Fat Movies { lines }')
        logging.info('Completed creation Fat Movies')
        
    except Exception as e:
        logging.error(f'Error to load start schema: {e}')
        InsertLog(4,'FatMovies','Error',0,e)
        raise TypeError(e)
    
    finally:
        logging.info('Completed process load start schema')
        