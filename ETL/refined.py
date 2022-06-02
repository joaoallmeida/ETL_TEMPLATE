from ETL.Connections.db_connection import engineSqlAlchemy, mysqlconnection
from ETL.Functions.utils_functions import *
from ETL.Functions.etl_monitor import InsertLog
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
def DataRefinement(TableName):

    logging.info(f'Starting the data refinement process')
    InsertLog(3,TableName,'InProgress')

    dt_now = datetime.datetime.now(pytz.timezone('UTC'))
    user = f'{getpass.getuser()}@{socket.gethostname()}'

    config = ConfigParser()
    config.read('ETL/Connections/credencials.ini')

    HOST=config['MySql']['host']
    USER=config['MySql']['user']
    PASSWORD=config['MySql']['pass']
    PORT = 3306
    DB_READ='bronze'
    DB_WRITE='silver'


    upper_columns = ['title','language','type','genre_0', 'genre_1', 'genre_2', 'genre_3']
    
    drop_columns = ['title_english','title_long','slug','description_full','peers',
                    'synopsis','mpa_rating','background_image','seeds','url_tt',
                    'background_image_original','small_cover_image','date_uploaded_unix_tt',
                    'state','date_uploaded_unix','medium_cover_image','hash']

    rename_columns = {
        "url":"url_yts",
        "date_uploaded_tt":"uploaded_torrent_at",
        "date_uploaded":"uploaded_content_at",
        "large_cover_image":"banner_image"
        }
    
    try:

        conn_read = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_READ)
        conn_write = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_WRITE)
        dbconn = mysqlconnection(HOST,USER,PASSWORD,PORT,DB_WRITE)

        df = pd.read_sql_table(TableName,conn_read)

        df = splitGenreColumn(df)
        df = getTorrentValue(df)
        df = upperString(df, upper_columns)
        df = df.drop(drop_columns,axis=1)
        df = df.drop_duplicates().reset_index(drop=True)
        df = df.rename(rename_columns,axis=1)

        df['uploaded_torrent_at'] = pd.to_datetime(df['uploaded_torrent_at'],errors='coerce')
        df['uploaded_content_at'] = pd.to_datetime(df['uploaded_content_at'],errors='coerce')
        df['loaded_at'] = pd.to_datetime(dt_now)
        df['loaded_by'] = user
        df.insert(0,'movie_sk',(df.index+1))

        logging.info('Loading refined table in MySQL')

        df.to_sql(TableName, conn_write, if_exists='append',index=False)

        logging.info('Completed load refined table in MySQL.')

        lines_number = len(df.index)
        InsertLog(3,TableName,'Complete',lines_number)
        
        logging.info(f'Refined lines {lines_number}')
        

    except Exception as e:
        logging.error(f'Error to refinement data: {e}')
        InsertLog(3,TableName,'Error',0,e)
        raise TypeError(e)
    
    finally:
        logging.info('Ending the data refinement process')
