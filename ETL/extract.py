from .Connections.connection_api import getResponseData
from .Connections.db_connection import engineSqlAlchemy, mysqlconnection
from .Functions.etl_monitor import InsertLog
from .Functions.utils_functions import *
from configparser import ConfigParser

import logging
import pandas as pd
import datetime
import pytz
import socket
import getpass

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

# * Function responsible for extacting data from the api
def ExtractData():

    logging.info('Extracting data from API')
    InsertLog(1,'yts_movies','InProgress')

    config = ConfigParser()
    config.read('ETL/Connections/credencials.ini')

    HOST=config['MySql']['host']
    USER=config['MySql']['user']
    PASSWORD=config['MySql']['pass']
    DB='bronze'
    PORT=3306
    
    dt_now = datetime.datetime.now(pytz.timezone('UTC'))
    user = f'{getpass.getuser()}@{socket.gethostname()}'
    
    try:
        # Creating SqlAlchemy engine and MySql Connection for connect to database and doing a minimal transformation on the raw data to insert into table. 
        
        mysqlconn = mysqlconnection(HOST,USER,PASSWORD,PORT,DB)
        dbconn = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB)
        
        df = getResponseData()
        df = addNewColumnToDF(df)
        df = pivotGenreColumn(df)
        df = df.drop_duplicates()

        df['extracting_at'] = pd.to_datetime(dt_now)
        df['extracting_by'] = user
        
        logging.info('Get load data')
        df = getChanges(df,'yts_movies',dbconn)
        
        logging.info('Start Incremental Load')
        InsertToMySQL(df,mysqlconn,'yts_movies')
        logging.info('Complete Incremental Load')

        lines = len(df.index)
        InsertLog(1,'yts_movies','Complete',lines)

        logging.info(f'Insert lines: {lines}')

    except Exception as e:
        logging.error(f'Error in extract process: {e}',exc_info=False)
        InsertLog(1,'yts_movies','Error',0,e)
        raise TypeError(e)
    
    finally:
        logging.info('Completing extract from api')