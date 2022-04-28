from .Connections.connection_api import getResponseData
from .Connections.db_connection import engineSqlAlchemy, mysqlconnection
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

config = ConfigParser()
config.read('ETL/Connections/credencials.ini')

HOST=config['MySql']['host']
USER=config['MySql']['user']
PASSWORD=config['MySql']['pass']
DB='db_movies_bronze'
PORT=3306

# * Function responsible for extacting data from the api
def ExtractData():

    logging.info('Extracting data from API')
    
    dt_now = datetime.datetime.now(pytz.timezone('UTC'))
    user = f'{getpass.getuser()}@{socket.gethostname()}'
    
    try:
        # Creating SqlAlchemy engine and MySql Connection for connect to database and doing a minimal transformation on the raw data to insert into table. 
        
        mysqlconn = mysqlconnection(HOST,USER,PASSWORD,PORT,DB)
        dbconn = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB)
        
        df = getResponseData()
        df = pivotGenreColumn(df)
        df = addNewColumnToDF(df)
        
        df['extracting_at'] = pd.to_datetime(dt_now)
        df['extracting_by'] = user
        
        logging.info('Get load data')
        df = getChanges(df,'yts_movies',dbconn)
        
        logging.info('Incremental load')
        InsertToMySQL(df,mysqlconn,'yts_movies')
        
        lines = len(df.index)
        logging.info(f'Insert lines: {lines}')
        
    except Exception as e:
        logging.error(f'Error in extract process: {e}',exc_info=False)
        raise TypeError(e)
    
    finally:
        logging.info('Completing extract from api')