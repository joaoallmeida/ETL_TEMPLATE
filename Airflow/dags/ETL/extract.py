from ETL.Connections.connection_api import getResponseData
from ETL.Connections.db_connection import engineSqlAlchemy, mysqlconnection
from ETL.Functions.etl_monitor import InsertLog
from ETL.Functions.utils_functions import *
from airflow.hooks.base import BaseHook

import logging
import pandas as pd
import datetime
import pytz
import socket
import getpass

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

# * Function responsible for extacting data from the api
def ExtractData(TableName):

    logging.info('Extracting data from API')
    InsertLog(2,TableName,'InProgress')

    conn = BaseHook.get_connection('MySql Localhost')
    HOST=conn.host
    USER=conn.login
    PASSWORD=conn.password
    PORT=3306
    DB='bronze'

    dt_now = datetime.datetime.now(pytz.timezone('UTC'))
    user = f'{getpass.getuser()}@{socket.gethostname()}'
    
    try:
        # Creating SqlAlchemy engine and MySql Connection for connect to database. 
        
        mysqlconn = mysqlconnection(HOST,USER,PASSWORD,PORT,DB)
        dbconn = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB)
        
        df = getResponseData()
        df = convertToJson(df,['genres','torrents'])

        df['extraction_at'] = pd.to_datetime(dt_now)
        df['extraction_by'] = user
        
        logging.info('Get load data')
        df = getChanges(df,TableName,dbconn)
        
        logging.info('Start Incremental Load')
        InsertToMySQL(df,mysqlconn,TableName)
        logging.info('Complete Incremental Load')

        lines = len(df.index)
        InsertLog(2,TableName,'Complete',lines)

        logging.info(f'Insert lines: {lines}')

    except Exception as e:
        logging.error(f'Error in extract process: {e}',exc_info=False)
        InsertLog(2,TableName,'Error',0,e)
        raise TypeError(e)
    
    finally:
        logging.info('Completing extract from api')