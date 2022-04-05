from .Connections.ConnectToAPI import getResponseData
from .Connections.DbConnection import engineSqlAlchemy
from .Functions.UnitFunctions import addNewColumnToDF
from configparser import ConfigParser

import logging
import datetime
import pytz

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
dt_now = datetime.datetime.now(pytz.timezone('UTC'))
dt_format = dt_now.strftime("%Y%m%d")

config = ConfigParser()
config.read('ETL/Connections/credencials.ini')

HOST=config['MySql']['host']
USER=config['MySql']['user']
PASSWORD=config['MySql']['pass']
DB='db_movies_bronze'

# # Extract Data
def extractData():

    logging.info('Extracting data from API')
    
    try:

        conn = engineSqlAlchemy(HOST,USER,PASSWORD,3306,DB)

        df, lines = getResponseData()
        # df = addNewColumnToDF(df)
        # df.to_parquet(f'./01.Bronze/yts_movies_{dt_now.strftime("%Y%m%d")}.parquet')
        df.to_sql(name='yts_movie',con=conn,if_exists='replace',index=False)
        
    except Exception as e:
        logging.error(f'Error in extract process: {e}',exc_info=False)
        raise TypeError(e)
    
    finally:
        logging.info(f'Torn lines: {lines}')
        logging.info('Completing extract from api')