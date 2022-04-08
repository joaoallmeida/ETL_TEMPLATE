from .Connections.connection_api import getResponseData
from .Connections.db_connection import engineSqlAlchemy
from .Functions.transform_functions import pivotGenreColumn , addNewColumnToDF
from configparser import ConfigParser

import logging

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

config = ConfigParser()
config.read('ETL/Connections/credencials.ini')

HOST=config['MySql']['host']
USER=config['MySql']['user']
PASSWORD=config['MySql']['pass']
DB='db_movies_bronze'

# * Function responsible for extacting data from the api
def extractData():

    logging.info('Extracting data from API')
    
    try:
        # Creating SqlAlchemy engine for connect to database and doing a minimal transformation on the raw data to insert into table. 
        conn = engineSqlAlchemy(HOST,USER,PASSWORD,3306,DB)
        df, lines = getResponseData()
        df = pivotGenreColumn(df)
        df = addNewColumnToDF(df)
        
        df.to_sql(name='yts_movie',con=conn,if_exists='replace',index=False)
        
    except Exception as e:
        logging.error(f'Error in extract process: {e}',exc_info=False)
        raise TypeError(e)
    
    finally:
        logging.info(f'Torn lines: {lines}')
        logging.info('Completing extract from api')