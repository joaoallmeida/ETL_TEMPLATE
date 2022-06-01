from ETL.Connections import db_connection
from ETL.Functions.etl_monitor import InsertLog
import os
import logging
from airflow.hooks.base import BaseHook

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

def createDB():

    logging.info('Starting creating databases')

    conn = BaseHook.get_connection('MySql Localhost')
    HOST=conn.host
    USER=conn.login
    PASSWORD=conn.password
    PORT=3306

    try:
        dbconn = db_connection.mysqlconnection(HOST,USER,PASSWORD,PORT)
        cursor = dbconn.cursor()

        for file in os.listdir(os.path.join('dags','ETL','SQL')):
            
            logging.info(f'Reading file {file}')

            with open(os.path.join('dags','ETL','SQL',file),'r') as script:
                
                for command in script.read().split(';'):
                    if len(command) > 0:
                        cursor.execute(command)
                    
            dbconn.commit()
            
        InsertLog(1,'N/D','InProgress')
        
    except Exception as e:
        cursor.close()
        dbconn.close()
        logging.error(f'Error on create databases: {e}')
        InsertLog(1,'N/D','Error',0,e)
        raise TypeError(e)
    finally:
        logging.info('Complete creation of databases')
    
    cursor.close()
    dbconn.close()
    InsertLog(1,'N/D','Complete')