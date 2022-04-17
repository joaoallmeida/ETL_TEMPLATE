from .Connections import db_connection
from configparser import ConfigParser
import os
import logging

log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def createDB():

    logging.info('Starting creating databases')

    config = ConfigParser()
    config.read('ETL/Connections/credencials.ini')

    HOST=config['MySql']['host']
    USER=config['MySql']['user']
    PASSWORD=config['MySql']['pass']
    PORT=3306

    try:
        dbconn = db_connection.mysqlconnection(HOST,USER,PASSWORD,PORT)
        cursor = dbconn.cursor()

        with open(os.path.join('ETL','SQL','create_database.sql'),'r') as script:
            sqlCommand = script.read().split('\n')
            
        for command in sqlCommand:
            cursor.execute(command)

        cursor.close()
        dbconn.close()

    except Exception as e:
        cursor.close()
        dbconn.close()
        logging.error(f'Error on create databases: {e}')
        raise TypeError(e)
    finally:
        logging.info('Complete creation of databases')
    