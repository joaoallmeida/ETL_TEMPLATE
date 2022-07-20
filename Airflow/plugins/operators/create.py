from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from connections.dbConnection import stringConnections
from functions.etlMonitor import control
import os
import logging


# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')


class runSql(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(self,context):

        logging.info('Starting creating databases')

        conn = BaseHook.get_connection('MySql Localhost')
        HOST=conn.host
        USER=conn.login
        PASSWORD=conn.password
        PORT=3306
        etlMonitor = control()
        db_connections = stringConnections()

        try:
            dbconn = db_connections.mysqlconnection(HOST,USER,PASSWORD,PORT)
            cursor = dbconn.cursor()

            for file in os.listdir(os.path.join('plugins','SQL')):
                
                logging.info(f'Reading file {file}')

                with open(os.path.join('plugins','SQL',file),'r') as script:
                    
                    for command in script.read().split(';'):
                        if len(command) > 0:
                            cursor.execute(command)
                        
                dbconn.commit()
                
            etlMonitor.InsertLog(1,'N/D','InProgress')
            
        except Exception as e:
            cursor.close()
            dbconn.close()
            logging.error(f'Error on create databases: {e}')
            etlMonitor.InsertLog(1,'N/D','Error',0,e)
            raise TypeError(e)
        finally:
            logging.info('Complete creation of databases')
        
        cursor.close()
        dbconn.close()
        etlMonitor.InsertLog(1,'N/D','Complete')