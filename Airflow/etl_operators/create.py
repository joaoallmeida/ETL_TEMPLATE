from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from .connections.dbConnection import stringConnections
from .utils.etlMonitor import control
import os
import logging


# ## Inicial Config
# log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

class runSql(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        conn = BaseHook.get_connection('MySql Localhost')
        self.host=conn.host
        self.user=conn.login
        self.password=conn.password
        self.port=conn.port
        self.etlMonitor = control()
        self.db_connections = stringConnections()

    def execute(self,context):

        logging.info('Starting creating databases')

        try:
            dbconn = self.db_connections.mysqlconnection(self.host,self.user,self.password,self.port)
            cursor = dbconn.cursor()

            for file in os.listdir(os.path.join('etl_operators','SQL')):
                
                logging.info(f'Reading file {file}')

                with open(os.path.join('etl_operators','SQL',file),'r') as script:
                    
                    for command in script.read().split(';'):
                        if len(command) > 0:
                            cursor.execute(command)
                        
                dbconn.commit()
                
            self.etlMonitor.InsertLog(1,'N/D','InProgress')
            
        except Exception as e:
            cursor.close()
            dbconn.close()
            logging.error(f'Error on create databases: {e}')
            self.etlMonitor.InsertLog(1,'N/D','Error',0,e)
            raise TypeError(e)
        finally:
            logging.info('Complete creation of databases')
        
        cursor.close()
        dbconn.close()
        self.etlMonitor.InsertLog(1,'N/D','Complete')