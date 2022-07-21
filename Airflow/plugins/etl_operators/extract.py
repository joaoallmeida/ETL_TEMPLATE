from connectionApi import sourceApi
from dbConnection import stringConnections
from etlMonitor import control
from utilsFunctions import utils
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

import logging
import pandas as pd
import datetime
import pytz
import socket
import getpass


# ## Inicial Config
# log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

class extractRawData(BaseOperator) :

    def __init__(self, tableName,**kwargs):
        
        super().__init__(**kwargs)
        conn = BaseHook.get_connection('MySql Localhost')
        self.host = conn.host
        self.user = conn.login
        self.password = conn.password
        self.port = 3306
        self.db ='bronze'
        self.tableName = tableName
        self.etlMonitor = control()
        self.ut = utils()
        self.source = sourceApi()
        db_connections = stringConnections()

        # Creating SqlAlchemy engine and MySql Connection for connect to database. 
        self.mySqlConn = db_connections.mysqlconnection(self.host,self.user,self.password,self.port,self.db)
        self.dbConn = db_connections.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.db)
        

    # * Function responsible for extacting data from the api
    def execute(self,context):

        logging.info('Extracting data from API')
        self.etlMonitor.InsertLog(2,self.tableName,'InProgress')

        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'
        
        try:
            
            df = self.source.getResponseData()
            df['extraction_at'] = pd.to_datetime(dt_now)
            df['extraction_by'] = user
            
            logging.info('Get load data')
            df = self.ut.getChanges(df,self.tableName, self.dbConn)
            
            logging.info('Start Incremental Load')
            self.ut.InsertToMySQL(df,self.mySqlConn,self.tableName)
            logging.info('Complete Incremental Load')

            lines = len(df.index)
            self.etlMonitor.InsertLog(2,self.tableName,'Complete',lines)

            logging.info(f'Insert lines: {lines}')

        except Exception as e:
            logging.error(f'Error in extract process: {e}',exc_info=False)
            self.etlMonitor.InsertLog(2,self.tableName,'Error',0,e)
            raise TypeError(e)
        
        finally:
            logging.info('Completing extract from api')