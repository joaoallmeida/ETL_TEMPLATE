from ETL.connections.apiRequest import apiRequest
from ETL.connections.dbConnection import stringConnections
from ETL.utils.etlMonitor import control
from ETL.utils.utilsFunctions import utils
from configparser import ConfigParser

import logging
import pandas as pd
import datetime
import pytz
import socket
import getpass

class Extract:

    def __init__(self):

        config = ConfigParser()
        config.read('./credencials.ini')

        self.host=config['MySQL']['host']
        self.user=config['MySQL']['user']
        self.password=config['MySQL']['password']
        self.port=int(config['MySQL']['port'])
        self.database = 'bronze'

        self.utils= utils()
        self.control = control()
        self.apiRequest = apiRequest()
        self.connString = stringConnections()
    
        self.mysqlConn = self.connString.mysqlConnection(self.host,self.user,self.password,self.port,self.database)
        self.dbConn = self.connString.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.database)

    # * Function responsible for extacting data from the api
    def execute(self,tableName):

        logging.info('Extracting data from API')

        self.control.InsertLog(2,'yts_movies','InProgress')
        
        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'
        
        try:
            
            df = self.apiRequest.getResponseData()
            df['extraction_at'] = dt_now
            df['extraction_by'] = user
            
            logging.info('Get load data')
            df = self.utils.getChanges(df,tableName,self.dbConn)
            
            logging.info('Start Incremental Load')
            self.utils.InsertToMySQL(df,self.mysqlConn,tableName)
            logging.info('Complete Incremental Load')

            lines = len(df.index)
            self.control.InsertLog(2,tableName,'Complete',lines)

            logging.info(f'Insert lines: {lines}')

        except Exception as e:
            logging.error(f'Error in extract process: {e}',exc_info=False)
            self.control.InsertLog(2,tableName,'Error',0,e)

            raise TypeError(e)
        
        finally:
            logging.info('Completing extract from api')