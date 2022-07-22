from .connections.dbConnection import stringConnections
from .utils.utilsFunctions import utils
from .utils.etlMonitor import control
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator

import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

class Refined(BaseOperator):

    ui_color="#FFB266"

    def __init__(self,tableName,**kwargs):
   
        super().__init__(**kwargs)
        conn = BaseHook.get_connection('MySql Localhost')
        self.host = conn.host
        self.user = conn.login
        self.password = conn.password
        self.port = conn.port
        self.dbRead ='bronze'
        self.dbWrite ='silver'
        self.tableName = tableName
        self.etlMonitor = control()
        self.ut = utils()
        db_connections = stringConnections()

        self.dbConnRead = db_connections.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbRead)
        self.dbConnWrite = db_connections.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbWrite)
        self.dbConn = db_connections.mysqlconnection(self.host,self.user,self.password,self.port,self.dbWrite)
    
    # Function responsible for refined the raw data.
    def execute(self,context):

        logging.info(f'Starting the data refinement process')
        self.etlMonitor.InsertLog(3,self.tableName,'InProgress')

        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'


        drop_columns = ['title_english','title_long','slug','description_full','peers',
                        'synopsis','mpa_rating','background_image','seeds','url_tt',
                        'background_image_original','small_cover_image','date_uploaded_unix_tt',
                        'state','date_uploaded_unix','medium_cover_image','hash']

        rename_columns = {
            "url":"url_yts",
            "date_uploaded_tt":"uploaded_torrent_at",
            "date_uploaded":"uploaded_content_at",
            "large_cover_image":"banner_image"
            }
        
        try:

            df = pd.read_sql_table(self.tableName, self.dbConnRead)
            df = self.ut.getChanges(df, self.tableName, self.dbConnWrite)

            if len(df.index) > 0:
                
                df = self.ut.getTorrentValue(df)
                df = df.drop(drop_columns,axis=1)
                df = df.drop_duplicates().reset_index(drop=True)
                df = df.rename(rename_columns,axis=1)

                df['uploaded_torrent_at'] = pd.to_datetime(df['uploaded_torrent_at'],errors='coerce')
                df['uploaded_content_at'] = pd.to_datetime(df['uploaded_content_at'],errors='coerce')
                df['loaded_at'] = pd.to_datetime(dt_now)
                df['loaded_by'] = user

                logging.info('Loading refined table in MySQL')

                self.ut.InsertToMySQL(df ,self.dbConn ,self.tableName)

                logging.info('Completed load refined table in MySQL.')

                lines_number = len(df.index)
                
                logging.info(f'Refined lines {lines_number}')
            else:
                lines_number = 0
                logging.warning('Not found changes')

            self.etlMonitor.InsertLog(3, self.tableName, 'Complete', lines_number)

        except Exception as e:
            logging.error(f'Error to refinement data: {e}')
            self.etlMonitor.InsertLog(3, self.tableName, 'Error', 0, e)
            raise TypeError(e)
        
        finally:
            logging.info('Ending the data refinement process')
