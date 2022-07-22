from ETL.connections.dbConnection import stringConnections
from ETL.utils.utilsFunctions import utils
from ETL.utils.etlMonitor import control
from configparser import ConfigParser

import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

class Refined:

    def __init__(self) -> None:
        
        config = ConfigParser()
        config.read('./credencials.ini')

        self.host=config['MySQL']['host']
        self.user=config['MySQL']['user']
        self.password=config['MySQL']['password']
        self.port=int(config['MySQL']['port'])
        self.dbRead='bronze'
        self.dbWrite='silver'

        self.utils= utils()
        self.control = control()
        self.connString = stringConnections()

        self.connRead = self.connString.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbRead)
        self.connWrite = self.connString.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbWrite)
        self.dbconn = self.connString.mysqlConnection(self.host,self.user,self.password,self.port,self.dbWrite)


    # Function responsible for refined the raw data.
    def execute(self,tableName):

        logging.info(f'Starting the data refinement process')
        self.control.InsertLog(3,tableName,'InProgress')

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

            df = pd.read_sql_table(tableName,self.connRead)
            df = self.utils.getChanges(df, tableName, self.connWrite)

            if len(df.index) > 0:

                df = self.utils.getTorrentValue(df)
                df = df.drop(drop_columns,axis=1)
                df = df.drop_duplicates().reset_index(drop=True)
                df = df.rename(rename_columns,axis=1)

                df['uploaded_torrent_at'] = pd.to_datetime(df['uploaded_torrent_at'],errors='coerce')
                df['uploaded_content_at'] = pd.to_datetime(df['uploaded_content_at'],errors='coerce')
                df['loaded_at'] = pd.to_datetime(dt_now)
                df['loaded_by'] = user

                logging.info('Loading refined table in MySQL')

                self.utils.InsertToMySQL(df,self.dbConn,tableName)

                logging.info('Completed load refined table in MySQL.')

                lines_number = len(df.index)
                
                logging.info(f'Refined lines {lines_number}')
                
            else:
                lines_number = 0
                logging.warning('Not found changes')

            self.control.InsertLog(3,tableName,'Complete',lines_number)

        except Exception as e:
            logging.error(f'Error to refinement data: {e}')
            self.control.InsertLog(3,tableName,'Error',0,e)
            raise TypeError(e)
        
        finally:
            logging.info('Ending the data refinement process')
