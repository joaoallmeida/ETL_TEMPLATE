from ETL.connections.dbConnection import stringConnections
from ETL.utils.etlMonitor import control
from configparser import ConfigParser
import os
import logging

class Create:

    def __init__(self) -> None:

        config = ConfigParser()
        config.read('./credencials.ini')

        self.host=config['MySQL']['host']
        self.user=config['MySQL']['user']
        self.password=config['MySQL']['password']
        self.port=int(config['MySQL']['port'])
        
        self.control = control()
        self.connString = stringConnections()

        self.dbconn = self.connString.mysqlConnection(self.host,self.user,self.password,self.port)

    def execute(self):

        logging.info('Starting creating databases')

        try:
            cursor = self.dbconn.cursor()

            for file in os.listdir(os.path.join('ETL','SQL')):
                
                logging.info(f'Reading file {file}')

                with open(os.path.join('ETL','SQL',file),'r') as script:
                    
                    for command in script.read().split(';'):
                        if len(command) > 0:
                            cursor.execute(command)
                        
                self.dbconn.commit()
                
            self.control.InsertLog(1,'N/D','InProgress')

        except Exception as e:
            logging.error(f'Error on create databases: {e}')
            cursor.close()
            self.dbconn.close()
            self.control.InsertLog(1,'N/D','Error',0,e)

            raise TypeError(e)

        finally:
            logging.info('Complete creation of databases')
        
        cursor.close()
        self.dbconn.close()
        self.control.InsertLog(1,'N/D','Complete')