from ETL.connections.dbConnection import stringConnections
from ETL.utils.etlMonitor import control
from configparser import ConfigParser
import pandas as pd
import logging

class DataQuality:
  
  def __init__(self) -> None:
    
    config = ConfigParser()
    config.read('./credencials.ini')

    self.host=config['MySQL']['host']
    self.user=config['MySQL']['user']
    self.password=config['MySQL']['password']
    self.port=int(config['MySQL']['port'])
    self.db='gold'
    self.etlMonitor = control()
    self.connString = stringConnections()
    self.dbconn = self.connString.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.db)

  def execute(self,tableName:list):

    self.etlMonitor.InsertLog(5,'N/D','InProgress')

    try:
      
      for table in tableName:

        logging.info(f'Checking data quality from table -> {table}')

        df = pd.read_sql_table(table,self.dbconn)
        countRecords = len(df.index)

        #check count records
        if countRecords < 1:
          raise ValueError(f'Data quality failed. Not found records in table {table}')

        #check duplicated 
        for dup in df.duplicated():
          if dup == True:
              raise ValueError(f'Found duplicate row in table {table}')

    except Exception as e:
      self.etlMonitor.InsertLog(5,'N/D','Error',0,e)
      raise e
    
    self.etlMonitor.InsertLog(5,'N/D','Complete')