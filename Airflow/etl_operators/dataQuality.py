from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from .connections.dbConnection import stringConnections
from .utils.etlMonitor import control
import pandas as pd
import logging

class DataQuality(BaseOperator):

  ui_color = "#fad326"
  
  def __init__(self, tableName:list, **kwargs) -> None:
    
    super().__init__(**kwargs)
    conn = BaseHook.get_connection('MySql Localhost')
    self.host=conn.host
    self.user=conn.login
    self.password=conn.password
    self.port=conn.port
    self.db = 'gold'
    self.tableName = tableName
    self.etlMonitor = control()
    self.db_connections = stringConnections()

    self.connString = stringConnections()
    self.dbconn = self.connString.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.db)

  def execute(self,context):

    self.etlMonitor.InsertLog(5,'N/D','InProgress')

    try:

      for table in self.tableName:

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