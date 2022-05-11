
from ..Connections.db_connection import engineSqlAlchemy, mysqlconnection
from .utils_functions import InsertToMySQL
from configparser import ConfigParser
from datetime import datetime
import pandas as pd

config = ConfigParser()
config.read('ETL/Connections/credencials.ini')

HOST=config['MySql']['host']
USER=config['MySql']['user']
PASSWORD=config['MySql']['pass']
DB='control'
PORT=3306

dbconn = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB)
mysqlconn = mysqlconnection(HOST,USER,PASSWORD,PORT,DB)

def InsertLog(process_id, table, status, row_count = 0, error=None):

    try:
        log_table = 'etl_logging'
        record = {  
                "process_id": process_id,
                "table_name": table,
                "start_date": datetime.now(),
                "complete_date": None,
                "row_count": row_count,
                "status": status,
                "error_message": error
            }

        if status == 'InProgress':

            df_log_insert = pd.DataFrame(record, index=[0])
            df_log_insert.to_sql(log_table,dbconn,if_exists='append',index=False)
        
        elif status == 'Complete':

            df_log = pd.read_sql(log_table,dbconn)
            df_log = df_log.loc[df_log['log_id'] == df_log['log_id'].max()]
            df_log['complete_date'] = datetime.now()
            df_log['row_count'] = row_count
            df_log['status'] = status

            InsertToMySQL(df_log,mysqlconn,log_table)

        else:

            df_log = pd.read_sql(log_table,dbconn)
            df_log = df_log.loc[df_log['log_id'] == df_log['log_id'].max()]
            df_log['complete_date'] = datetime.now()
            df_log['row_count'] = row_count
            df_log['status'] = status
            df_log['error_message'] = error

            InsertToMySQL(df_log,mysqlconn,log_table)
        
    except Exception as e:
        raise e


