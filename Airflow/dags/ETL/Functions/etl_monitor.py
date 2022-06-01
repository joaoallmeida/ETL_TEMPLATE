
from ..Connections.db_connection import engineSqlAlchemy, mysqlconnection
from .utils_functions import InsertToMySQL
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd

def InsertLog(process_id, table, status, row_count = 0, error=None):
    

    conn = BaseHook.get_connection('MySql Localhost')
    HOST=conn.host
    USER=conn.login
    PASSWORD=conn.password
    DB='monitoring'
    PORT=3306

    dbconn = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB)
    mysqlconn = mysqlconnection(HOST,USER,PASSWORD,PORT,DB)

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
            df_log = df_log[df_log['table_name'] == table].sort_values(by=['log_id'], ascending=False).head(1)
            df_log['complete_date'] = datetime.now()
            df_log['row_count'] = row_count
            df_log['status'] = status

            InsertToMySQL(df_log,mysqlconn,log_table)

        else:

            df_log = pd.read_sql(log_table,dbconn)
            df_log = df_log[df_log['table_name'] == table].sort_values(by=['log_id'], ascending=False).head(1)
            df_log['complete_date'] = datetime.now()
            df_log['row_count'] = row_count
            df_log['status'] = status
            df_log['error_message'] = error

            InsertToMySQL(df_log,mysqlconn,log_table)
        
    except Exception as e:
        raise e


