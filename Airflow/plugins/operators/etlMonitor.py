
from dbConnection import stringConnections
from utilsFunctions import utils
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd

class control:

    def __init__(self):

        conn = BaseHook.get_connection('MySql Localhost')
        self.host = conn.host
        self.user = conn.login
        self.password = conn.password
        self.port = 3306
        self.db = 'monitoring'
        self.ut = utils()

        connections = stringConnections()

        self.dbConn = connections.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.db)
        self.mySqlConn = connections.mysqlconnection(self.host,self.user,self.password,self.port,self.db)

    def InsertLog(self, process_id, table, status, row_count = 0, error=None):

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
                df_log_insert.to_sql(log_table,self.dbConn,if_exists='append',index=False)
            
            elif status == 'Complete':

                df_log = pd.read_sql(log_table,self.dbConn)
                df_log = df_log[df_log['table_name'] == table].sort_values(by=['log_id'], ascending=False).head(1)
                df_log['complete_date'] = datetime.now()
                df_log['row_count'] = row_count
                df_log['status'] = status

                self.ut.InsertToMySQL(df_log, self.mySqlConn, log_table)

            else:

                df_log = pd.read_sql(log_table,self.dbConn)
                df_log = df_log[df_log['table_name'] == table].sort_values(by=['log_id'], ascending=False).head(1)
                df_log['complete_date'] = datetime.now()
                df_log['row_count'] = row_count
                df_log['status'] = status
                df_log['error_message'] = error

                self.ut.InsertToMySQL(df_log, self.mySqlConn, log_table)
            
        except Exception as e:
            raise e


