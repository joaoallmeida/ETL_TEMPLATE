import sqlalchemy
import logging
import pymysql

class stringConnections: 
    def __init__(self) -> None:
        pass
    
    def mysqlconnection(self,host,user,password,port,db=''):
        db_conn = pymysql.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            database=db
        )
        return db_conn

    def engineSqlAlchemy(self,host,user,password,port,db=''):
        try:
            urlDb = f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'
            engine = sqlalchemy.create_engine(urlDb)
            
            return engine

        except Exception as e:
            logging.error('Erro ao criar a engine')
            raise TypeError(e)
