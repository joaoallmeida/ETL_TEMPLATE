import sqlalchemy
import logging
import pymysql

class stringConnections:
    def __init__(self) -> None:
        pass

    def mysqlConnection(self,host,user,password,port,db=''):
        try:

            db_conn = pymysql.connect(
                host=host,
                user=user,
                port=port,
                password=password,
                database=db
        )

        except Exception as e:
            raise e

        return db_conn

    def engineSqlAlchemy(self,host,user,password,port,db=''):
        try:

            urlDb = f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'
            engine = sqlalchemy.create_engine(urlDb)
            
        except Exception as e:
            logging.error('Erro ao criar a engine')
            raise TypeError(e)

        return engine
