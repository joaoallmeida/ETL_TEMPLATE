from mysql.connector import connect
import sqlalchemy
import logging

def mysqlconnection(host,user,password,db=''):
    db_conn = connect(
        host=host,
        user=user,
        password=password,
        database=db
    )
    return db_conn

def engineSqlAlchemy(host,user,password,port,db=''):
    try:
        urlDb = f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'
        engine = sqlalchemy.create_engine(urlDb)
        
        return engine

    except Exception as e:
        logging.error('Erro ao criar a engine')
        raise TypeError(e)
