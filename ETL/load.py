from ETL.connections.dbConnection import stringConnections
from ETL.utils.etlMonitor import control
from ETL.utils.utilsFunctions import utils
from configparser import ConfigParser

import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

class Load:

    def __init__(self) -> None:

        config = ConfigParser()
        config.read('./credencials.ini')

        self.host=config['MySQL']['host']
        self.user=config['MySQL']['user']
        self.password=config['MySQL']['password']
        self.port=int(config['MySQL']['port'])
        self.dbRead='silver'
        self.dbWrite='gold'
        
        self.utils= utils()
        self.control = control()
        self.connString = stringConnections()

        self.connRead = self.connString.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbRead)
        self.connWrite = self.connString.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbWrite)

    def createDimTorrent(self):

        logging.info('Creating Dim Torrent')
        self.control.InsertLog(4,'DimTorrent','InProgress')

        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'

        try:

            df = pd.read_sql_table('yts_movies',self.connRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)

            torrent_columns = ["url_torrent","size","size_bytes"
                            ,"type","quality","language"
                            ,"uploaded_torrent_at"]

            df_torrent = df.copy()
            df_torrent = df_torrent[torrent_columns[:7]]
            df_torrent['created_at'] = pd.to_datetime(dt_now)
            df_torrent['updated_at'] = pd.to_datetime(dt_now)
            df_torrent['loaded_at'] = pd.to_datetime(dt_now)
            df_torrent['loaded_by'] = user
            df_torrent.insert(0, 'torrent_id' , (df_torrent.index+1) )

            df_torrent.to_sql('DimTorrent',self.connWrite,if_exists='replace',index=False)
            
            lines = len(df_torrent.index)
            self.control.InsertLog(4,'DimTorrent','Complete',lines)

            logging.info(f'Insert lines in Dim Torrent { lines }')
            logging.info('Completed creation Dim Torrent')

        except Exception as e:
            logging.error(f'Error to load start schema: {e}')
            self.control.InsertLog(4,'DimTorrent','Error',0,e)
            raise TypeError(e)

    def createDimGenres(self):

        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'

        try:
            df = pd.read_sql_table('yts_movies',self.connRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)

  
            logging.info('Creating Dim Genres')

            self.control.InsertLog(4,'DimGenres','InProgress')

            genres_columns = ["genre_0","genre_1","genre_2","genre_3"]

            df_genres = df.copy()
            df_genres = self.utils.splitGenreColumn(df)
            df_genres = df_genres[genres_columns]
            df_genres = df_genres.drop_duplicates().reset_index(drop=True)
            df_genres = self.utils.upperString(df_genres, genres_columns)
            df_genres['created_at'] = pd.to_datetime(dt_now)
            df_genres['updated_at'] = pd.to_datetime(dt_now)
            df_genres['loaded_at'] = pd.to_datetime(dt_now)
            df_genres['loaded_by'] = user
            df_genres.insert(0, 'genre_id' , (df_genres.index+1))

            df_genres.to_sql('DimGenres',self.connWrite,if_exists='replace',index=False)
            
            lines = len(df_genres.index)
            self.control.InsertLog(4,'DimGenres','Complete',lines)

            logging.info(f'Insert lines in Dim Genres { lines }')
            logging.info('Completed creation Dim Genres')

        except Exception as e:
            logging.error(f'Error to load start schema: {e}')
            self.control.InsertLog(4,'DimGenres','Error',0,e)
            raise TypeError(e)

    def createDimMovie(self):

        logging.info('Creating Dim Movie')
        self.control.InsertLog(4,'DimMovie','InProgress')

        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'

        try:
            df = pd.read_sql_table('yts_movies',self.connRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)
       
            movie_columns = ['id','url_yts', 'title', 'summary', 'banner_image',"imdb_code",	"year",	"rating"
                            ,"runtime" ,"yt_trailer_code",'uploaded_content_at']

            movie_rename = {
                        'id':'movie_id'
                        }

            df_movie = df.copy()
            df_movie = df_movie[movie_columns[:10]]
            df_movie = df_movie.drop_duplicates().reset_index(drop=True)
            df_movie = df_movie.rename(movie_rename,axis=1)
            df_movie['created_at'] = pd.to_datetime(dt_now)
            df_movie['updated_at'] = pd.to_datetime(dt_now)
            df_movie['loaded_at'] = pd.to_datetime(dt_now)
            df_movie['loaded_by'] = user

            df_movie.to_sql('DimMovie',self.connWrite,if_exists='replace',index=False)
            
            lines = len(df_movie.index)
            self.control.InsertLog(4,'DimMovie','Complete',lines)

            logging.info(f'Insert lines in Dim Movie { lines }')
            logging.info('Completed creation Dim Movie')

        except Exception as e:
            logging.error(f'Error to load start schema: {e}')
            self.control.InsertLog(4,'DimMovie','Error',0,e)

            raise TypeError(e)

    def createFatFilms(self):

        logging.info('Creating Fat Film')
        self.control.InsertLog(4,'FatFilm','InProgress')

        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'


        movie_columns = ['id','url_yts', 'title', 'summary', 'banner_image',"imdb_code",	"year",	"rating"
                            ,"runtime" ,"yt_trailer_code",'uploaded_content_at','updated_at','loaded_at','loaded_by']

        genres_columns = ["genre_0","genre_1","genre_2","genre_3",'created_at','updated_at','loaded_at','loaded_by']

        torrent_columns = ["url_torrent","size","size_bytes"
                            ,"type","quality","language"
                            ,"uploaded_torrent_at",'created_at','updated_at'
                            ,'loaded_at','loaded_by']
        try:

            df = pd.read_sql_table('yts_movies',self.connRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)
            df_torrent = pd.read_sql_table('DimTorrent',self.connWrite)
            df_genres = pd.read_sql_table('DimGenres',self.connWrite)
            df_movie = pd.read_sql_table('DimMovie',self.connWrite)

            df = self.utils.splitGenreColumn(df)
            df = self.utils.upperString(df,genres_columns[:4])
            df_fat = pd.merge(df ,df_torrent ,how='inner' , on=torrent_columns[:7]).drop(torrent_columns ,axis=1)
            df_fat = pd.merge(df_fat ,df_genres ,how='inner' ,on=genres_columns[:4]).drop(genres_columns ,axis=1)
            df_fat = pd.merge(df_fat ,df_movie ,how='inner' ,on=movie_columns[1:10]).drop(movie_columns ,axis=1)
            df_fat = df_fat.drop_duplicates()
            df_fat['created_at'] = pd.to_datetime(dt_now)
            df_fat['updated_at'] = pd.to_datetime(dt_now)
            df_fat['loaded_at'] = pd.to_datetime(dt_now)
            df_fat['loaded_by'] = user
            df_fat = df_fat[['movie_id', 'torrent_id', 'genre_id', 'created_at', 'updated_at', 'extraction_at', 'extraction_by', 'loaded_at','loaded_by']]

            df_fat.to_sql('FatFilm',self.connWrite,if_exists='replace',index=False)

            lines = len(df_fat.index)
            self.control.InsertLog(4,'FatFilm','Complete',lines)

            logging.info(f'Insert lines in Fat Film { lines }')
            logging.info('Completed creation Fat Film')
            
        except Exception as e:
            logging.error(f'Error to load start schema: {e}')
            self.control.InsertLog(4,'FatFilm','Error',0,e)

            raise TypeError(e)


    def execute(self,tableId):

        logging.info('Starting process load star schema')

        if tableId == 'DimTorrent':
            self.createDimTorrent()

        elif tableId == 'DimGenres':    
            self.createDimGenres()

        elif tableId == 'DimMovie':    
            self.createDimMovie()

        elif tableId == 'FatFilms':
            self.createFatFilms()

        else:
            logging.error(f'Table Id {tableId} not found!')    


        logging.info('Completed process load start schema')