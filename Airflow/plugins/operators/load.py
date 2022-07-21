from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from dbConnection import stringConnections
from etlMonitor import control
from utilsFunctions import utils
import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

class starSchemaModel(BaseOperator):
    
    def __init__(self,tableId,**kwargs):

        super().__init__(**kwargs)
        conn = BaseHook.get_connection('MySql Localhost')
        self.host = conn.host
        self.user = conn.login
        self.password = conn.password
        self.port = 3306
        self.dbRead ='silver'
        self.dbWrite ='gold'
        self.tableId = tableId
        self.etlMonitor = control()
        self.ut = utils()
        db_connections = stringConnections()

        self.dbConnRead = db_connections.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbRead)
        self.dbConnWrite = db_connections.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbWrite)

    def createDimTorrent(self):

        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'

        try:
            df = pd.read_sql_table('yts_movies', self.dbConnRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)

            ## ----- ## -----## ----- ## -----## ----- ## -----
            # ## Dim Torrent
            logging.info('Creating Dim Torrent')

            self.etlMonitor.InsertLog(4,'DimTorrent','InProgress')

            torrent_columns = ["url_torrent","size","size_bytes"
                            ,"type","quality","language"
                            ,"uploaded_torrent_at"]

            df_torrent = df.copy()
            df_torrent = df_torrent[torrent_columns]
            df_torrent['created_at'] = pd.to_datetime(dt_now)
            df_torrent['updated_at'] = pd.to_datetime(dt_now)
            df_torrent['loaded_at'] = pd.to_datetime(dt_now)
            df_torrent['loaded_by'] = user
            df_torrent.insert(0, 'torrent_id' , (df_torrent.index+1) )

            df_torrent.to_sql('DimTorrent',self.dbConnWrite,if_exists='replace',index=False)
            
            lines = len(df_torrent.index)
            self.etlMonitor.InsertLog(4,'DimTorrent','Complete',lines)

            logging.info(f'Insert lines in Dim Torrent { lines }')
            logging.info('Completed creation Dim Torrent')

        except Exception as e:
            logging.error(f'Error to load start schema: {e}')
            self.etlMonitor.InsertLog(4,'DimTorrent','Error',0,e)
            raise TypeError(e)

    def createDimGenres(self):

        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'

        try:
            df = pd.read_sql_table('yts_movies',self.dbConnRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)

            ## ----- ## -----## ----- ## -----## ----- ## -----
            # ## Dim Genres

            logging.info('Creating Dim Genres')

            self.etlMonitor.InsertLog(4,'DimGenres','InProgress')

            genres_columns = ["genre_0","genre_1","genre_2","genre_3"]

            df_genres = df.copy()
            df_genres = self.ut.splitGenreColumn(df)
            df_genres = df_genres[genres_columns]
            df_genres = df_genres.drop_duplicates().reset_index(drop=True)
            df_genres = self.ut.upperString(df_genres, genres_columns)
            df_genres['created_at'] = pd.to_datetime(dt_now)
            df_genres['updated_at'] = pd.to_datetime(dt_now)
            df_genres['loaded_at'] = pd.to_datetime(dt_now)
            df_genres['loaded_by'] = user
            df_genres.insert(0, 'genre_id' , (df_genres.index+1))

            df_genres.to_sql('DimGenres',self.dbConnWrite,if_exists='replace',index=False)
            
            lines = len(df_genres.index)
            self.etlMonitor.InsertLog(4,'DimGenres','Complete',lines)

            logging.info(f'Insert lines in Dim Genres { lines }')
            logging.info('Completed creation Dim Genres')

        except Exception as e:
            logging.error(f'Error to load start schema: {e}')
            self.etlMonitor.InsertLog(4,'DimGenres','Error',0,e)
            raise TypeError(e)

    def createDimMovie(self):

        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'

        try:
            df = pd.read_sql_table('yts_movies',self.dbConnRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)
            ## ----- ## -----## ----- ## -----## ----- ## -----
            # ## Dim Movie

            logging.info('Creating Dim Movie')

            self.etlMonitor.InsertLog(4,'DimMovie','InProgress')

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

            df_movie.to_sql('DimMovie',self.dbConnWrite,if_exists='replace',index=False)
            
            lines = len(df_movie.index)
            self.etlMonitor.InsertLog(4,'DimMovie','Complete',lines)

            logging.info(f'Insert lines in Dim Movie { lines }')
            logging.info('Completed creation Dim Movie')

        except Exception as e:
            logging.error(f'Error to load start schema: {e}')
            self.etlMonitor.InsertLog(4,'DimMovie','Error',0,e)
            raise TypeError(e)

    def createFatFilms(self):

        ## ----- ## -----## ----- ## -----## ----- ## -----
        # ## Fat Movies

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

            df = pd.read_sql_table('yts_movies',self.dbConnRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)
            df_torrent = pd.read_sql_table('DimTorrent',self.dbConnWrite)
            df_genres = pd.read_sql_table('DimGenres',self.dbConnWrite)
            df_movie = pd.read_sql_table('DimMovie',self.dbConnWrite)
            
            logging.info('Creating Fat Film')
            self.etlMonitor.InsertLog(4,'FatFilm','InProgress')

            df = self.ut.splitGenreColumn(df)
            df = self.ut.upperString(df,genres_columns[:4])
            df_fat = pd.merge(df ,df_torrent ,how='inner' , on=torrent_columns[:7]).drop(torrent_columns ,axis=1)
            df_fat = pd.merge(df_fat ,df_genres ,how='inner' ,on=genres_columns[:4]).drop(genres_columns ,axis=1)
            df_fat = pd.merge(df_fat ,df_movie ,how='inner' ,on=movie_columns[1:10]).drop(movie_columns ,axis=1)
            df_fat = df_fat.drop_duplicates()
            df_fat['created_at'] = pd.to_datetime(dt_now)
            df_fat['updated_at'] = pd.to_datetime(dt_now)
            df_fat['loaded_at'] = pd.to_datetime(dt_now)
            df_fat['loaded_by'] = user
            df_fat = df_fat[['movie_id', 'torrent_id', 'genre_id', 'created_at', 'updated_at', 'extraction_at', 'extraction_by', 'loaded_at','loaded_by']]

            df_fat.to_sql('FatFilm',self.dbConnWrite,if_exists='replace',index=False)

            lines = len(df_fat.index)
            self.etlMonitor.InsertLog(4,'FatFilm','Complete',lines)

            logging.info(f'Insert lines in Fat Film { lines }')
            logging.info('Completed creation Fat Film')
            
        except Exception as e:
            logging.error(f'Error to load start schema: {e}')
            self.etlMonitor.InsertLog(4,'FatFilm','Error',0,e)
            raise TypeError(e)


    def execute(self,context):

        logging.info('Starting process load star schema')

        if self.tableId == 'DimTorrent':
            self.createDimTorrent()
        if self.tableId == 'DimGenres':
            self.createDimGenres()
        if self.tableId == 'DimMovie':
            self.createDimMovie()
        else:
            self.createFatFilms()

        logging.info('Completed process load start schema')