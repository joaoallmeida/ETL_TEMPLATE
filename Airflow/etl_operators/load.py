from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from .connections.dbConnection import stringConnections
from .utils.etlMonitor import control
from .utils.utilsFunctions import utils
from datetime import datetime
import pandas as pd
import pytz
import getpass
import socket
import logging


class Load(BaseOperator):

    ui_color='#66FF66'
    
    def __init__(self,tableId,**kwargs):

        super().__init__(**kwargs)
        conn = BaseHook.get_connection('MySql Localhost')
        self.host = conn.host
        self.user = conn.login
        self.password = conn.password
        self.port = conn.port
        self.dbRead ='silver'
        self.dbWrite ='gold'
        self.tableId = tableId
        self.etlMonitor = control()
        self.ut = utils()
        db_connections = stringConnections()

        self.dbConnRead = db_connections.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbRead)
        self.dbConnWrite = db_connections.engineSqlAlchemy(self.host,self.user,self.password,self.port,self.dbWrite)

    def createDimCalendar(self):

        logging.info('Creating Dim Calendar')
        self.etlMonitor.InsertLog(4,'DimCalendar','InProgress')

        def is_weekend(x):
            if x == 5 or x == 6:
                return 1
            else:
                return 0

        lastYear = (datetime.today().year - 1)
        currentYear = datetime.today().year

        days_names = {
                i: name
                for i, name
                in enumerate(['Monday', 'Tuesday', 'Wednesday',
                            'Thursday', 'Friday', 'Saturday', 
                            'Sunday'])
            }

        month_names = {
                i: name
                for i, name
                in enumerate(["None", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"])
            }

        try:
            dfCalendar = pd.DataFrame({"db_date": pd.date_range(datetime(lastYear,1,1).strftime('%Y-%m-%d'), datetime(currentYear,12,31).strftime('%Y-%m-%d'))})
            dfCalendar["time_id"] = dfCalendar.db_date.dt.strftime('%Y%m%d').astype(int)
            dfCalendar["db_year"] = dfCalendar.db_date.dt.year
            dfCalendar["db_month"] = dfCalendar.db_date.dt.month
            dfCalendar["db_day"] = dfCalendar.db_date.dt.day
            dfCalendar["db_quarter"] = dfCalendar.db_date.dt.quarter
            dfCalendar["db_semester"] = (dfCalendar.db_quarter + 1) // 2
            dfCalendar["db_week"] = dfCalendar.db_date.dt.strftime('%U').astype(int)
            dfCalendar["db_day_of_weak"] = dfCalendar.db_date.dt.dayofyear
            dfCalendar["nome_of_day"] = dfCalendar.db_date.dt.dayofweek.map(days_names.get)
            dfCalendar["day_of_weak"] = dfCalendar.db_date.dt.dayofweek
            dfCalendar["month_name"] = dfCalendar.db_date.dt.month.map(month_names.get)
            dfCalendar["is_weeked"] = dfCalendar.day_of_weak.apply(is_weekend).astype(bool)
            dfCalendar["is_leap_year"] = dfCalendar.db_date.dt.is_leap_year
            dfCalendar["create_by"] = self.user
            dfCalendar["updated_by"] = self.user
            dfCalendar["created_at"] = datetime.now()
            dfCalendar["updated_at"] = datetime.now()
            dfCalendar = dfCalendar.drop(["day_of_weak"], axis=1)

            lines = len(dfCalendar.index)

            dfCalendar.to_sql('DimCalendar',self.dbConnWrite,if_exists='replace',index=False)

            logging.info(f'Insert lines in Dim Calendar { lines }')
            logging.info('Completed creation Dim Calendar')

            self.etlMonitor.InsertLog(4,'DimCalendar','Complete',lines)

        except Exception as e:
            logging.error(f'Error to load start schema: {e}')
            self.etlMonitor.InsertLog(4,'DimCalendar','Error',0,e)
            raise TypeError(e)

    def createDimTorrent(self):

        logging.info('Creating Dim Torrent')
        self.etlMonitor.InsertLog(4,'DimTorrent','InProgress')

        dt_now = datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'

        torrent_columns = ["url_torrent","size","size_bytes"
                        ,"type","quality","language"
                        ,"uploaded_torrent_at"]

        try:
            df = pd.read_sql_table('yts_movies', self.dbConnRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)


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

        logging.info('Creating Dim Genres')
        self.etlMonitor.InsertLog(4,'DimGenres','InProgress')

        dt_now = datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'

        try:
            df = pd.read_sql_table('yts_movies',self.dbConnRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)

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

        logging.info('Creating Dim Movie')
        self.etlMonitor.InsertLog(4,'DimMovie','InProgress')

        dt_now = datetime.now(pytz.timezone('UTC'))
        user = f'{getpass.getuser()}@{socket.gethostname()}'

        try:
            df = pd.read_sql_table('yts_movies',self.dbConnRead).drop(['loaded_at','loaded_by','movie_sk'],axis=1)
  

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

        dt_now = datetime.now(pytz.timezone('UTC'))
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

            df_fat['time_id'] = int(dt_now.strftime('%Y%m%d'))
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

        elif self.tableId == 'DimGenres':
            self.createDimGenres()
        
        elif self.tableId == 'DimCalendar':    
            self.createDimCalendar()

        elif self.tableId == 'DimMovie':
            self.createDimMovie()

        elif self.tableId == 'FatFilms':
            self.createFatFilms()
        
        else:
            logging.warning(f'Table Id {self.tableId} not found !')

        logging.info('Completed process load start schema')