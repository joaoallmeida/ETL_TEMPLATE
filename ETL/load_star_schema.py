from .Connections.db_connection import engineSqlAlchemy
from .Functions.etl_monitor import InsertLog
from configparser import ConfigParser

import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

def LoadStartSchema():

    logging.info('Starting process load star schema')

    dt_now = datetime.datetime.now(pytz.timezone('UTC'))
    user = f'{getpass.getuser()}@{socket.gethostname()}'

    config = ConfigParser()
    config.read('ETL/Connections/credencials.ini')

    HOST=config['MySql']['host']
    USER=config['MySql']['user']
    PASSWORD=config['MySql']['pass']
    PORT=3306
    DB_READ='db_movies_silver'
    DB_WRITE='db_movies_gold'

    dbcon_read = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_READ)
    dbcon_write = engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_WRITE)

    try:

        df = pd.read_sql_table('yts_movies',dbcon_read).drop('movie_sk',axis=1)

        ## ----- ## -----## ----- ## -----## ----- ## -----
        # ## Dim Torrent
        logging.info('Creating Dim Torrent')

        InsertLog(3,'DimTorrent','InProgress')

        dict_columns_torrent = {
            "url_torrent":"TorrentURL",
            "size":"Size",
            "size_bytes":"Bytes",
            "type":"Type",
            "quality":"Quality",
            "language":"Language",
            "uploaded_torrent_at":"TorrentUploadedAt",
        }

        df_torrent = df.copy()
        df_torrent = df[dict_columns_torrent.keys()]
        df_torrent = df_torrent.rename(dict_columns_torrent,axis=1)
        df_torrent['CreatedAt'] = pd.to_datetime(dt_now)
        df_torrent['UpdatedAt'] = pd.to_datetime(dt_now)
        df_torrent['LoadedAt'] = pd.to_datetime(dt_now)
        df_torrent['LoadedBy'] = user
        df_torrent.insert(0, 'TorrentId' , (df_torrent.index+1) )

        df_torrent.to_sql('DimTorrent',dbcon_write,if_exists='replace',index=False)
        
        lines = len(df_torrent.index)
        InsertLog(3,'DimTorrent','Complete',lines)

        logging.info(f'Insert lines in Dim Torrent { lines }')
        logging.info('Completed creation Dim Torrent')

    except Exception as e:
        logging.error(f'Error to load start schema: {e}')
        InsertLog(3,'DimTorrent','Error',0,e)
        raise TypeError(e)

    try:
        ## ----- ## -----## ----- ## -----## ----- ## -----
        # ## Dim Genres

        logging.info('Creating Dim Genres')

        InsertLog(3,'DimGenres','InProgress')

        dict_rename = {
                        "id":"MovieId",
                        "genre_0":"Genre0",
                        "genre_1":"Genre1",
                        "genre_2":"Genre2",
                        "genre_3":"Genre3",
                        "genre_4":"Genre4"
                      }
        
        df_genres = df.copy()
        df_genres = df.loc[:,df.columns.str.startswith(('id','genre'))]
        df_genres = df_genres.drop_duplicates().reset_index(drop=True)
        df_genres = df_genres.rename(dict_rename,axis=1)
        df_genres['CreatedAt'] = pd.to_datetime(dt_now)
        df_genres['UpdatedAt'] = pd.to_datetime(dt_now)
        df_genres['LoadedAt'] = pd.to_datetime(dt_now)
        df_genres['LoadedBy'] = user
        df_genres.insert(0, 'GenreId' , (df_genres.index+1))

        df_genres.to_sql('DimGenres',dbcon_write,if_exists='replace',index=False)
        
        lines = len(df_genres.index)
        InsertLog(3,'DimGenres','Complete',lines)

        logging.info(f'Insert lines in Dim Genres { lines }')
        logging.info('Completed creation Dim Genres')

    except Exception as e:
        logging.error(f'Error to load start schema: {e}')
        InsertLog(3,'DimGenres','Error',0,e)
        raise TypeError(e)

    try:
        ## ----- ## -----## ----- ## -----## ----- ## -----
        # ## Fat Movies

        logging.info('Creating Fat Movies')
        InsertLog(3,'FatMovies','InProgress')

        drop_columns = ['TorrentURL', 'Size', 'Bytes', 'Type', 'Quality'
                        ,'Language', 'TorrentUploadedAt', 'CreatedAt_x', 'UpdatedAt_x', 'LoadedAt_x'
                        ,'LoadedBy_x','url_torrent','size','size_bytes','type'
                        ,'quality','language','uploaded_torrent_at'
                        ,'Genre0', 'Genre1', 'Genre2', 'Genre3'
                        ,'Genre4',"extracting_at","loaded_at","loaded_by","genre_0"
                        ,"genre_1","genre_2","genre_3","genre_4" , 'CreatedAt_y', 'UpdatedAt_y' ,'LoadedAt_y' ,'LoadedBy_y']

        dict_colums_fat = {
            'id':'MovieId',
            'url_yts':'YtsURL',
            'imdb_code':'IMDB',
            'title':'Title',
            'year':'Year',
            'rating':'Rating',
            'runtime':'Runtime',
            'summary':'Summary',
            'yt_trailer_code':'TrailerCode',
            'banner_image':'Banner',
            'uploaded_content_at':'UploadedContentAt'
        }

        df_fat = pd.merge(df ,df_torrent ,how='inner', left_on=list(dict_columns_torrent.keys()), right_on=list(dict_columns_torrent.values()))
        df_fat = pd.merge(df_fat,df_genres, how='inner', left_on='id', right_on='MovieId')
        df_fat = df_fat.drop(drop_columns,axis=1)
        df_fat = df_fat.rename(dict_colums_fat,axis=1)
        df_fat['CreatedAt'] = pd.to_datetime(dt_now)
        df_fat['UpdatedAt'] = pd.to_datetime(dt_now)
        df_fat['LoadedAt'] = pd.to_datetime(dt_now)
        df_fat['LoadedBy'] = user

        df_fat.to_sql('FatMovies',dbcon_write,if_exists='replace',index=False)

        lines = len(df_fat.index)
        InsertLog(3,'FatMovies','Complete',lines)

        logging.info(f'Insert lines in Fat Movies { lines }')
        logging.info('Completed creation Fat Movies')
        
    except Exception as e:
        logging.error(f'Error to load start schema: {e}')
        InsertLog(3,'FatMovies','Error',0,e)
        raise TypeError(e)
    
    finally:
        logging.info('Completed process load start schema')
        