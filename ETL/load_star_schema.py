from ETL.Connections import db_connection
from configparser import ConfigParser

import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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

    dbcon_read = db_connection.engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_READ)
    dbcon_write = db_connection.engineSqlAlchemy(HOST,USER,PASSWORD,PORT,DB_WRITE)

    df = pd.read_sql_table('yts_movies',dbcon_read)

    ## ----- ## -----## ----- ## -----## ----- ## -----
    # ## Dim Torrent

    logging.info('Creating dim torrent')

    dict_columns_torrent = {
        "url_torrent":"TorrentURL",
        "size":"Size",
        "size_bytes":"Bytes",
        "type":"Type",
        "quality":"Quality",
        "language":"Language",
        "uploaded_torrent_at":"TorrentUploadedAt",
    }

    drop_cols_torrent = ['TorrentURL', 'Size', 'Bytes', 'Type', 'Quality'
                        ,'Language', 'TorrentUploadedAt', 'CreatedAt', 'UpdatedAt', 'LoadedAt'
                        ,'LoadedBy','url_torrent','size','size_bytes','type'
                        ,'quality','language','uploaded_torrent_at']

    df_torrent = df.copy()
    df_torrent = df[dict_columns_torrent.keys()]
    df_torrent = df_torrent.rename(dict_columns_torrent,axis=1)
    df_torrent['CreatedAt'] = pd.to_datetime(dt_now)
    df_torrent['UpdatedAt'] = pd.to_datetime(dt_now)
    df_torrent['LoadedAt'] = pd.to_datetime(dt_now)
    df_torrent['LoadedBy'] = user
    df_torrent.insert(0, 'TorrentId' , df_torrent.index )

    df_torrent.to_sql('DimTorrent',dbcon_write,if_exists='replace',index=False)

    logging.info('Completed creation dim torrent')
    ## ----- ## -----## ----- ## -----## ----- ## -----
    # ## Dim Genres

    logging.info('Creating Dim Genres')

    drop_columns_genres = ['Genres','CreatedAt','UpdatedAt','LoadedAt','LoadedBy','genre_0', 'genre_1', 'genre_2', 'genre_3',"extracting_at","loaded_at","loaded_by"]

    df_genres = df.copy()
    df_genres = df.loc[:,df.columns.str.startswith('genre')]
    df_genres = pd.melt(df_genres,value_name='Genres').drop(['variable'],axis=1)
    df_genres = df_genres.drop_duplicates()
    df_genres['CreatedAt'] = pd.to_datetime(dt_now)
    df_genres['UpdatedAt'] = pd.to_datetime(dt_now)
    df_genres['LoadedAt'] = pd.to_datetime(dt_now)
    df_genres['LoadedBy'] = user
    df_genres.insert(0, 'GenreId' , df_genres.index)

    df_genres.to_sql('DimGenres',dbcon_write,if_exists='replace',index=False)

    logging.info('Completed creation dim genres')
    ## ----- ## -----## ----- ## -----## ----- ## -----
    # ## Fat Movies

    logging.info('Creating Fat Movies')

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
        'uploaded_content_at':'UploadedContentAt',
        'TorrentId':'TorrentId',
        'GenreId':'GenreId'
    }

    df_fat = pd.merge(df ,df_torrent ,how='inner', left_on=list(dict_columns_torrent.keys()), right_on=list(dict_columns_torrent.values())).drop(drop_cols_torrent ,axis=1)
    df_fat = pd.merge(df_fat,df_genres, how='inner', left_on='genre_0', right_on='Genres').drop(drop_columns_genres,axis=1)
    df_fat = df_fat.rename(dict_colums_fat,axis=1)
    df_fat['CreatedAt'] = pd.to_datetime(dt_now)
    df_fat['UpdatedAt'] = pd.to_datetime(dt_now)
    df_fat['LoadedAt'] = pd.to_datetime(dt_now)
    df_fat['LoadedBy'] = user

    df_fat.to_sql('FatMovies',dbcon_write,if_exists='replace',index=False)

    logging.info('Completed creation fat movies')

    logging.info('Completed process load start schema')
