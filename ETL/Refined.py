
from .Functions.UnitFunctions import addNewColumnToDF
import pandas as pd
import datetime
import pytz
import getpass
import socket
import logging

def dataRefinement():
    
    logging.info(f'Starting the data refinement process')
    
    try:

        user = f'{getpass.getuser()}@{socket.gethostname()}'
        dt_now = datetime.datetime.now(pytz.timezone('UTC'))
        dt_format = dt_now.strftime("%Y%m%d")


        df_movies = pd.read_parquet(f'./01.Bronze/yts_movies_{dt_format}.parquet')
        df_movies = addNewColumnToDF(df_movies)


        drop_columns = ['title_english','title_long','slug','description_full','peers',
                        'synopsis','mpa_rating','background_image','torrents','seeds','url_y',
                        'background_image_original','small_cover_image','date_uploaded_unix_x',
                        'state','date_uploaded_unix_y','medium_cover_image','genres']

        rename_columns = {
            "url_x":"url_yts",
            "date_uploaded_y":"date_uploaded_torrent",
            "date_uploaded_x":"date_uploaded_content",
            "large_cover_image":"banner_image"
            }

        df_aux = df_movies.groupby(['id']).agg({'size_bytes':'max'})
        df_movie = df_movies.merge(df_aux, left_on=['id','size_bytes'], right_on=['id','size_bytes'],how='inner')
        df_movie[['genre_01','genre_02','genre_03']] = df_movie.apply(lambda x: pd.Series(x['genres']) ,axis=1)

        df_movie = df_movie.drop(drop_columns,axis=1).drop_duplicates().reset_index(drop=True)
        df_movie = df_movie.rename(rename_columns,axis=1)

        df_movie['title'] = df_movie['title'].str.upper()
        df_movie['language'] = df_movie['language'].str.upper()
        df_movie['type'] = df_movie['type'].str.upper()

        df_movie['date_uploaded_torrent'] = pd.to_datetime(df_movie['date_uploaded_torrent'],errors='coerce')
        df_movie['date_uploaded_content'] = pd.to_datetime(df_movie['date_uploaded_content'],errors='coerce')

        df_movie['date_loaded'] = pd.to_datetime(dt_now)
        df_movie['loaded_by'] = user

        df_movie.to_parquet(f'./02.Silver/yts_movies_{dt_format}.parquet')
    
    except Exception as e:
        logging.error(f'Error to refinement data: {e}')
        raise TypeError(e)
    
    finally:
        logging.info('Ending the data refinement process')
    