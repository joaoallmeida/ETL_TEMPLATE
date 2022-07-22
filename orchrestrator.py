from ETL.create import Create
from ETL.extract import Extract
from ETL.refined import Refined
from ETL.load import Load
import logging

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

def main(event):
    
    logging.info(f'Starting Process')

    create = Create()
    extract = Extract()
    refined = Refined()
    load = Load()

    create.execute()
    extract.execute('yts_movies')
    refined.execute('yts_movies')

    for table in event['tables']:
        load.execute(table)
    
    logging.info('Completing Process')
    
if __name__=='__main__':

    event = {
        "tables": [
            'DimTorrent',
            'DimGenres',
            'DimMovie',
            'FatFilms'
        ]
    }

    main(event)