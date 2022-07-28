from ETL.create import Create
from ETL.extract import Extract
from ETL.refined import Refined
from ETL.load import Load
from ETL.dataQuality import DataQuality
import logging

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

def main(event):
    
    logging.info(f'Starting Process')

    create = Create()
    extract = Extract()
    refined = Refined()
    load = Load()
    dataQuality = DataQuality()

    create.execute()
    extract.execute('yts_movies')
    refined.execute('yts_movies')

    for table in event['tables']:
        load.execute(table)

    dataQuality.execute(event['tables'])
    
    logging.info('Completing Process')
    
if __name__=='__main__':

    event = {
        "tables": [
            'DimCalendar',
            'DimTorrent',
            'DimGenres',
            'DimMovie',
            'FatFilm'
        ]
    }

    main(event)