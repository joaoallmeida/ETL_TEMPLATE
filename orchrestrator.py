from ETL import create_database
from ETL import extract
from ETL import refined
from ETL import load_star_schema
import logging

# ## Inicial Config
log_conf = logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s -> %(message)s')

def main():
    
    logging.info(f'Starting Process')
    
    create_database.createDB()
    extract.ExtractData('yts_movies')
    refined.DataRefinement('yts_movies')
    load_star_schema.LoadStartSchema()
    
    logging.info('Completing Process')
    
if __name__=='__main__':
    main()