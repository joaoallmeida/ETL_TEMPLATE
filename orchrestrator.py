from ETL import create_database
from ETL import extract
from ETL import refined
from ETL import load_star_schema
import logging

def main():
    
    logging.info(f'Starting Process')
    
    create_database.createDB()
    extract.ExtractData()
    refined.DataRefinement()
    load_star_schema.LoadStartSchema()
    
    logging.info('Completing Process')
    
if __name__=='__main__':
    main()