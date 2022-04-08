from ETL import extract
from ETL import refined
import logging

def main():
    
    logging.info(f'Starting Process')
    
    extract.extractData()
    refined.dataRefinement()
    
    logging.info('Completing Process')
    
if __name__=='__main__':
    main()