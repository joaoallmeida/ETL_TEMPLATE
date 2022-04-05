from ETL import Extract,Refined
import logging

def main():
    
    logging.info(f'Starting Process')
    
    Extract.extractData()
    # Refined.dataRefinement()
    
    logging.info('Completing Process')
        
if __name__=='__main__':
    main()