import requests
import pandas as pd
import datetime
import pytz

def getResponseData():
    
    dt_now = datetime.datetime.now(pytz.timezone('UTC'))
    movies_list = list()
    
    try:
        for page in range(1,11):
            
            response = requests.get(f'http://yts.torrentbay.to/api/v2/list_movies.json?page={page}')
            response.raise_for_status()
        
            content = response.json()['data']['movies']
            movies_list.append(content )
        
        data = [d for m in movies_list for d in m]
        df = pd.DataFrame(data)
        df['extract_at'] = pd.to_datetime(dt_now)
                
        lines = len(df.index)

        return df, lines

    except requests.exceptions.RequestException as e:
        raise TypeError(e)
