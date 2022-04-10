import requests
import pandas as pd


def getResponseData():
    
    movies_list = list()
    
    try:
        for page in range(1,11):
            
            response = requests.get(f'http://yts.torrentbay.to/api/v2/list_movies.json?limit=50&page={page}')
            response.raise_for_status()
        
            content = response.json()['data']['movies']
            movies_list.append(content )
        
        data = [d for m in movies_list for d in m]
        df = pd.DataFrame(data)

        return df

    except requests.exceptions.RequestException as e:
        raise TypeError(e)
