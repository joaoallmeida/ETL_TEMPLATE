# from functions.utilsFunctions import utils
import requests
import pandas as pd
from utilsFunctions import utils


class sourceApi:

    def getResponseData():
        
        movies_list = list()
        ut = utils()
        
        try:
            for page in range(0,10):
                
                response = requests.get(f'http://yts.torrentbay.to/api/v2/list_movies.json?limit=50&page={page}')
                response.raise_for_status()
            
                content = response.json()['data']['movies']
                movies_list.append(content)
            
            data = [d for m in movies_list for d in m]
            df = pd.DataFrame(data)
            df = ut.convertToJson(df,['genres','torrents'])
            
            return df

        except requests.exceptions.RequestException as e:
            raise TypeError(e)
