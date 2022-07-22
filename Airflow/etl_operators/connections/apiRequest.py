# from functions.utilsFunctions import utils
import requests
import pandas as pd
from ..utils.utilsFunctions import utils

class apiRequest:

    def __init__(self) -> None:
        self.ut = utils()

    def getResponseData(self):
        
        movies_list = list()
        
        try:
            for page in range(0,10):
                
                response = requests.get(f'http://yts.torrentbay.to/api/v2/list_movies.json?limit=50&page={page}')
                response.raise_for_status()
            
                content = response.json()['data']['movies']
                movies_list.append(content)
            
            data = [d for m in movies_list for d in m]
            df = pd.DataFrame(data)
            df = self.ut.convertToJson(df,['genres','torrents'])
            
            return df

        except requests.exceptions.RequestException as e:
            raise TypeError(e)
