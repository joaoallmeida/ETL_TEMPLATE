import pandas as pd

def addNewColumnToDF(df):
    
    torrent_list = list()
    
    for a,b in df.iterrows():
        for t in b.torrents:
            t['id'] = b.id
            t['url_torrent'] =  f"magnet:?xt=urn:btih:{t['hash']}&dn={b.title}-{t['quality']}-{t['type']}&tr=http://track.one:1234/announce&tr=udp://open.demonii.com:1337/announce&tr=udp://tracker.openbittorrent.com:80&tr=udp://tracker.coppersurfer.tk:6969&tr=udp://glotorrents.pw:6969/announce&tr=udp://tracker.opentrackr.org:1337/announce&tr=udp://torrent.gresille.org:80/announce&tr=udp://p4p.arenabg.com:1337&tr=udp://tracker.leechers-paradise.org:6969"
            torrent_list.append(t)

    df_aux = pd.DataFrame(torrent_list)
    df_merge = df.merge(df_aux, on='id',how='inner')
    df_merge = df_merge.drop(['torrents'],axis=1)
        
    return df_merge

def pivotGenreColumn(df):

    df[['genre_01','genre_02','genre_03','genre_04']] = df.apply(lambda x: pd.Series(x['genres']) ,axis=1)   
    df = df.drop(['genres'],axis=1)

    return df 