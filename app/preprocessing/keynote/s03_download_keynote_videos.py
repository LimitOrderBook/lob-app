import preprocessing.preglobal as pg
import os
    
def start_one(tbl, i):
    filename = pg.folder['video_sd']+'/'+i['id']+'.mp4'
    url = i['url']
    pg.download(filename, url)
    
    # No need to download hd, if subtitles are already generated
    if not 'subtitle_downloaded' in i or not 'time_aligned' in i:
        filename = pg.folder['video_hd']+'/'+i['id']+'.mp4'
        url = i['url_hd']
        pg.download(filename, url)
    
    tbl.update_one({ '_id': i['_id'] },{ '$set': { 'downloaded': 1 } }, upsert=False)
    return True
    
def start():
    return pg.start_step(start_one, {'downloaded':None})