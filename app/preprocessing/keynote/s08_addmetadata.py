import preprocessing.preglobal as pg
import cv2
    
def start_one(tbl, i):
    video_hd_folder = pg.folder['video_hd']
    cap = cv2.VideoCapture(video_hd_folder+'/'+i['id']+'.mp4')
    fps = cap.get(cv2.CAP_PROP_FPS)
    frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    
    video_length = frame_count/fps
    
    
    tt = pg.get_time_tbl()
        
    sts1 = list(tt.aggregate([{'$match':{'id':i['id']}},{'$group':{'_id':'$id','sts':{'$avg':{'$arrayElemAt':['$timekey',1]}}}}]))
    if len(sts1)!=1:
        print('WARNING: Time alignment not found, assuming video is started on the hour')
        sts = 13*3600*1000
    else:
        sts = sts1[0]['sts']
    
    # Time difference to cupertino time
    timezones = {
        'ct': -2,
        'pt':0,
        'et':-3
        }

    time_mapping = [{ # stock time is in milliseconds, video time is in seconds
            'stock_time_start': sts+timezones[i['time-zone']]*3600*1000, 
            'stock_time_end': None, 
            'video_time_start':0
        }]
    
    for t in time_mapping:
        if not t['stock_time_end']:
            t['stock_time_end'] = t['stock_time_start']+video_length*1000-t['video_time_start']
        t['video_time_end'] = t['video_time_start']+(t['stock_time_end']-t['stock_time_start'])/1000
         
         
    
    
    tbl.update_one({ '_id': i['_id'] },{ '$set': { 'misc_and_meta':1, 'time_mapping':time_mapping, 'video_length':video_length} }, upsert=False)
                            
    return True
    
def start():
    return pg.start_step(start_one, {'time_aligned':1}) 