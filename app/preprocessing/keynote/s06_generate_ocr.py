import preprocessing.preglobal as pg
from google.cloud import videointelligence_v1p3beta1 as videointelligence
import os

def start():
    return pg.start_step(start_one, {'downloaded':1,'ocr_generated':None,'time_aligned':None})


import_always = False        
def start_one(tbl,i):
    global import_always
    # Step 1: Upload video to google cloud  
    filename = pg.folder['video_hd']+'/'+i['id']+'.mp4'
    bucket_name = pg.bucket['ocr']
    
    pg.upload_file(filename, i['id']+'.mp4', bucket_name)
    
    
    # Step 2: Run Google OCR
    video_client = videointelligence.VideoIntelligenceServiceClient()
    features = [
               videointelligence.enums.Feature.TEXT_DETECTION,
             ]
    
    videofile = i['id']+'.mp4'
    assert import_always or input(['confirm ocr recognition of',videofile]) == ''
    import_always = True
    
    operation = video_client.annotate_video('gs://'+bucket_name+'/'+videofile, features=features) 
        
    print(videofile, operation.operation)
    
    tbl.update_one({ '_id': i['_id'] },{ '$set': { 'ocr_generated':1,'ocr_operation': operation.operation.name } }, upsert=False)
    
    return True
    
    