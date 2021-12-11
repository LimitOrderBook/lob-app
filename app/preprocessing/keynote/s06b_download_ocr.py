import preprocessing.preglobal as pg
import os
    
def start_one(tbl, i):
    assert pg.google_status('https://videointelligence.googleapis.com/v1/'+i['ocr_operation'], pg.folder['ocr']+'/'+i['id']+'.json')
                     
    # Delete file from google bucket
    #pg.delete_file(i['id']+'.flac', pg.bucket['audio'])
    
    tbl.update_one({ '_id': i['_id'] },{ '$set': { 'ocr_downloaded':1} }, upsert=False)
                            
    return True
    
def start():
    return pg.start_step(start_one, {'ocr_generated':1, 'ocr_downloaded':None,'time_aligned':None})