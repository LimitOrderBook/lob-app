import preprocessing.preglobal as pg
import os
    
def start_one(tbl, i):
    if not pg.google_status('https://speech.googleapis.com/v1p1beta1/operations/'+i['subtitle_operation'], pg.folder['subtitle']+'/'+i['id']+'.json', checkuntildone=False):
        print('skipping ',i['id'])
        return False
                     
    # Delete audio file from google bucket
    pg.delete_file(i['id']+'.flac', pg.bucket['audio'])
    
    tbl.update_one({ '_id': i['_id'] },{ '$set': { 'subtitle_downloaded':1} }, upsert=False)
                            
    return True
    
def start():
    return pg.start_step(start_one, {'subtitle_generated':1, 'subtitle_downloaded':None})