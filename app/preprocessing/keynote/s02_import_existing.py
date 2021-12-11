import preprocessing.preglobal as pg
import os
import json

def start_one(tbl, i):
    if os.path.exists(pg.folder['subtitle']+'/'+i['id']+'.json'):
        tbl.update_one({ '_id': i['_id'] },{ '$set': {  'subtitle_generated':1, 'subtitle_downloaded':1} }, upsert=False)
    
    return True

def start_one2(tbl, i):
    # No need to extract audio, if subtitle is already there
    tbl2 = pg.get_time_tbl()
    fn = pg.folder['timealignment']+'/'+i['id']+'.json'
    if os.path.exists(fn):
        with open(fn, 'r') as f:
            res = json.load(f)
        for ii in res:
            tbl2.insert_one(ii)
        tbl.update_one({ '_id': i['_id'] },{ '$set': {  'time_aligned':1} }, upsert=False)
    
    return True


def start_one4(tbl, i):
    if os.path.exists(pg.folder['ocr']+'/'+i['id']+'.json'):
        tbl.update_one({ '_id': i['_id'] },{ '$set': {  'ocr_generated':1, 'ocr_downloaded':1} }, upsert=False)
    
    return True
    
def start():
    return pg.start_step(start_one, {'subtitle_generated':None}) and \
        pg.start_step(start_one2, {'time_aligned':None}) and \
                     pg.start_step(start_one4, {'ocr_generated':None})

