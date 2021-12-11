import preprocessing.preglobal as pg
import os
import requests
import lob.lob as loblib

def start_one(tbl, i):
    print('Start Import of',i['id'])
    # Check if LOB file already exists
    fn = pg.folder['lob']+'/'+i['id']+'.i40.z8'
    
    lob = loblib.LimitOrderBook()
    lob.connect(pg.get_db_client(), database=i['id'])
    lob.import_file(fn, typ='ITCH')
    
    lob.do_self_test(testtime=37800000, depth=20)
    st = { 'lob_imported':1}
    if len(lob.warnings) > 0:
        st['lob_warnings'] = lob.warnings
    tbl.update_one({ '_id': i['_id'] },{ '$set': st }, upsert=False)                        
    return True
    
def start():
    return pg.start_step(start_one, {'selected':1,'lob_downloaded':1, 'lob_imported':None})