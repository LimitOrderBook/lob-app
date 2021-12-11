import preprocessing.preglobal as pg
import os
import requests
    
def start_one(tbl, i):
    # Check if LOB file already exists
    fn = pg.folder['lob']+'/'+i['id']+'.i40.z8'
    if os.path.exists(fn):
        print('WARNING: LOB File already exists. Check if download was successful') 
    else:
        # Download LOB from trading physics
        query = 'getdata?type=orderflow&date='+i['eventdate'].strftime('%Y%m%d')+'&stock=AAPL&format=i40&compression=file'
        
        with open(pg.basefolder+'/tradingphyurl.txt') as f:
            turl = f.readline()
        getticket = turl+query
        
        res = requests.get(getticket, timeout=5)
        ticket = res.text
        if not ('{' in ticket and '}' in ticket):
            print('Error in receiving the LOB. ', ticket)
            return False
        
        print('Got ticket',ticket)

        url = 'http://api.tradingphysics.com/'+query+'&t='+ticket
        assert input('Please confirm download of LOB-data [yn] '+url) == 'y'

        pg.download(fn, url)
        
        
    tbl.update_one({ '_id': i['_id'] },{ '$set': { 'lob_downloaded':1} }, upsert=False)                        
    return True
    
def start():
    return pg.start_step(start_one, {'lob_downloaded':None})