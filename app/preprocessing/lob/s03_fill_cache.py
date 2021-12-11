import frontend.stock_analytics as salib
import preprocessing.preglobal as pg
import numpy as np
import copy
import time


def start_one(tbl, i):
    print('Start filling cache of',i['id'])
    
    fillcache(i['id'])
        
    tbl.update_one({ '_id': i['_id'] },{ '$set': { 'lob_cache_filled':1} }, upsert=False)                        
    return True
    
def start():
    return pg.start_step(start_one, {'lob_imported':1, 'lob_cache_filled':None})

def start_again():
    return pg.start_step(start_one, {'lob_imported':1})

class Timer(object):
    def __init__(self, name=None):
        self.name = name
        if self.name:
            print('[%s] Starting' % self.name,)
    def __enter__(self):
        self.tstart = time.time()

    def __exit__(self, type, value, traceback):
        if self.name:    
            print('['+self.name+'] Elapsed: %s' % (time.time() - self.tstart))
        else:
            print('Elapsed: %s' % (time.time() - self.tstart))
            
def fillcache(idn):
    sa = salib.stock_analytics(idn, gui_mode=False)
    
    print('2D LOB Image Unit Test')
    sa.image_unit_test(drawlob=False)
    print('2D LOB Image Unit Test completed')
    
    d_array = []
    i = 0
    while True:
        zoom_from = int(100005*(np.e**i))
        zoom_dmax = int(np.exp(int(np.log(24*3600*1000))))
        d = zoom_from
        e = int(np.exp(int(np.log(d))))
        if e > zoom_dmax:
            break
        d_array.append(zoom_from)
        i+=1

    filte = copy.deepcopy(sa.filters['default'])
    filte['range']['time'][0] = 0

    for numb in [10,100,1000]:
        filte['numbins'] = numb
        for name, i in sa.graphlist.items():
            if i['xaxis'] == 'time':
                with Timer(name):
                    for d in d_array:
                        with Timer('--'+str(d)):
                            filte['range']['time'][1] = d
                            sa.calculate_graph(i, filte, histogram=True)