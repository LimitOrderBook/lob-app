from pymongo import MongoClient, UpdateMany, UpdateOne, InsertOne
import pandas as pd
import timeit
from datetime import datetime,timedelta
import queue
from bson.code import Code
import numpy as np
import time 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
import hashlib
import json
import pickle
import copy
import preprocessing.preglobal as pg

tic_t = 0
def tic():
    global tic_t
    tic_t = timeit.default_timer();
def toc():
    global tic_t
    toc_t = timeit.default_timer();
    print('Elapsed time: '+str(toc_t-tic_t)+'s')
    tic_t = toc_t
    

time_as_string = True

def generate_buy_sell_variants(graphlist):
    graphlist2 = copy.deepcopy(graphlist)
    for a,b in graphlist.items():
        if 'aggtype' in b and b['aggtype'] == 'sum' and not ('generate_variants' in b and not b['generate_variants']):
            for d in [1,-1]:
                vis = copy.deepcopy(b)
                if 'query' not in vis:
                    vis['query'] = []
                if len(vis['query']) > 0 and '$match' in vis['query'][0]:
                    vis['query'][0]['$match']['direction'] = d
                else:
                    assert False, 'not implemented'
                vis['name'] += ('Buy' if d==1 else 'Sell')+' market orders'
                vis['hidden'] = True
                graphlist2[a+'_'+('buy' if d==1 else 'sell')] = vis
            graphlist2[a+'_buysell'] = {
                'name':b['name']+' - Buy/Sell market orders',
                'combination': {'Sell MO':a+'_sell', 'Buy MO':a+'_buy' },
                'xaxis': b['xaxis'],
                'aggtype':'sum', 'hidden':True}
    return graphlist2


def path_exists(q, *path):
    for i in path:
        if i in q:
            q = q[i]
        else:
            return False
    return True  
def reset(dt):
    return dt + timedelta(hours = -dt.hour, minutes = -dt.minute, seconds = -dt.second, microseconds = - dt.microsecond)
def to_date(df):
    return [to_date_i(m) for m in df]

def to_millis(s):
    if type(s) == datetime:
        return (s-reset(s)).total_seconds()*1000
    if '.' not in s:
        s += '.0'
    return int(1000*(datetime.strptime("1970-01-01 "+s.split(' ')[1], "%Y-%m-%d %H:%M:%S.%f")).timestamp())

def to_date_i(m):
    dt = timedelta(milliseconds=int(m))  + reset(datetime.now())
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f") if time_as_string else dt
zoom_from = 100000
zoom_dmax = int(np.exp(int(np.log(24*3600*1000))))
globalquery = []
        
        
class cache:
    disable_redis_cache = False
    def __init__(self, ticks, name):
        self.r = pg.get_redis_client()
        self.ticks = ticks
        self.name = name
    
    def _query(self, key, ofun):
        if self.disable_redis_cache:
            return list(ofun())
            
        qhash = hashlib.md5((self.name+json.dumps(key)).encode()).hexdigest()
        res = self.r.get(qhash)
        if res is None:
            print('NOT IN CACHE - QUERING DB')
            res = list(ofun())
            self.r.set(qhash, pickle.dumps(res))
        else:
            res = pickle.loads(res)
        return res
    
    def zoom_cache(self,query, allowDiskUse=True):
        return self.ticks.aggregate(query, allowDiskUse=allowDiskUse)
    
    def is_cache(self):
        return True
        
    def aggregate(self, query, allowDiskUse=True):
        return self._query(query, lambda : self.zoom_cache(query, allowDiskUse=allowDiskUse))
    
    def map_reduce(self, mapper, reducer, tabl, query, finalize):
        class mr:
            def __init__(self, res):
                self.res = res
            def find(self):
                return self.res
        
        return mr(self._query({'m':mapper,'r':reducer,'q':query,'f':finalize}, 
                             lambda : self.ticks.map_reduce(mapper, reducer, tabl, query=query, finalize=finalize).find()))


############################################

class stock_analytics:
    def __init__(self,asc, gui_mode=True, use_cache=True):
        self.sc = asc
        
        if gui_mode:
            self.sc.register_callback('active_id', self._change_keynote, callnow=False)
        else:
            self._change_keynote(self.sc, use_cache=use_cache)
        
        self.graphlist = generate_buy_sell_variants(self.graphlist)
        self.cnter = 0
        self.time_mapping = None
        

        
    def _change_keynote(self,aid, use_cache=True):
        client = pg.get_db_client()
        db = client[aid]

        # Use Cache 
        self.ticks = cache(db.ticks, aid+'#ticks#') if use_cache else db.ticks    # Uncomment to not use cache 
        self.db = db
        
        # Init price filter
        cs = self.ticks.aggregate([{'$match':{'timestamp':{'$gt':3.6e7}}}, {'$sort':{'msgid':1}}, {'$limit':1}])
        df = list(cs)
        midp = df[0]['mid']
        self.filters['default']['range']['price'] = [midp-100000,midp+100000]


        # Load time keys (only relevant for gui, maybe put somewhere else)
        metadata = list(pg.get_keynote_tbl().find({'id':aid}))[0]
        if 'time_mapping' not in metadata or 'video_length' not in metadata:
            return # TODO: implement!
        else:
            self.time_mapping = metadata['time_mapping']
        
            
        
        
    def query_histogram(self, fieldX, fieldY, query = None, aggtype="avg", xrange=None, numbins=100, histogram=True):
        if query is None:
            query = []

        query.extend(globalquery.copy())

        
        if xrange is None:
            xrange = [None,None]
        
        filte = {}
        if xrange[0] is not None:
            filte["$gte"] = xrange[0]
        if xrange[1] is not None:
            filte["$lte"] = xrange[1]
            
                   
        if xrange[0] is None or xrange[1] is None:
            #Get smallest and biggest point
            query1 = query.copy()
            if len(filte) > 0:
                query1.append({'$match':{fieldX:filte}})
            query1.append({'$group':{"_id":1,"minx":{'$min':'$'+fieldX}, "maxx":{'$max':'$'+fieldX}}})
            
            cs = self.ticks.aggregate(query1)
            df =  list(cs)
            xrange[0] = df[0]['minx'] if xrange[0] is None else xrange[0]
            xrange[1] = df[0]['maxx'] if xrange[1] is None else xrange[1]
            
        # Get binsize for histograms
        histd = xrange[1]-xrange[0]

        zoom = histd > zoom_from
            
        
            
        
        d =  histd/numbins 
        
        if zoom:
            d = min(int(np.exp(int(np.log(d)))), zoom_dmax)
        
        project = {}
        group = {}
        
        if histogram:
            project['fieldX'] = {"$multiply":[d,{"$floor":{"$divide":["$"+fieldX,d]}}]}
            project['fieldY'] = "$"+fieldY if aggtype != 'time' else {"$multiply":["$"+fieldY, '$ttn']}
            if aggtype == 'time':
                project['ttn'] = 1

            
            group["_id"] = '$fieldX'
            group["fieldY"] = {"$"+('sum' if aggtype == 'time' else aggtype):"$fieldY"}
            if aggtype == 'time':
                group['ttn'] = {"$sum":"$ttn"}
        else:
            project['_id'] = '$'+fieldX
            project['fieldY'] = '$'+fieldY

        
        
        if len(filte) > 0 and not zoom:
            query.append({'$match':{fieldX:filte}})
        query.append({'$project':project})
        
        if histogram:
            query.append({'$group':group})
            if aggtype == 'time':
                query.append({'$project':{"fieldY":{'$cond':[{ '$eq': [ "$ttn", 0 ] }, None, {"$divide":["$fieldY", "$ttn"]}] }}})

        fieldX = '_id'
        fieldY = 'fieldY'

        query.append({"$sort": {fieldX:1}})

        cs = self.ticks.aggregate(query, allowDiskUse=True)
        df = list(cs)

        df =  pd.DataFrame(df)
        # Attention: In our convention, the xaxis shows the beginning of the bin. e.g. if we have a result of [[0,5],[2,3],[4,5]] that means, that in the range [0,2) there are 5, in the range [2,4) there are 3 and in [4,6) there are 5 items.
        if zoom:
            return df.where((df['_id']>= xrange[0]) & (df['_id']<= xrange[1])).dropna(), xrange
        return df, xrange


    def query_histogram_vola(self,fieldX, xrange=None, query=None, numbins=100):
        if query is None:
            query = []

        query.extend(globalquery.copy())
        
        if xrange is None:
            xrange = [None,None]
        
        filte = {}
        if xrange[0] is not None:
            filte["$gte"] = xrange[0]
        if xrange[1] is not None:
            filte["$lte"] = xrange[1]
            
                   
        if xrange[0] is None or xrange[1] is None:
            #Get smallest and biggest point
            query1 = query.copy()
            if len(filte) > 0:
                query1.append({'$match':{fieldX:filte}})
            query1.append({'$group':{"_id":1,"minx":{'$min':'$'+fieldX}, "maxx":{'$max':'$'+fieldX}}})
            
            cs = self.ticks.aggregate(query1)
            df =  list(cs)
            xrange[0] = df[0]['minx'] if xrange[0] is None else xrange[0]
            xrange[1] = df[0]['maxx'] if xrange[1] is None else xrange[1]
            
        # Get binsize for histograms
        histd = xrange[1]-xrange[0]
        zoom = histd > zoom_from
        d =  histd/numbins
        if zoom:
            d = min(int(np.exp(int(np.log(d)))), zoom_dmax)
        #Adjusted so that times are in the middle of their bin
        mapper = Code("""
                       function () {
                               emit("""+str(d)+"""*(Math.floor(this.timestamp/"""+str(d)+""")+.5), {ttp:this.ttp, logror:this.logror, S:0});
                       }
                       """)
                       
        reducer = Code("""
                        function (key, values) {
                          var wsum = 0;
                       
                          var mean = 0;
                          var meanold = 0;
                          var S = 0;
                          
                          for (var i = 0; i < values.length; i++) {
                            wsum += values[i].ttp;
       
                            meanold = mean;
                            mean += (values[i].ttp/wsum) * (values[i].logror - mean);
                            S += values[i].S + values[i].ttp*(values[i].logror-meanold)*(values[i].logror-mean);
                          }
                          return {ttp:wsum, logror:mean, S:S};
                        }
                        """)
        
        finalize = Code(""" function (key, reducedVal) {

                           return Math.sqrt(reducedVal.S / (reducedVal.ttp));

                        }
        """)
        query = {"ttp": {"$gt": 0}}
        if not zoom:
            query["timestamp"] = {
                                                                      "$gte": xrange[0],
                                                                      "$lte": xrange[1]
                                                                  }
        cs = self.ticks.map_reduce(mapper, reducer, "myresults", query=query,
                             finalize=finalize)

        df =  pd.DataFrame(list(cs.find()))
        if zoom:
            return df.where((df['_id']>= xrange[0]) & (df['_id']<= xrange[1])).dropna(), xrange
        return df, xrange

    def calc_lob_by_price_mongo(self, g,f, twod=False, enable_zoom=True):
        yrange = f['range']['price']

        if not twod and 'cursor' not in f:
            return np.array([]), np.array([])
            
        xrange = [to_millis(f['cursor']), to_millis(f['cursor'])] if not twod else f['range']['time']
        numbins = f['numbins']
        if twod and enable_zoom:
            numbins = min(100, numbins)
        fieldX = "timestamp"
        
        zoom = False
        
        d=0
        if twod:
            if xrange is None:
                xrange = [None,None]
            
            filte = {}
            if xrange[0] is not None:
                filte["$gte"] = xrange[0]
            if xrange[1] is not None:
                filte["$lte"] = xrange[1]
                
                       
            if xrange[0] is None or xrange[1] is None:
                #Get smallest and biggest point
                query1 = query.copy()
                if len(filte) > 0:
                    query1.append({'$match':{fieldX:filte}})
                query1.append({'$group':{"_id":1,"minx":{'$min':'$'+fieldX}, "maxx":{'$max':'$'+fieldX}}})
                
                cs = self.ticks.aggregate(query1)
                df =  list(cs)
                xrange[0] = df[0]['minx'] if xrange[0] is None else xrange[0]
                xrange[1] = df[0]['maxx'] if xrange[1] is None else xrange[1]
                
            # Get binsize for histograms
            histd = xrange[1]-xrange[0]
            zoom = histd > zoom_from
            d =  histd/numbins 
            if zoom and enable_zoom:
                d = min(int(np.exp(int(np.log(d)))), zoom_dmax)
                
        #TODO ENABLE ZOOM for better caching of 2d plot.
        zoom = False
        
        query = []
        query.extend(globalquery.copy())
        
        if not zoom:
            query.append(
             { "$match": { #"type": {"$nin":["trade","cross"]} not needed, because these types do not have the field last_action
                         "timestamp": {"$lte":xrange[1]}
                        , "last_action": {"$gt":xrange[0]},
                        "price": {"$gte":int(yrange[0]/100)*100, "$lt":int(yrange[1]/100)*100}
                      }})
        else:
            query.append(
             { "$match": { #"type": {"$nin":["trade","cross"]} not needed, because these types do not have the field last_action
                        "price": {"$gte":int(yrange[0]/100)*100, "$lt":int(yrange[1]/100)*100}
                      }})
                      
        group =  {
                   "_id": {"price":"$price","direction":"$direction"},
                   "volume_avail_sum": {"$sum": "$qty"}
                     # sum +1 if sell, -1 if buy, 0 else
                 }
        project = {"_id":0,"price":"$_id.price","direction":"$_id.direction","qty":"$volume_avail_sum"}
        sort = {"price":1}
        
        if twod:
            group["_id"]["timestamp"] = {"$multiply":[d,{"$ceil":{"$divide":["$"+fieldX,d]}}]}
            project["timestamp"] = "$_id.timestamp"
            sort = {"timestamp":1, "price":1}
         
        query.append({"$group":group})
        query.append({"$project":project})
        query.append({"$sort":sort})


        cs = self.ticks.aggregate(query)
        df = pd.DataFrame(list(cs))
        
        if not twod:
            return df['price'].values, df['qty'].values*df['direction'].values
            
            
        #convert to 2d image
        #print(df, query, g,f,twod)
        
        lobimage = []
        lasttimestamp = 0
        a = f['range']['price'][0]
        b = f['range']['price'][1]
        a = int(a/100)*100
        b = int(b/100)*100
        deltap = a
        x = list(range(a,b,100))
        y = []
        #print(x)
        steps = len(x)
        lob_row = np.zeros(steps)


        if len(df) == 0:
            return [],[],[],[]
            
        lob = df.groupby(by=['direction','price','timestamp']).sum().sort_values('timestamp').groupby(level=[0,1]).cumsum().reset_index().sort_values(['timestamp','qty'])

        
        pr = ((lob['price']-deltap)/100).astype(int).tolist()
        qtydir = (lob['qty']*lob['direction']).tolist()
        tim = lob['timestamp'].tolist()

        for i in range(0, len(pr)):
            ct = tim[i]
            if lasttimestamp != ct and lasttimestamp > xrange[0]:
                lobimage.append(lob_row.copy())
                y.append(lasttimestamp)
            try:
                lob_row[pr[i]] = qtydir[i]
            except:
                print(i, len(pr), len(qtydir), len(lob_row), pr[i])
                assert False
            lasttimestamp = ct
        lobimage.append(lob_row.copy())
        y.append(lasttimestamp)

        
        return lobimage,x,y, df
    
    def image_unit_test(self, drawlob=True):
        global time_as_string
        time_as_string_o = time_as_string
        time_as_string = True
        filte = copy.deepcopy(self.filters['default'])
        filte['range']['time'] = [12*3600*1000, 12*3600*1000+3.2*3600*1000]
        filte['range']['price'] = [1000100,2200000]
        filte['numbins'] = 100
        xrange = filte['range']['time']
        
        lobimage,x,y, df = self.calc_lob_by_price_mongo(self.graphlist['full_lob'], filte, twod=True)
        
        if drawlob:
            plt.imshow(lobimage, aspect='auto')
        
        for times,slices in [(y[0],0),(y[12],12),(y[48],48),(y[-10],-10),(y[-2],-2),(filte['range']['time'][1],-1)]:
            filte['cursor'] = to_date_i(times)
            print(times,slices,'ok?')
            lob = self.calc_lob_by_price_mongo(self.graphlist['full_lob'], filte)
            a = []
            b = []
            for i in range(0,len(x)):
                qty = lobimage[slices][i]
                if qty != 0:
                    a.append(x[i])
                    b.append(qty)
            
            if  len(b) != len(lob[1]):
                assert False, [len(b), len(lob[1])]
                return lobimage,df

            assert len(b) == len(lob[1]), (len(b), len(lob[1]), b, lob[1])
            assert (b==lob[1]).all()
            assert (a==lob[0]).all()
            print(times,slices,'ok')
        
        time_as_string = time_as_string_o
    
    def get_graphlist(self):
        return [a for a,b in self.graphlist.items() if not ('hidden' in b and b['hidden']) ]
        
    def calculate_graph(self,g,f, histogram=True, splitallowed=False):

        self.cnter+=1
        if 'type' in g and g['type'] == 'heatmap':
            img,x,y,_ = g['func'](self,g,f)
            return img, g['scalex'](x), g['scaley'](y)
            
        if 'func' in g:
            x,y = g['func'](self, g,f)
            return g['scalex'](x), g['scaley'](y)
            
        if 'combination' in g:
            res = []
            for a,b in g['combination'].items():
                x,y = self.calculate_graph(self.graphlist[b], f, histogram=histogram, splitallowed=False)
                res.append({'name':a, 'x':x, 'y':y})
            if splitallowed:
                return res
            
            assert g['aggtype'] in ['sum']
            if g['aggtype'] == 'sum':
                df = pd.DataFrame(res[0]['y'], res[0]['x'])

                for yy in range(1, len(res)):
                    df = pd.DataFrame(res[yy]['y'],res[yy]['x']).join(df, how='outer', rsuffix='a')
                
                df = df.sum(axis=1)
                return df.keys().tolist(), df.tolist()
        
                
        q = g['query'].copy() if 'query' in g else None
        fieldY=''
        if g['aggtype'] == 'vola':
            df, _ = self.query_histogram_vola(g['fieldX'], numbins=f['numbins'], xrange=f['range'][g['xaxis']])
            fieldY = 'value'
        else:
            df, _ = self.query_histogram(g['fieldX'], g['fieldY'], query=q, aggtype=g['aggtype'], numbins=f['numbins'], xrange=f['range'][g['xaxis']], histogram=histogram)
            fieldY = 'fieldY'
        return g['scalex'](df['_id'].values), g['scaley'](df[fieldY].values)

    filters = {
        'default':{
            'name':'Business hours / 1000 bins',
            'range':
            {
            'time':[3.5e7,5.5e7],
            'price':[1500000,1700000]
            },
            'numbins':1000,
            'cursor':to_date_i(37800000)
        }
    }

    graphlist = {
        'midprice':{
            'name':'Midprice Histogram (Time average)',
            'fieldX':'timestamp',
            'fieldY':'mid',
            'aggtype':'time',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x/10000,
            'xaxis':'time'
        },
        'bidprice':{
            'name':'Bidprice Histogram (Time average)',
            'fieldX':'timestamp',
            'fieldY':'sell',
            'aggtype':'time',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x/10000,
            'xaxis':'time'
        },
        'askprice':{
            'name':'Askprice Histogram (Time average)',
            'fieldX':'timestamp',
            'fieldY':'ask',
            'aggtype':'time',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x/10000,
            'xaxis':'time'
        },
        'marketorder_vis':{
            'name':'Executed market orders - Visible (Volume)',
            'query':[{'$match':{'type':{'$in':['fill','execute']}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: -x.astype(float),
            'xaxis':'time'
        },
        'limitorder':{
            'name':'Executed market orders - Visible (Volume)',
            'query':[{'$match':{'type':{'$in':['buy','sell']}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x.astype(float),
            'xaxis':'time'
        },
        'marketorder_num':{
            'name':'Executed market orders - Visible (Volume)',
            'query':[{'$match':{'type':{'$in':['fill','execute','trade']}}}, {'$project':{'timestamp':1, 'ttn':1, 'qty':{'$add':[1,0]}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x.astype(float),
            'xaxis':'time'
        },
        'limitorder_num':{
            'name':'Executed market orders - Visible (Volume)',
            'query':[{'$match':{'type':{'$in':['buy','sell']}}}, {'$project':{'timestamp':1, 'ttn':1, 'qty':{'$add':[1,0]}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x.astype(float),
            'xaxis':'time'
        },
        'marketorder_hid':{
            'name':'Executed market orders - Hidden (Volume)',
            'query':[{'$match':{'type':{'$in':['trade']}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x.astype(float),
            'xaxis':'time'
        },
        'marketorder_vis_num':{
            'name':'Executed market orders - Visible (Number)',
            'query':[{'$match':{'type':{'$in':['fill','execute']}}}, {'$project':{'timestamp':1, 'ttn':1, 'qty':{'$add':[1,0]}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x.astype(float),
            'xaxis':'time'
        },
        'marketorder_hid_num':{
            'name':'Executed market orders - Hidden (Number)',
            'query':[{'$match':{'type':{'$in':['trade']}}}, {'$project':{'timestamp':1, 'ttn':1, 'qty':{'$add':[1,0]}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x.astype(float),
            'xaxis':'time'
        },
        'delta_volume':{
            'name':'Change of Volume (New Limit Orders minus Cancellations) (Volume)',
            'query':[{'$match':{'type':{'$in':['cancel','delete','buy','sell']}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x.astype(float),
            'xaxis':'time'
        },
        'delta_volume_real':{
            'name':'Change of Volume (New Limit Orders minus Cancellations and Executions) (Volume)',
            'query':[{'$match':{'type':{'$in':['cancel','delete','buy','sell','fill','execute']}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x.astype(float),
            'xaxis':'time'
        },
        'vol_touch':{
            'name':'Volume at touch (Time average)',
            'query':[{'$project':{'timestamp':1, 'ttn':1,
                         'vol':{'$sum':[{'$arrayElemAt':[{'$arrayElemAt':[{'$arrayElemAt':['$lob',0]},0]},1]},
                         {'$arrayElemAt':[{'$arrayElemAt':[{'$arrayElemAt':['$lob',1]},0]},1]}],
                         }}}],
            'fieldX':'timestamp',
            'fieldY':'vol',
            'aggtype':'time',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x,
            'xaxis':'time'
        },
        'vol_touch_sell':{
            'name':'Volume at bid price (Time average)',
            'query':[{'$project':{'timestamp':1, 'ttn':1,
                         'bid_vol':{'$arrayElemAt':[{'$arrayElemAt':[{'$arrayElemAt':['$lob',0]},0]},1]}
                         }}],
            'fieldX':'timestamp',
            'fieldY':'bid_vol',
            'aggtype':'time',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x,
            'xaxis':'time',
            'hidden':True
        },
        'vol_touch_buy':{
            'name':'Volume at ask price (Time average)',
            'query':[{'$project':{'timestamp':1, 'ttn':1,
                         'ask_vol':{'$arrayElemAt':[{'$arrayElemAt':[{'$arrayElemAt':['$lob',1]},0]},1]}  
                         }}],
            'fieldX':'timestamp',
            'fieldY':'ask_vol',
            'aggtype':'time',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x,
            'xaxis':'time',
            'hidden':True
        },
        'vol_touch_buysell':{
            'name':'Volume at touch (Time average)',
            'combination':{'bid':'vol_touch_sell', 'ask':'vol_touch_buy' },
            'fieldX':'timestamp',
            'fieldY':'ask_vol',
            'aggtype':'sum',
            'xaxis':'time',
            'generate_variants':False,
            'hidden':True
        },
        'volatility':{
            'name':'Annual Volatility (Time weighted)',
            'fieldX':'timestamp',
            'aggtype':'vola',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: np.sqrt(252*8*3600*1e3)*x,
            'xaxis':'time'
        },
        'full_lob':{
            'name':'Full depth LOB @ cursor',
            'func': lambda self, g,f : self.calc_lob_by_price_mongo(g,f),
            'scalex': lambda x : x/10000,
            'scaley': lambda x : x.astype(float),
            'xaxis': 'price',
            'type':'bar'
        },
        'full_lob_2d':{
            'name':'Full depth LOB (heatmap)',
            'func': lambda self, g,f : self.calc_lob_by_price_mongo(g,f, twod=True),
            'scalex': lambda x : np.array(x)/10000,
            'scaley': lambda x : to_date(np.array(x)),
            'xaxis': 'time',
            'type':'heatmap'
        },
        'full_lob_2d_nozoom':{
            'name':'Full depth LOB (heatmap)',
            'func': lambda self, g,f : self.calc_lob_by_price_mongo(g,f, twod=True, enable_zoom=False),
            'scalex': lambda x : np.array(x)/10000,
            'scaley': lambda x : to_date(np.array(x)),
            'xaxis': 'time',
            'type':'heatmap'
        },
        'spread':{
            'name':'Spread (Time average)',
            'query':[{'$project':{'timestamp':1, 'ttn':1, 'spread':{'$subtract':['$ask','$bid']}}}],
            'fieldX':'timestamp',
            'fieldY':'spread',
            'aggtype':'time',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x/10000,
            'xaxis':'time'
        },
        'messages_num':{
            'name':'Number of messages',
            'query':[{'$project':{'timestamp':1, 'ttn':1, 'qty':{'$add':[1,0]}}}],
            'fieldX':'timestamp',
            'fieldY':'qty',
            'aggtype':'sum',
            'scalex': lambda x : to_date(x),
            'scaley': lambda x: x.astype(float),
            'xaxis':'time',
             'generate_variants':False,
        },
    }

                
         
        

