from pymongo import MongoClient, UpdateMany, UpdateOne, InsertOne, DeleteMany
from operator import neg
from pymongo.errors import BulkWriteError
from datetime import datetime,timedelta
import timeit
import os
import zlib
import struct
import numpy as np
from pprint import pprint
from sortedcontainers import SortedDict
import pandas as pd
import xml.etree.ElementTree

not_strict = False # if True some sanity checks do to not throw errors

# Measure time
tic_t = 0
def tic():
    global tic_t
    tic_t = timeit.default_timer();
def toc():
    global tic_t
    toc_t = timeit.default_timer();
    print('Elapsed time: '+str(toc_t-tic_t)+'s')
    tic_t = toc_t
    
    
def read(r,p,t):
    lengths = {
        '>H':2,
        'c':1,
        '>I':4,
        '>Q':8,
        'cccccc':6,
        'cccc':4
    }
    maps = {
        'c':(lambda x: x[0].decode()),
        'cccccc':(lambda x: ''.join([i.decode() for i in x]).rstrip()),
        'cccc':(lambda x: ''.join([i.decode() for i in x]).rstrip())
    }
    l = lengths[t]
    re = struct.unpack(t, r[p:(p+l)])
    if t in maps:
        re = maps[t](re)
    else:
        re = re[0]
    return re, p+l

class LimitOrderBook:
    db = None
    ticks = None
    dbname = None
    ticksname = None
    pricebase = 10000
    basicfilter = {'direction':1, 'price':1, 'timestamp':1, 'qty':1, 'type':1, 'id':1, 'msgid':1}
    def connect(self,client, database='itch', table='ticks'):
        self.db = client[database]
        self.ticks = self.db[table]
        self.ticksname = table
        self.dbname = database
        self.warnings = []
        
    def loop(self,silent=False, batch=None, i=None, final=False, obj=None):
        if not silent and i is not None and i%100000==0:
            print(i)
        if (i is not None and i%100000==0) or final:
            if obj is None:
                obj = self.ticks
               
            if batch is not None and len(batch)>0:
                print('start bulk update')
                try:
                    obj.bulk_write(batch, ordered=False)
                except BulkWriteError as bwe:
                    pprint(bwe.details)
                print('finished bulk update')
                return True
        return False
          
        
    messages = {
        'T':{
            'name':'timestamp seconds',
            'len':5
        },
        'S':{
            'name':'system event',
            'len':6
        },
        'R':{
            'name':'stock directory',
            'len':18
        },
        'H':{
            'name':'stock trading action',
            'len':17
        },
        'L':{
            'name':'market participant position',
            'len':18
        },
        'A':{
            'name':'add order',
            'len':28,
            'mapmsg':[
                ('>Q',['id']),
                ('c',[('type',lambda x: 'sell' if x=='S' else 'buy')]),
                ('>I',['qty']),
                ('cccccc',['ticker']),
                ('>I',['price'])
            ]
        },
        'F':{
            'name':'add order MPID',
            'len':32,
            'mapmsg':[
                ('>Q',['id']),
                ('c',[('type',lambda x: 'sell' if x=='S' else 'buy')]),
                ('>I',['qty']),
                ('cccccc',['ticker']),
                ('>I',['price']),
                ('cccc',['mpid'])
            ]
        },
        'E':{
            'name':'order executed',
            'len':25,
            'mapmsg':[
                ('>Q',['id',('type',lambda x:'execute')]),
                ('>I',['qty']),
                ('>Q',[]) #matchid is always 0
            ]
        },
        'C':{
            'name':'order executed with price',
            'len':30
        },
        'X':{
            'name':'order cancelled',
            'len':17,
            'mapmsg':[
                ('>Q',['id',('type',lambda x:'cancel')]),
                ('>I',['qty'])
            ]
        },
        'D':{
            'name':'order deleted',
            'len':13,
            'mapmsg':[
                ('>Q',['id',('type',lambda x:'delete')])
            ]
        },
        'U':{
            'name':'order replaced',
            'len':29
        },
        'P':{
            'name':'trade',
            'len':36,
            'mapmsg':[
                ('>Q',[('type',lambda x:'trade')]),
                ('c',[]),
                ('>I',['qty']),
                ('cccccc',['ticker']),
                ('>I',['price']),
                ('>Q',[])
            ]
        },
        'Q':{
            'name':'cross trade',
            'len':32,
            'mapmsg':[
                ('>Q',['qty',('type',lambda x:'cross')]),
                ('cccccc',['ticker']),
                ('>I',['price']),
                ('>Q',['id']),
                ('c',['crosstype'])
            ]
        },
        'B':{
            'name':'broken trade',
            'len':13
        },
        'I':{
            'name':'net order imbalance indicator',
            'len':42
        }
    }



    def import_itch(self, filename, compressed=True): # Nasdaq TradeView ITCH format
        with open(filename, 'rb') as f:
            if compressed:
                size = f.read(8)
                size = struct.unpack('q',size)[0]
            data = f.read(os.path.getsize(filename))
            assert not f.read(1)
          
        res = None
        if compressed:
            res = zlib.decompress(data)
            assert len(res) == size
        else:
            res = data
            
        p = 0 # file position
        time_seconds = -1
        
        batch = []
        ii = 0
        prevts = -1
        # statistics
        for a,b in self.messages.items():
            b['count'] = 0

        basedate = datetime.strptime(filename.split('/')[-1].split('_')[0], 
                                             '%Y%m%d') #Get Date from Filename

        while p < len(res):
            # Read message length
            l,p = read(res, p, '>H')
            typ,p = read(res, p, 'c')
            assert self.messages[typ]['len'] == l

            # Statistics
            self.messages[typ]['count']+=1

            # Process message | T,A,F,E,X,D,P,Q
            if typ=='T':
                tsnew,p = read(res, p, '>I')
                assert tsnew > time_seconds
                time_seconds = tsnew
            elif typ in ['A','F','E','X','D','P','Q']:
                ns,p = read(res, p, '>I')
                ts = time_seconds*1000+ns/1e6
                msg = {'timestamp':ts,
                      'timestamp_date':basedate+timedelta(milliseconds=ts)
                      }
                assert (ns-int(ns/1000)*1000) == 0 # Maximum precicion is microseconds...
                assert (ns-int(ns/1e6)*1e6) == 0 # Maximum precicion is milliseconds...
                #assert (ns-int(ns/1e7)*1e7) == 0 # returns False, so maximum precicion is milliseconds

                if 'mapmsg' in self.messages[typ]:
                    for i in self.messages[typ]['mapmsg']:
                        data,p = read(res, p, i[0])
                        for j in i[1]:
                            dat = data
                            fld = j
                            if type(j) is tuple:
                                dat = j[1](dat)
                                fld = j[0]
                            msg[fld] = dat
                     
                    msg['msgid'] = ii
                    assert prevts <= msg['timestamp']
                    prevts =  msg['timestamp']
                    
                    ii+=1
                    batch.append(InsertOne(msg))
                    if self.loop(batch=batch,i=ii):
                        batch = []
                else:
                    assert False
                    p+=l-5

            else:    # Skip message
                assert False
                p+=l-1
        
        self.loop(batch=batch, final=True)
        assert p == len(res)    
        
        
    def import_xml(self,filename): # XML Format from Trading Physics
        e = xml.etree.ElementTree.parse(filename).getroot()
        batch = []
        for session in e.iter('session'):
            ses = session.attrib.copy()

            basedate = datetime.strptime(ses['date'], '%Y%m%d')
            del ses['date'], ses['type']
            i=0
            for message in session.iter('message'):
                i+=1
                msg = message.attrib.copy()
                msg.update(ses)
                for order in message.iter('order'):
                    msg['id'] = int(order.text)
                for order in message.iter('quantity'):
                    msg['qty'] = int(order.text)
                for order in message.iter('price'):
                    msg['price'] = int(order.text)#/10000
                for order in message.iter('mpid'):
                    msg['mpid'] = order.text   
                msg['timestamp'] = int(msg['timestamp'])
                msg['timestamp_date'] = basedate+timedelta(milliseconds=int(msg['timestamp']))

                batch.append(InsertOne(msg))
                if self.loop(batch=batch,i=i):
                    batch = []
                    
        self.loop(batch=batch, final=True)
            
    def import_csv(self,filename): # CSV Format from LOBSTER
        df = pd.read_csv(filename, header=None, names=
                        ['time','type','orderid','size','price','direction','mpid'])
        
        batch = []
        i=0
        basedate = datetime.strptime(filename.split('/')[-1].split('_')[1], 
                                     '%Y-%m-%d') #Get Date from Filename
        
        ticker = filename.split('/')[-1].split('_')[0]
        
        msg_types = {
            1:'LO',
            2:'cancel',
            3:'delete',
            4:'execute',
            5:'trade',
            6:'cross',
            7:'stop'
        } # Fehlt: FILL
        mpna = df['mpid'].isna()
        
        for key, row in df.iterrows():
            i+=1
            typ = msg_types[row['type']]
            if typ == 'LO':
                if row['direction'] == 1:
                    typ='buy'
                elif row['direction']==-1:
                    typ='sell'
                else:
                    assert False
                    
            msg = {
                'ticker': ticker,
                'timestamp': row['time']*1000.,
                'timestamp_date': basedate+timedelta(milliseconds=row['time']*1000.),
                'direction': -row['direction'], # Opposite than original data
                'price': row['price'], 
                'qty':row['size'],
                'type':typ 
            }
            if row['orderid'] > 0:
                msg['id'] = row['orderid']
                
            if not mpna[key]:
                msg['mpid'] = row['mpid']
             
            batch.append(InsertOne(msg))
            if self.loop(batch=batch,i=i):
                batch = []
                    
        self.loop(batch=batch, final=True)
        
    def update_lobster_fill(self, throwerror=False):
        # Reconstruct the limit order book from top to bottom
        orderbook = {}
        query = {"type":{"$nin":["trade","cross"]}}
        fillc=0
        lu_requests = []
             
        cs = self.ticks.find(query, self.basicfilter).sort("msgid",1)
        i = 0
    
        
        
        for en in cs:
            i += 1
            typ = en['type']
            if typ in ["sell","buy"]:
                d = 1 if typ=="sell" else -1
                orderbook[en['id']] = (en['qty'],en['price'],d)
            elif typ in ["fill","delete"]: 
                if en['id'] not in orderbook:
                    if throwerror:
                        return False
                    print("WARNING: this warning should not appear for ITCH files")
                    lu_requests.append(DeleteMany({"_id": en['_id']}))

                    a = pd.DataFrame.from_dict(orderbook, orient='index', columns=['qty','price','d'])
 
                    bid = a[a['d']==-1]['price'].max()
                    ask = a[a['d']==1]['price'].min()
                    mid = 0.5*(bid+ask)
                    
                    dist = abs(en['price']-mid)/100
                    
                    if dist <= 70:
                        print('bid,ask',bid,ask,mid)
                        print('error, cant delete non existant order with id',en['id'],en['price'], dist)
                else:
                    del orderbook[en['id']] 
            elif typ in ["execute", "cancel"]:
                if en['id'] not in orderbook:
                    if throwerror:
                        return False
                    print("WARNING: this warning should not appear for ITCH files")
                    if typ=='execute':
                        print('execute long lasting lob => change to hidden order')
                        lu_requests.append(UpdateMany({"_id": en['_id']},{"$set":{"type": 'trade'},"$unset":{'id':''}}))  
                    else:
                        lu_requests.append(DeleteMany({"_id": en['_id']}))


                        a = pd.DataFrame.from_dict(orderbook, orient='index', columns=['qty','price','d'])

                        bid = a[a['d']==-1]['price'].max()
                        ask = a[a['d']==1]['price'].min()
                        mid = 0.5*(bid+ask)

                        dist = abs(en['price']-mid)/100

                        assert dist > 70

                
                else:
                    (a,b,c) = orderbook[en['id']]
                    orderbook[en['id']] = (a-en['qty'],b,c)
                    assert en['qty'] > 0, en
                    assert a-en['qty'] >= 0
                    if a-en['qty'] == 0:
                        assert typ=='execute' #we have executions that should be fills
                        fillc+=1
                        lu_requests.append(UpdateMany({"_id": en['_id']},{"$set":{"type": 'fill'}}))       
            elif typ not in ['stop']:
                print("error")
            
            if self.loop(batch=lu_requests,i=i):
                lu_requests = []

           

        self.loop(batch=lu_requests,final=True)
        print('executions converted into fills: ',fillc)

        return True

        
    def create_index(self):
        ns = self.dbname+'.'+self.ticksname
        self.db.command(
        {
            "createIndexes" : self.ticksname,
            "indexes" : [
                {
                    "v" : 2,
                    "key" : {
                        "timestamp" : 1
                    },
                    "name" : "timestamp_1",
                    "ns" : ns
                },
                {
                    "v" : 2,
                    "key" : {
                        "msgid" : 1
                    },
                    "name" : "msgid_1",
                    "ns" : ns
                },
                {
                    "v" : 2,
                    "key" : {
                        "type" : 1
                    },
                    "name" : "type_1",
                    "ns" : ns
                },
                {
                    "v" : 2,
                    "key" : {
                        "id" : 1
                    },
                    "name" : "id_1",
                    "ns" : ns
                },
                {
                    "v" : 2,
                    "key" : {
                        "type" : 1,
                        "timestamp" : 1
                    },
                    "name" : "timestamp_1_type_1",
                    "ns" : ns
                },
                {
                    "v" : 2,
                    "key" : {
                        "type" : 1,
                        "msgid" : 1
                    },
                    "name" : "msgid_1_type_1",
                    "ns" : ns
                },
                {
                    "v" : 2,
                    "key" : {
                        "last_action" : 1,
                        "timestamp" : 1,
                        "direction" : 1,
                        "price" : 1
                    },
                    "name" : "last_action_1_timestamp_1",
                    "ns" : ns
                }
            ]
        })
                
    def test_pre_sanity(self):
        # Sanity checks: All order ids have the same price
        self.assert_q([
            {"$match": {"id":{"$ne":None}}},
           { "$group": { "_id": "$id",
                       "maxp": { "$max": "$price" }, 
                       "minp": { "$min": "$price" }

            }},
           { "$match": { "$expr": { "$ne": ["$minp","$maxp"]} }  
           },

        ], allowDiskUse=True, t='a')
        
    def import_file(self,filename='20170912_AAPL.xml', depth=20, typ='TRPHY'):
        # Check if files are already imported
        if self.ticks.count_documents ({}) != 0:
            print('Import aborted. Collection already exists')
            return
        tic()

        if typ=='TRPHY':
            print ("==== Import XML to Database ====")
            self.import_xml(filename)
        elif typ=='LOBSTER':
            print ("==== Import LOBSTER CSV to Database ====")
            self.import_csv(filename)
        elif typ=='ITCH':
            print ("==== Import Nasdaq TotalView ITCH to Database ====")
            self.import_itch(filename)
        else:
            print('Unknown file type',typ,'Aborting')
            return
        toc()
        
        print ("==== Create indices ====")
        self.create_index()
        toc()        

        if typ in ['LOBSTER']: # 2021 only relevant for lobster? ,'ITCH']:
            print ("==== Update fill ====")
            self.update_lobster_fill()

        print ("==== Check LOB consistency ====")
        assert self.update_lobster_fill(throwerror=True)
        toc()

        print ("==== Sanity check (pre) ====")
        self.test_pre_sanity()
        toc()

        print ("==== Set directed quantity - do only run once!! ====")
        self.ticks.update_many({"type":{"$in":['execute','cancel']}},{"$mul":{"qty":-1}})
        toc()

        print("==== Denormalize entries (calc direction, price, qty, last_action) ====")
        self.calc_lob_by_order_native(time=None, write_lu=True)
        toc()

        print("==== Calculate LOB with depth "+str(depth)+" and save it to DB ====")
        self.calc_lob_by_price_native(depth=depth,write_lu=True)
        toc()
        
        print("==== FINISHED IMPORT ====")
        

        
    def test_compare_df(self,df1, df2, mismatch=0):
        df = pd.concat([df1, df2], sort=False)
        df = df.reset_index(drop=True)

        df_gpby = df.groupby(list(df.columns))

        idx = [x[0] for x in df_gpby.groups.values() if len(x) != 2]

        assert len(idx) == mismatch, ["Error: DataFrames are NOT the same. Differences:", df.reindex(idx)]

        print("Passed test. Dataframes are identical.")

    def test_lob_engine(self, testtime=37800000, depth=20):
        print("=== Generate LOB by order (aggregated by price, native) ===")
        orderbook_v01 = self.calc_lob_by_order_native(time=testtime)
        by_price_v01 = self.aggregate_by_order_by_price(orderbook_v01)
        toc()
        
        print("=== Generate LOB by price (native) ===")
        orderbook2 = self.calc_lob_by_price_native(time=testtime, depth=None)
        by_price_v02 = self.convert_sorted_to_dataframe(orderbook2)
        toc()
        
        print("=== Generate LOB by price (mongo) ===")
        by_price_v03 = self.calc_lob_by_price_mongo(time=testtime)
        toc()
        
        print("=== Generate LOB by order (mongo) ===")
        orderbook_v02 = self.calc_lob_by_order_mongo(time=testtime)
        toc()
        
        print("=== Read LOB by price with depth "+str(depth)+" (mongo) ===")
        by_price_v04 = self.convert_sorted_to_dataframe(self.read_lob_by_price_mongo(time=testtime))
        toc()
        
        self.test_compare_df(by_price_v01, by_price_v02)
        self.test_compare_df(by_price_v01, by_price_v03)
        self.test_compare_df(by_price_v01, by_price_v04, mismatch=len(by_price_v01)-2*depth)
        self.test_compare_df(orderbook_v01, orderbook_v02)
        
    def assert_q(self,q,t='f',allowDiskUse=False):
        if t == 'f':
            assert len(list(self.ticks.find(q))) == 0,[q,t,allowDiskUse]
        elif t== 'a':
            assert len(list(self.ticks.aggregate(q,allowDiskUse=allowDiskUse))) == 0,[q,t,allowDiskUse]
        else:
            assert 1==0
            
    def test_data_verifcation(self):
        # Verify that every item has now a entry price, direction, last_action
        tic()
        self.assert_q({"price":None,"type":{"$nin":['trade','cross']}})
        self.assert_q({"direction":None,"type":{"$nin":['trade','cross']}})
        self.assert_q({"last_action":None,"type":{"$nin":['trade','cross']}})
        toc()

        # Verify that the total quantity of each order is zero
        self.assert_q([{"$match":{"type":{"$nin":["cross","trade"]}}},
                       {"$group": { "_id": "$id", "sumqty":{"$sum": "$qty"}}}, 
                       {"$match":{"sumqty":{"$ne":0}}}], allowDiskUse=True, t='a')
        toc()

        # Next occurences of one id can never be sell or buy (cant increase order volume)
        # E.g. sell or buy can only be present once per id
        self.assert_q(
           [
             
             { "$sort": { "id": 1, "msgid": 1 } },
             {
               "$group":
                 {
                   "_id": {"id":"$id","type":"$type"},
                   "count": { "$sum": 1}
                 }
             }
               ,
               {"$match": {"count": {"$gt":1}}},
                {"$match": {"_id.type":{"$nin": ["trade","execute","cancel"]}}}



           ], allowDiskUse=True, t='a')
    
        toc()
        
        # first occurence can only be buy and sell per order id
        self.assert_q(
           [
             { "$sort": { "id": 1, "msgid": 1 } },
             {
               "$group":
                 {
                   "_id": "$id",
                   "firstType": { "$first": "$type" }
                 }
             }
               ,
               {"$match": {"count": {"$gt":1}}}
               ,{"$match": {"_id.firstType":{"$nin": ["trade","cross","buy","sell"]}}}




           ], allowDiskUse=True, t='a')
        
        toc()
        
        # Trade does not have orderid
        self.assert_q({"type":"trade","id":{"$ne":None}})
        toc()
        
        # Everything else has orderid
        self.assert_q({"type":{"$ne":"trade"},"id":None})
        toc()
        
        try:
            # There are exactly two crosses
            self.assert_q([{"$match":{"type":"cross"}},
                           {"$group": {"_id": "$type", "count":{"$sum": 1}}},
                           {"$match":{"count":{"$ne":2}}}], allowDiskUse=True, t='a')
        except:
            print('WARNING: Number of crosses is not two!')
            self.warnings.append({'warning':'number of crosses is not two', 'detail':list(self.ticks.aggregate([
               {"$match": {"type": "cross"}},
               ]
                           ,allowDiskUse=True))})
        toc()
        
        
       
        
    def do_self_test(self, testtime=37800000, depth=20):
        tic()
        print ("==== Sanity check (pre after denormalisation) ====")
        self.test_pre_sanity()
        toc()
        
        print("===== Execute self-test =====")
        print("==== Test LOB functions ====")
        self.test_lob_engine(testtime=testtime, depth=depth)
        toc()
        
        print("==== Verify data integrety ====")
        self.test_data_verifcation()
        toc()
        
        print("===== Finished self-test =====")
        
        
    def aggregate_by_order_by_price(self, orderbook):
        return orderbook.set_index("orderid").groupby(["price","direction"]).sum().reset_index().rename(columns={1:'price',2:'direction',0:'qty'})
    
    def calc_lob_by_order_native(self, time, write_lu=False):
        # Reconstruct the limit order book from top to bottom
        orderbook = {}
        query = {"type":{"$nin":["trade","cross"]}}
        if time is not None:
            query["timestamp"] = {"$lt":time}
        lu_requests = None
        if write_lu:
            lu_requests = []         
        cs = self.ticks.find(query, self.basicfilter).sort("msgid",1)
        i = 0
        
        for en in cs:
            i += 1
            typ = en['type']
            if typ in ["sell","buy"]:
                d = 1 if typ=="sell" else -1
                assert en['qty'] > 0
                orderbook[en['id']] = (en['qty'],en['price'],d)
                if write_lu:
                    lu_requests.append(UpdateMany({"id": en['id']},{"$set":{"direction": d, "price": en['price']}}))
            elif typ in ["fill","delete"]: 
                if write_lu:
                    lu_requests.append(UpdateMany({"id": en['id']},{"$set":{"last_action": en['timestamp']}}))  
                    lu_requests.append(UpdateMany({"_id": en['_id']},{"$set":{"qty": -orderbook[en['id']][0]}}))
                del orderbook[en['id']] 
            elif typ in ["execute", "cancel"]:
                (a,b,c) = orderbook[en['id']]
                orderbook[en['id']] = (a+en['qty'],b,c)
                assert en['qty'] < 0, en
                assert a+en['qty'] > 0
            elif typ not in ['stop']:
                print("error")
            
            if self.loop(batch=lu_requests,i=i):
                lu_requests = []

           

        self.loop(batch=lu_requests,final=True)
        
        # Convert to DataFrame
        return pd.DataFrame(orderbook).transpose().reset_index().rename(columns={0:"qty",1:"price",2:"direction","index":"orderid"})


    def convert_sorted_to_dataframe(self,orderbook):
        dflob = pd.DataFrame(list(dict(orderbook[0]).items()))
        dflob = dflob.rename(columns={0:"price",1:"qty"})
        dflob['direction'] = -1

        dflos = pd.DataFrame(list(dict(orderbook[1]).items()))
        dflos = dflos.rename(columns={0:"price",1:"qty"})
        dflos['direction'] = 1
        dflob = dflob.append(dflos)
        return dflob

    def calc_lob_by_price_native(self,time=None, depth=20, write_lu=False):
        query = {}
        if time is not None:
            query["timestamp"] = {"$lt":(time+10000)}

        lob_s = SortedDict({})
        lob_b = SortedDict(neg,{})
        
        lu_requests = None
        if write_lu:
            lu_requests = []

        cs = self.ticks.find(query, self.basicfilter).sort("msgid",1)
        i = 0
        tupl = None
        prevtime = 0
        prevmid = 1
        en = None
        for nex in cs:
            if en is not None:
                if time and en['timestamp'] >= time:
                    break
                    
                i += 1
                ttp = en['timestamp']-prevtime # time to previous used by volatility calculation
                p = en['price']
                if en['type'] not in ['trade','cross','stop']:
                    obj = None
                    d = en['direction']
                    if d == 1:
                        obj = lob_s
                    elif d == -1:
                        obj = lob_b
                    else:
                        print('error')

                    if en['type'] in ['execute','fill']:
                        if obj.items()[0][0] != p:
                            print('Warning!! Buy/sell not at touch! (touch,actionprice,obj)', [obj.items()[0][0], p, en]) # Buy and sell always at the touch
                            self.warnings.append({'warning':'Buy/sell not at touch! (touch,actionprice,obj) - Hint: timestamp of 34200000 is exchange opening time', 'detail':[obj.items()[0][0], p, en]})
                            
                    if p not in obj:
                        obj[p] = en['qty']
                    elif obj[p]==-en['qty']:
                        obj.pop(p)
                    else:
                        obj[p] += en['qty']

                if write_lu:                
                    if depth is None:
                        tupl = lob_b.items(), lob_s.items()
                    else:
                        tupl = lob_b.items()[:depth], lob_s.items()[:depth]

                    if len(tupl[0]) > 0 and len(tupl[1]) > 0:
                        bid = tupl[0][0][0]
                        ask = tupl[1][0][0]
                        mid = (bid+ask)/2
                        
                        ttn = nex['timestamp']-en['timestamp']
                        
                        logror = None if ttp == 0 else np.log(mid/prevmid)/ttp #logarithmic rate of return
                        sets = {"bid":bid,"ask":ask,"mid":mid}
                        if logror is not None:
                            sets['logror'] = logror
                        sets['ttp'] = ttp
                        sets['ttn'] = ttn
                        lu_requests.append(UpdateOne({"_id": en['_id']},{"$set":sets}))
                        
                        
                        assert bid <= ask, (bid, ask) # Bid is always smaller than ask
                        if bid == ask:
                            print('Warning: bid==ask')
                            assert not_strict
                        prevmid = mid

                    lu_requests.append(UpdateOne({"_id": en['_id']},{"$set":{"lob": tupl}}))

                    if en['type'] in ['trade','cross']: # Reconstruct direction by selecting closest distance to touch
                        assert len(lob_b.items())>0 and len(lob_s.items())>0

                        touch_b = lob_b.items()[0][0] 
                        touch_s = lob_s.items()[0][0] 
                        d = 0
                        if 'direction' in en:
                            d = en['direction']
                        else:
                            d = 1 if touch_s-p < p-touch_b else -1
                            if p-touch_b == touch_s-p:
                                d = 0
                        lu_requests.append(UpdateOne({"_id": en['_id']},{"$set":{"direction": d}}))

                if self.loop(batch=lu_requests,i=i):
                    lu_requests = []

                prevtime = en['timestamp']
            en = nex
            

        self.loop(batch=lu_requests,final=True)
        
        if depth is None:
            tupl = lob_b.items(), lob_s.items()
        else:
            tupl = lob_b.items()[:depth], lob_s.items()[:depth]
        return tupl
    
    def read_lob_by_price_mongo(self, time):
        cs = self.ticks.find({"timestamp": {"$lt":time}}).sort("msgid",-1).limit(1)
        return list(cs[0]['lob'])
        
        
    def calc_lob_by_price_mongo(self, time):
        cs = self.ticks.aggregate(
           [
             { "$match": { #"type": {"$nin":["trade","cross"]} not needed, because these types do not have the field last_action
                         "timestamp": {"$lt":time}
                        , "last_action": {"$gte":time}
                      }},
             {
               "$group":
                 {
                   "_id": {"price":"$price","direction":"$direction"},
                   "volume_avail_sum": {"$sum": "$qty"}
                     # sum +1 if sell, -1 if buy, 0 else
                 }
             }
             ,{"$project":{"_id":0,"price":"$_id.price","direction":"$_id.direction","qty":"$volume_avail_sum"}}
           ]
        )
        return pd.DataFrame(list(cs))
    
    def calc_lob_by_order_mongo(self,time): # This function may even work before normalisation
        cs = self.ticks.aggregate(
           [
             { "$match": { 
                         "timestamp": {"$lt":time}
                        , "last_action": {"$gte":time}
                      }},
             {
               "$group":
                 {
                   "_id": "$id",
                   "price": { "$max": "$price"},
                   "last_action": { "$max": "$timestamp"},
                   "count": { "$sum": 1},
                   "volume_avail_sum": {"$sum": "$qty"},
                   "direction": {"$sum": {"$switch":{
                       "branches": [
                           {
                               "case": {"$eq":["$type","sell"]},
                               "then": 1
                           },
                           {
                               "case": {"$eq":["$type","buy"]},
                               "then": -1
                           }],
                           "default": 0
                       }}}
                 }
             }
             ,{"$project":{"_id":0,"price":"$price","direction":"$direction","qty":"$volume_avail_sum","orderid":"$_id"}}
           ]
        )
        return pd.DataFrame(list(cs))