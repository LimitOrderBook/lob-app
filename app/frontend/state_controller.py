import preprocessing.preglobal as pg
import datetime
from bson.objectid import ObjectId

def deepcopy(iv, rr, root=False):
    transform = lambda x: deepcopy(x, rr) if isinstance(x, dict) else x

    res = {} if root else state_controller(root=rr)
    
    for key, value in iv.items():
        value = transform(value)
        if isinstance(value, list):
            for i in range(len(value)):
                value[i] = transform(value[i])
        res[key] = value    
        
    return res

def todict(iv):
    transform = lambda x: todict(x) if isinstance(x, state_controller) else x

    res = {}
    
    for key, value in iv._state.items():
        value = transform(value)
        if isinstance(value, list):
            for i in range(len(value)):
                value[i] = transform(value[i])
        res[key] = value    
        
    return res

def deepupdate(new, old):
    def transform(x,y,z):
        if isinstance(x, dict):
            deepupdate(x,y[z])
        elif isinstance(x, list):
            listchange = False
            for i in range(len(x)):
                if isinstance(x[i], dict):
                    deepupdate(x[i], y[z][i])
                else:
                    if y[z][i] != x[i]:
                        y[z][i] = x[i]
                        listchange = True
            if listchange:
                y.exec_callback(z)
        else:
            y[z] = x


    for key, value in new.items():    
        value = transform(value, old, key)
              


    
class state_controller:    
    def connectdb(self):
        if not self.tabl:
            client = pg.get_db_client()
            self.tabl = client['global']['states']
            
    def save(self, name):
        self.connectdb()
        dic = todict(self)
        if len(list(self.tabl.find({'name':name}))) > 0:
            self.tabl.update_one({'name':name},{'$set':{'modified': datetime.datetime.now(), 'state': dic}})
        else:
            self.tabl.insert_one({'name':name, 'modified': datetime.datetime.now(), 'state': dic})
    
    def load_list(self):
        self.connectdb()
        if self.controllerlist is not None:
            return self.controllerlist
        self.controllerlist = list(self.tabl.aggregate([{'$project':{'_id':1,'name':1}}]))
        return self.controllerlist
        
    def load(self,name):
        self.connectdb()
        q = {'_id':name} if isinstance(name, ObjectId) else {'_id':ObjectId(name)}
        res = self.tabl.find_one(q)
        return res['state']
        
        
    def __init__(self, root, state=None, loadfromdb=None):
        self.isqueuing = False
        self.lock = False
        self.cbqueue = []
        self._cb = {}
        self.tabl = None
        self.controllerlist = None
        self.startmode = False
        
        self.root = root if root is not None else self
        self._state = {}
        if loadfromdb is not None:
            state = self.load(loadfromdb)
        if state is not None:
            self.root.startmode = True
            self._state = deepcopy(state, rr=self.root, root=True)
            self.root.startmode = False
    
    def queueadd(self,cb,arg):
        if self.root.isqueuing:
            print('added to queue',id(self.root),id(self),{'cb':cb,'arg':arg})
            self.root.cbqueue.append({'cb':cb,'arg':arg})
        else:
            cb(arg)
        
    def queuestartstop(self,start=True):
        if start:
            self.root.isqueuing = True
        else:
            print('---------=== START QUEUE ===----------------')
            try:
                while len(self.root.cbqueue)>0:
                    elm = self.root.cbqueue[0]
                    del self.root.cbqueue[0]
                    print('executing',elm,'with current state',self.root)
                    elm['cb'](elm['arg'])
            except Exception as e:
                raise e
                print('ERROR WHILE IN QUEUE:', str(e))
            self.root.isqueuing = False
            print('---------=== END QUEUE ===----------------')
        
        
        
    def reload(self, name):
        s = self.load(name)
        print('------------ START RELOAD -------------------')
        self.queuestartstop(start=True)
        deepupdate(s, self)
        self.queuestartstop(start=False)
        print('------------ FINISHED RELOAD -------------------')
    
    def __repr__(self):
        return repr(self._state)
        
    def __str__(self):
        return str(self._state)
        
    def __setitem__(self, key, value):
        return self.set_silent(key,value, silent=False)
        
    def set_silent(self, key, value, silent=True):
        if self.root.lock or (key in self._state and self._state[key] == value):
            return
            
        self._state[key] = value
        
        if self.root.startmode:
            return
            
        if not silent:
            self.exec_callback(key)

        self.root.save('latest')
            
        print ('STATE CHANGED', self)
        return
        
        
    def __contains__(self, key):
        return key in self._state
        
    def __delitem__(self,key):
        del self._state[key]
        
    def __getitem__(self, key):
        return self._state[key]
    def new_context(self, key):
        self._state[key] = state_controller()
        return self._state[key]
        
    def exec_callback(self, key):
        if key in self._cb:
            for i in self._cb[key]:
                print ('EXEC CALLBACK FOR',key)
                self.queueadd(i,self[key])
        
    def register_callback(self, key, cb, callnow=False):
        print('REGISTER CALLBACK', id(self.root), id(self), key, cb)
        if key not in self._cb:
            self._cb[key] = []
        self._cb[key].append(cb)
        if callnow:
            self.exec_callback(key)