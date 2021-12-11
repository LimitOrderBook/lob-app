import os
import sys
from flexx import flx, event
import threading
from tornado.web import StaticFileHandler, RequestHandler
import time
from flexx.ui.widgets import Widget
import asyncio
from bson.objectid import ObjectId
import json 
import frontend.benplot as bp
import frontend.stock_analytics as salib
import copy
import preprocessing.preglobal as pg
from bson.objectid import ObjectId
import frontend.state_controller as sclib
import traceback


import cgi


initial_state = {
    'sa':{
        'active_id':None
    },
    'filter':{
        'name':'Business hours / 1000 bins',
            'range':
            {
            'time':[0,0],
            'price':[0,0]
            },
            'numbins':0,
            'cursor':salib.to_date_i(0)
    },
    'histogram': False,
    'plots0': 
        {
            'type': 'midprice',
            'subtype': '', #possible values: '', '_buysell', '_sell', '_buy'
            'relative': False # False = Absolute, True = relative (only applicable for subtype=_buysell)
        },
    'plots1':     {
            'type': 'midprice',
            'subtype':'',
            'relative': False
        },
    'plots2':     {
            'type': 'midprice',
            'subtype': '',
            'relative': False
        },
    'plots3':     {
            'type': 'midprice',
            'subtype': '',
            'relative': False
        }
    ,
    'cursor': None
    
}


class MessageBox(flx.Label):

    CSS = """
    .flx-MessageBox {
        overflow-y:scroll;
        background: #e8e8e8;
        border: 1px solid #444;
        margin: 3px;
    }
    """

    def init(self):
        super().init()
        global window
        self._se = window.document.createElement('div')


class BenVideoWidget(Widget):


    DEFAULT_MIN_SIZE = 100, 100

    source = event.StringProp('', settable=True, doc="""
        The source of the video. This must be a url of a resource
        on the web.
        """)
    time = event.IntProp('', settable=True, doc="""
        
        """)
    
    def _create_dom(self):
        global window
        node = window.document.createElement('video')
        node.controls = 'controls'
        node.textContent = 'Your browser does not support HTML5 video.'

        self.src_node = window.document.createElement('source')
        self.src_node.type = 'video/mp4'
        self.src_node.src = None
        node.appendChild(self.src_node)
        
        self._addEventListener(node, 'playing', self.playing, False)
        self._addEventListener(node, 'ended', self.pause, False)
        self._addEventListener(node, 'pause', self.pause, False)
        
        return node

    def _render_dom(self):
        return None

    @event.reaction('source')
    def __source_changed(self):
        self.src_node.src = self.source or None
        self.node.load()
   
    @event.reaction('time')
    def __time_changed(self):
        self.node.currentTime = self.time or 0
    
    @event.emitter
    def playing(self):
        return {'time':self.node.currentTime}
    
    @event.emitter
    def pause(self):
        return {'pause':True}
        

        
class app(flx.PyWidget):
       
    def to_video_time(self,s): # calculates stock time to video time
        
        t = salib.to_millis(s)/1000
        for i in self.sa.time_mapping:
            if t >= i['stock_time_start']/1000 and t < i['stock_time_end']/1000:
                return t-i['stock_time_start']/1000+i['video_time_start']
        return None
        
        
    def to_stock_time(self,t): # calculates video time to stock time
        for i in self.sa.time_mapping:
            if t >= i['video_time_start'] and t < i['video_time_end']:
                return (t-i['video_time_start'])*1000+i['stock_time_start']
        return None
        
    def mongoconnect(self):
        client = pg.get_db_client()
        self.tabl = client[self.sa.sc['active_id']]['subtitle']
        
    def load_sentence(self,t):
        query = [
            {'$match':{'endTime':{'$gt':t}}},
            {'$project':{"sentence":1,"endTime":1}}, 
            {'$sort':{"endTime":1}},
            {'$limit':1}
        ]
        cs = self.tabl.aggregate(query)
        res = list(cs)
        print(res)
        if len(res) == 1:
            sen = res[0]['sentence']
            cs = self.tabl.aggregate([
                {'$match':{'sentence':sen}},
                {'$sort':{"startTime":1}}
            ])
            res = list(cs)
            valid_min = min(res[0]['startTime'],t)
            valid_max = max(res[-1]['endTime'],t)
            sentence = ' '.join([r['word'] for r in res])
            print(valid_min, valid_max, sentence)
            return valid_min, valid_max, res
        else:
            print("no more sentences")
            return t,1e6, []
        
    def render(self,t):
        res = '<span style="white-space: normal">'
        if len(self.sentence) > 0:
            res += "<b>Sentence "+str(self.sentence[0]['sentence'])+"</b><br>"
        for a in self.sentence:
            currentword = False
            if t >= a['startTime'] and t <= a['endTime']:
                currentword = True
            
            word = a['word'] if 'overwrite' not in a else a['overwrite']
            word = word
            word = word if 'alternative' not in a else '<i>'+word+'</i>'
            res += word if not currentword else "<b>"+word+"</b>"
            res += " "
         
        res += "</span>"
        
        if self.currentrender != res:
            self.currentrender = res
            
            try:
                self.time_label.set_text('Video-time: {:.2f}s'.format(t))
                self.people_label.set_html(res)
                self.sc['cursor'] = salib.to_date_i(self.to_stock_time(t))
                self.update_graphs(layout=True, data=False, origin='render')
            except:
                print('error in writing current subs')
        
        
        
    def load_id(self,i):
        self.idloaded = i
        cs = self.tabl.aggregate([
                {'$match':{'_id':ObjectId(i)}},
            ])
        res = list(cs)
        for a in res:
            a['_id'] = str(a['_id'])
        if len(res) > 0:
            self.player.set_time (res[0]['startTime'])

    
    def update_overwrite(self, i, text):
        self.tabl.update_one({
              '_id': ObjectId(i)
            },{
              ('$set' if text else '$unset'): {
                'overwrite': text
              }
            }, upsert=False)
        self.load_id(i)
        self.valid_min = self.valid_max = -1
        
    def updatesubs(self,x):
        t = self.to_video_time(self.sc['cursor']) if self.sc['cursor'] else 0
        if t is not None and t != self.lastt:
            self.lastt = t
            if t > self.valid_max or t < self.valid_min:
                self.valid_min, self.valid_max, self.sentence = self.load_sentence(t)
            self.render(t)
        
    def loop(self):
        loop_ = asyncio.new_event_loop()
        asyncio.set_event_loop(loop_)
        
    def update_graphs(self, layout=False, data=True, origin=None):
        self.messages.set_html('Updating graphs, please wait (be patient)')
        try:
            print('UPDATE GRAPHS', origin)
            for i in self.plots:
                if self.sc['cursor']:
                    self.sc['filter']['cursor'] = self.sc['cursor']
                elif 'cursor' in self.sc['filter']:
                    del self.sc['filter']['cursor']

                i.update(filter=self.sc['filter'], histogram=self.sc['histogram'], numbins=self.sc['filter']['numbins'], layout=layout, data=data)
            print('//DONE//')    
            self.messages.set_html('')
        except:
            self.messages.set_html('<pre>'+cgi.escape(traceback.format_exc())+'</pre>')
        
    
        
    def init(self, *args, **kwargs):
        print('***********************************************************')
        print('***********************************************************')
        print('***********************************************************')
        print('*********************   INIT CALLED  **********************')
        print('***********************************************************')
        print('***********************************************************')
        print('***********************************************************')
        print(self)
        
        self.plots = []
        self.lastt = 0
        self.playing = False
        self.currenttime = 0
        self.lock = threading.Lock()
        self.clocktime = 0
        self.currentrender = ""
        self.idloaded = -1
        self.valid_min = -1
        self.valid_max = -1
        self.v_init=0
        
        parts = self.session.request.uri.split('?')[0].split('/')
        while parts[-1] == '':
            del parts[-1]
            
        if parts[-1] == 'app':
            parts[-1] = '5d9f9b8f2d0d990b62015cb8' #latest
            
        self.current_sc_name = parts[-1]
        self.sc = sclib.state_controller(state=initial_state, root=None)
        self.sc.lock = True
            
        self.sa = salib.stock_analytics(self.sc['sa']) #default active id
        self.sc['sa'].register_callback('active_id', self._change_kn_callback)
        
        
        with flx.VFix():
            with flx.HFix(flex=0.08):
                with flx.VFix(flex=0.5):
                    self.start_button = flx.Button(text='Start')
                with flx.VFix(flex=1):
                    self.save_state = flx.Button(text='Save as:')
                with flx.VFix(flex=1):
                    self.sate_name = flx.LineEdit(placeholder_text='<Please name me>')
                with flx.VFix(flex=1.5):
                    self.state_combo = flx.ComboBox(options=[a['name'] for a in self.sc.load_list()], selected_index=[a['name'] for a in self.sc.load_list()].index('latest'), style='width: 100%')
             
                flx.VFix(flex=3)
            with flx.HFix(flex=1):
                    
                with flx.VFix(flex=1):
                    self.player = BenVideoWidget()
                with flx.VFix(flex=1):
                    self.keynote_list = [a['id'] for a in pg.get_kn_entries({'lob_imported':1})]
                    
                    self.current_keynote = flx.ComboBox(options=self.keynote_list, selected_index=0, style='width: 100%')
                    
                    self.people_label = flx.Label(flex=1, minsize=150, text='Please press "Start" to start the application!')
                    with flx.HBox():
                        flx.Label(flex=1,text='Binsize (max 1000):')
                        self.text_numbins = flx.LineEdit(placeholder_text='Number of bins', text=str(self.sc['filter']['numbins']))
                        self.time_label = flx.Label(flex=1)
                with flx.VFix(flex=1):#minsize=450):
                    self.messages = MessageBox(flex=1)
        
            with flx.HFix(flex=1):  
                self.plots.append( bp.NBenPlot(self.sa, self.sc['plots0']))
                self.plots.append( bp.NBenPlot(self.sa, self.sc['plots1']))
            with flx.HFix(flex=1):
                self.plots.append( bp.NBenPlot(self.sa, self.sc['plots2']))
                self.plots.append( bp.NBenPlot(self.sa, self.sc['plots3']))
                
            self.sc['filter']['range'].register_callback('time', lambda x: self.update_graphs(origin='_relayout'))
            self.sc['filter']['range'].register_callback('price', lambda x: self.update_graphs(origin='_relayout'))
            self.sc['filter'].register_callback('numbins', self.update_cb)
            
            self.sc.register_callback('cursor', self.updatesubs)

        threading.Thread(target=self.loop).start()
     
    def update_cb(self,x):
        if str(self.sc['filter']['numbins']) != self.text_numbins.text:
            self.text_numbins.set_text( str(self.sc['filter']['numbins']))
            
        self.update_graphs(origin='_updacb')
        
        
    @flx.reaction('text_numbins.submit')
    def _updacb(self, *events):
        if not self.init_done():
            return
            
        try:
            self.sc['filter']['numbins'] = min(int(self.text_numbins.text),1000)
            
        except:
            self.messages.set_html('numbins is not an integer')
        
    @flx.reaction('start_button.pointer_down')
    def _start_app(self, *events):
        print('STARTBUTTON',self)
        self.sc.root.lock = False
        self.sc.reload(self.current_sc_name)
        
    @flx.reaction('save_state.pointer_down')
    def _save_state(self, *events):
        if self.sate_name.text == '':
            self.messages.set_html('Please enter a name!')
            return
        
        self.sc.save(self.sate_name.text)
        self.messages.set_html('State saved.')
        print ('save state')
        
    @flx.reaction('state_combo.selected_index')  # State loader (z.B. long lasting lo)
    def _change_state_combo(self, *events):
        if not self.init_done() or self.sc.root.lock:
            return
            
        self.sc.reload(self.sc.load_list()[self.state_combo.selected_index]['_id'])
    
    @flx.reaction('player.playing')  #
    def _position_change(self, *events):
        print(events)
        for ev in events:
            print('reaction', ev['time'])
            #with self.lock:
            self.clocktime = time.time()
            self.currenttime = ev['time']
            self.playing = True    
            
    @flx.reaction('plots*.plt.plt_click')  #
    def _click(self, *events):
        for ev in events:
            id = [g.plt for g in self.plots].index(ev['source'])
            print('Plot Number: ',id)
            print(ev)
            
            g = self.sa.graphlist[self.plots[id].get_name()]
            if g['xaxis'] == 'time':
                self.sc['cursor'] = ev['x']
                self.update_graphs(layout=True, data=True, origin='_click')
                vt = self.to_video_time(self.sc['cursor'])
                if vt:
                    self.player.set_time (vt)
    
    @flx.reaction('plots*.plt.plt_relayout')  #
    def _relayout(self, *events):
        if not self.init_done():
            return
        change=False    
        for ev in events:
            if 'xaxis.autorange' not in ev['relayout'] and 'xaxis.range[0]' not in ev['relayout']:
                break
        
            id = [g.plt for g in self.plots].index(ev['source'])
            print('Plot Number: ',id)
            print(ev)
            
            g = self.sa.graphlist[self.plots[id].get_name()]
            
            mapfunct = (lambda x: salib.to_millis(x)) if g['xaxis'] == 'time' else (lambda x: x*10000)
            
            if 'xaxis.autorange' in ev['relayout']:
                self.sc['filter']['range'][g['xaxis']] = self.sa.filters['default']['range'][g['xaxis']].copy()
            elif 'xaxis.range[0]' in ev['relayout']:
                self.sc['filter']['range'][g['xaxis']] = [mapfunct(ev['relayout']['xaxis.range[0]']), mapfunct(ev['relayout']['xaxis.range[1]'])]

     
    @flx.reaction('player.pause')  #
    def _pause(self, *events):
        self.playing = False
               
    @flx.reaction('current_keynote.selected_index') # Change keynote
    def _change_graph(self, *events):
        self.sa.sc['active_id'] = self.keynote_list[self.current_keynote.selected_index]
            
        
    def _change_kn_callback(self,x):    
        # Change keynote!
        print('=============CHANGING KEYNOTE===================',self.sa.sc['active_id'])
        print(self)
        if self.sa.sc['active_id'] != self.keynote_list[self.current_keynote.selected_index]:
            print(self.sc)
            print(self.sa.sc)
            self.current_keynote.set_selected_index (self.keynote_list.index(self.sa.sc['active_id']))
        
        
        
        self.mongoconnect()
        
        self.player.set_source('/static/'+self.sa.sc['active_id']+'.mp4?v=1234')
        
        self.update_graphs(layout=False, data=True, origin='_change_graph')
        
        
        self.valid_max = -1
        self.valid_min = -1
        self.lastt = -1
        
        if self.sc['cursor']:
            vt = self.to_video_time(self.sc['cursor'])
            if vt:
                self.player.set_time (vt)
                
            res = self.sa.ticks.aggregate([{'$match':{'timestamp':{'$gt':salib.to_millis(self.sc['cursor'])}}},{'$sort':{'timestamp':1}},
                                     {'$project':{'mid':1}},{'$limit':1}])
            if len(res) == 1:
                mid = res[0]['mid']
                self.sc['filter']['range']['price'] = [mid-2000, mid+2000]
        
        print(self.sc)
        print(self.sa.sc)
    
    
    def init_done(self):
        print('core',self.v_init)
        if self.v_init==0:
            return True
        self.v_init-=1
        return False


class RestartHandler(RequestHandler):
    def initialize(self):
        return True
    
    def get(self):
        sys.exit()
        
        
def start_gui():
    print('static folder',pg.folder['video_sd'])
    a = flx.App(app, 'app', title='Limit Order Book Application')
    tornado_app = flx.create_server(port=8123,host='0.0.0.0').app
    tornado_app.add_handlers(r".*", [
    (r"/static/(.*)", StaticFileHandler, {"path": pg.folder['video_sd'] }),
    (r"/()", StaticFileHandler, {"path": "index.html" }),
        (r"/demo.gif()", StaticFileHandler, {"path": "demo.gif" }),
        (r"/restart", RestartHandler),
    ])
    a.serve()
    
    flx.start()