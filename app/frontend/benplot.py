from flexx import flx, app,event
from flexx.ui.widgets import Widget
import numpy as np

app.assets.associate_asset(__name__, 'https://cdn.plot.ly/plotly-latest.js')



def draw_graph(g,f, sa, data=True, histogram=True, numbins=None, relative=False):     
    fig = { 
        'layout': {
            'title': g['name'],
            'hoverinfo': 'y',
            'hoverlabel': {"font":{"size":10}},
            'margin': {'t': 30, 'b': 22},
            'height':  250,
            'xaxis': {
                'fixedrange': False,
                'showline':  True,
                'zeroline': False,
                'showgrid': True,
                'showticklabels':  True
            },
            'yaxis': {
                'fixedrange': True,
                'showline':  True,
                'zeroline': False,
                'showgrid': True,
                'showticklabels':  True
            },
            'plot_bgcolor': 'white',
            'paper_bgcolor': 'white'}
        }
        
        
        
    if data:
        if numbins:
            f['numbins'] = numbins
        res = sa.calculate_graph(g,f, histogram=histogram, splitallowed=True)
        
        if 'combination' in g:
            fig['data'] = []
            for a in res:
                fig['data'].append({ 
                    'x': a['x'],
                    'y': a['y'],
                    'colorscale': [[0, 'rgba(255, 255, 255,0)'], [1, 'rgba(0,0,255,1)']],
                    'showscale':  True,
                    'name': a['name']})
        elif 'type' in g and g['type'] == 'heatmap':
            midpnt = (0 - np.amin(res[0])) / (np.amax(res[0]) - np.amin(res[0]))
            d1 = 1/ (np.amax(res[0]) - np.amin(res[0]))
            fig['data'] = [{ 
                'x': res[2],
                'y': res[1],
                'z': list(zip(*res[0])),
                'type':'heatmap',
                'colorscale': [[0, 'rgba(0, 0, 255,1)'],[midpnt-d1, 'rgba(255, 255, 255,1)'], [midpnt, 'rgba(100, 100, 100,1)'],[midpnt+d1, 'rgba(255, 255, 255,1)'],[1, 'rgba(255,0,0,1)']],
                'showscale':True}]
        else:
            fig['data'] = [{ 
                'x': res[0],
                'y': res[1],
                'colorscale': [[0, 'rgba(255, 255, 255,0)'], [1, 'rgba(0,0,255,1)']],
                'showscale':  True}]
        
        
    if 'cursor' in f and ('xaxis' not in g or g['xaxis'] == 'time'):
        fig['layout']['shapes'] = [{
            'type': 'line',
            'x0': f['cursor'],
            'y0':0,
            'x1':f['cursor'],
            'yref':'paper',
            'y1':1,
            'line': {
                'color': 'rgb(128,0,128)',
                'width': 1
            }
        }]
        
    if ('aggtype' in g and g['aggtype'] in ['sum']) or ('type' in g and g['type'] == 'bar'):
        if data:
            for i in fig['data']:
                i['type'] = 'bar'
        fig['layout']['barmode'] = 'relative' if relative else 'stack'
        if relative:
            fig['layout']['barnorm'] = 'percent'
    return fig


def getsi(t):
    si = ['', '_buysell', 'XXX', '_sell', '_buy'].index(t['subtype'])
    if si==1 and t['relative']:
        si=2
    return si

class NBenPlot(flx.PyWidget): 

    def init_done(self):
        if self.v_init==0:
            return True
        self.v_init-=1
        return False
        
    @flx.reaction('sellbuymode.selected_index')
    def _change_sellbuymode(self, *events):
        if not self.init_done():
            return
        si = self.sellbuymode.selected_index
        self.sc['subtype'] = ['', '_buysell', '_buysell', '_sell', '_buy'][si]
        self.sc['relative'] = (si==2)
    
    def _set_sellbuymode(self, x):
        if not self.init_done():
            return
            
        si = self.sellbuymode.selected_index    
        if self.sc['relative'] != (si==2):
            self.sellbuymode.set_selected_index(2)
        elif self.sc['subtype'] != ['', '_buysell', 'XXX', '_sell', '_buy'][si]:
            self.sellbuymode.set_selected_index(['', '_buysell', 'XXX', '_sell', '_buy'].index(self.sc['subtype']))
        
        
        self.update(layout=True, data=True)
        
        
    @flx.reaction('sgraph.selected_index')
    def _change_graph(self, *events):
        if not self.init_done():
            return
        si = list(self.sa.get_graphlist())[self.sgraph.selected_index]
        self.sc['type'] =  si
        
    
    def _set_graph(self,name):
        if list(self.sa.get_graphlist()).index(self.sc['type']) != self.sgraph.selected_index:
            self.sgraph.set_selected_index (list(self.sa.get_graphlist()).index(self.sc['type']))
        self.update(layout=True, data=True)
        
    def get_name(self):
        return self.sc['type']+self.sc['subtype']
        
    def update(self, filter=None, histogram=None, numbins=None, layout=False, data=True):
    
        
            
        print('update')
        if filter is not None:
            self.filter = filter
            
    
            
        if histogram is not None:
            self.histogram = histogram
            
        if numbins is not None:
            self.numbins = numbins
            
        if self.initial:
            self.initial = False
            if list(self.sa.get_graphlist()).index(self.sc['type']) != self.sgraph.selected_index:
                self.sgraph.set_selected_index (list(self.sa.get_graphlist()).index(self.sc['type']))
            return self._set_sellbuymode(0)
            
        fig = draw_graph(self.sa.graphlist[self.get_name()], self.filter, self.sa, data=data, histogram=self.histogram, numbins=self.numbins, relative=self.sc['relative'])
        
        if data:
            self.plt.set_data(fig['data'])
        if layout:
            self.plt.set_layout(fig['layout'])
            
    def init(self, sa, sc):
        self.plt = None
        self.numbins=None
        self.v_init=0
        self.initial=True
    
        self.sa = sa
        self.sc = sc
        self.initial = True
        print('LOCAL SC',sc)
        self.sc.register_callback('type', self._set_graph, callnow=False)
        self.sc.register_callback('subtype', self._set_sellbuymode, callnow=False)
        self.sc.register_callback('relative', self._set_sellbuymode, callnow=False)
        
        with flx.VBox():
            with flx.HBox():
                self.sgraph = flx.ComboBox(options=list(self.sa.get_graphlist()), selected_index=list(self.sa.get_graphlist()).index(self.sc['type']), style='width: 100%')
                self.sellbuymode = flx.ComboBox(options=['Combined', 'Sell/Buy (Abs)', 'Sell/Buy (rel)','Sell only','Buy only'], selected_index=getsi(self.sc), style='width: 100%')
            self.plt = BenPlt()
        
        print('init BenPlot ok')

class BenPlt(Widget):      
    data = event.ListProp(settable=True, doc="""
        """)

    layout = event.DictProp(settable=True, doc="""
        """)

    config = event.DictProp(settable=True, doc="""
        """)

    @event.reaction('size')
    def __relayout(self):
        global Plotly
        w, h = self.size
        if len(self.node.children) > 0:
            Plotly.relayout(self.node, dict(width=w, height=h))

    @event.reaction('data','layout','config')
    def _init_plot(self):
        global Plotly
        Plotly.newPlot(self.node, self.data, self.layout, self.config)

        self.node.on('plotly_click', self.plt_click)
        self.node.on('plotly_relayout', self.plt_relayout)
        
    @event.emitter
    def plt_click(self, *data):
        return {'x':data[0].points[0].x,'y':data[0].points[0].y}
        
    @event.emitter
    def plt_relayout(self, data):
        return {'relayout':data}