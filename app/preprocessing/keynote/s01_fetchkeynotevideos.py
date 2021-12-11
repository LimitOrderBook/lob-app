from bs4 import BeautifulSoup
import requests
import preprocessing.preglobal as pg
import datetime
import pandas

def get_podcast(id): # Code snippet from https://github.com/dirkprimbs/itunes-podcast-crawler
    theID = str(id)

    lookupurl = 'https://itunes.apple.com/de/lookup?id=' + theID


    luresults = requests.get(lookupurl, timeout=5).json()
    assert 'feedUrl' in luresults['results'][0]

    itunesData = luresults['results'][0]
    feedurl = itunesData['feedUrl']

    xmlfeed = requests.get(feedurl, timeout=5)
    return BeautifulSoup(xmlfeed.content, 'xml')


def get_list(xf):
    llist = []
    for i in xf.channel.find_all('item'):
        msg = {}
        msg['title'] = i.title.text
        if i.subtitle is not None:
            msg['subtitle'] = i.subtitle.text
        else:
            msg['subtitle'] = ""
        msg['text'] = i.summary.text
        msg['url'] = i.enclosure['url']
        try:
            msg['pubdate'] =  datetime.datetime.strptime(' '.join(i.pubDate.text.split(', ')[1].split(' ')[0:4]), 
                                             '%d %B %Y %H:%M:%S') #Get Date from Filename
        except:
            msg['pubdate'] =  datetime.datetime.strptime(' '.join(i.pubDate.text.split(', ')[1].split(' ')[0:4]), 
                                             '%d %b %Y %H:%M:%S') #Get Date from 
        msg['duration'] = i.duration.text
        llist.append(msg)
    return llist

def start():
    # Keynote SD Videos
    xf_sd = get_podcast(275834665)
    
    # Keynot HD Videos
    xf_hd = get_podcast(470664050) 
    # 1080p: 509310064 - 720p is enough and 1080p some does not contain all videos
    
    list_sd = get_list(xf_sd)
    list_hd = get_list(xf_hd)
    
    df_sd = pandas.DataFrame(list_sd)
    df_hd = pandas.DataFrame(list_hd)
    
    df_hd_c = df_hd[['title','url']].rename(columns={'url':'url_hd'})
    df_hd_c['title'] = df_hd_c['title'].map(lambda x: x.replace(' (HD)',''))
    
    df_hd_c = df_hd_c.set_index('title')
    
    print(df_hd)
    print("sd")
    print(df_sd)
    res = df_sd.set_index('title').join(df_hd_c, how='outer').sort_values(by='pubdate',ascending=False).reset_index()
    
    # Manually add dates of actual keynote
    res['eventdate'] = res['pubdate']

    res.loc[res['title']=='Apple Special Event, September 2016','eventdate'] = datetime.datetime(2016,9,7)
    res.loc[res['title']=='Apple Special Event, September 2017','eventdate'] = datetime.datetime(2017,9,12)

    # only select first 25 entries
    res = res[0:25]
    res['id'] = res['eventdate'].map(lambda x: x.strftime('%Y%m%d'))+'_AAPL'

    tbl = pg.get_keynote_tbl()
    assert False
    if len(list(tbl.find({})))>0: # 'List already exists'
        print('List already exists, updating keynotes')
        for i in res.iterrows():
            r = i[1].to_dict()
            if len(list(tbl.find({'id':r['id']}))) == 0:
                print('New keynote found',r['id'])
                tbl.insert_one(r)
    else:
        # write to mongo db
        for i in res.iterrows():
            tbl.insert_one(i[1].to_dict())
    
    return True
