import preprocessing.preglobal as pg
import os
import json
from difflib import Differ
    
def start_one(tbl, i):
    with open(pg.folder['subtitle']+'/'+i['id']+'.json', 'rb') as f:
        res = json.load(f)
    
    res = reformat(res['response']['results'])
    stream = streamline(res)
        
    tbl2 = pg.client[i['id']]['subtitle']
    for j in stream:
        j['startTime'] = float(j['startTime'].replace('s',''))
        j['endTime'] = float(j['endTime'].replace('s',''))
        tbl2.insert_one(j)
    
    tbl.update_one({ '_id': i['_id'] },{ '$set': {  'subtitles':1} }, upsert=False)
                            
    return True
    
def start():
    return pg.start_step(start_one, {'subtitle_downloaded':1,'subtitles':None})
                            
def response_to_dict(response):
    res = []
    for i in response.results:
        a = {}
        a['channel_tag'] = i.channel_tag
        a['language_code'] = i.language_code
        alt = []
        for j in i.alternatives:
            b = {}
            b['transcript'] = j.transcript
            b['confidence'] = j.confidence
            words = []
            for k in j.words:
                w = {}
                w['start_time'] = k.start_time.seconds + k.start_time.nanos*1e-9
                w['end_time'] = k.end_time.seconds + k.end_time.nanos*1e-9
                w['word'] = k.word
                w['confidence'] = k.confidence
                words.append(w)
            b['words'] = words
            alt.append(b)
        a['alternative'] = alt
        res.append(a)
    return res
                            
def clean(x):
    return ''.join(filter(str.isalpha, x.lower()))
                            
def reformat(res):
    d = Differ()
    for i in res:
        alt = i['alternatives']
        if len(alt) == 0:
            continue
        if len(alt) > 1:
            awords = [clean(a['word']) for a in alt[0]['words']]
            for j in alt[1:]:
                cwords = [clean(a) for a in j['transcript'].split(' ')[1:]]

                position = 0
                diff = list(d.compare(awords,cwords))
                lastact = ''
                for k in range(0, len(diff)):
                    act = diff[k][0]
                    if act == '?':
                        continue

                    if act == ' ':
                        assert diff[k][2:]==awords[position],[diff[k][2:],awords[position]]
                        position+=1
                    elif act == '-':
                        assert diff[k][2:]==awords[position],[diff[k][2:],awords[position]]
                        position+=1
                    elif act == '+':
                        if 'alternative' not in alt[0]['words'][position-1]:
                            alt[0]['words'][position-1]['alternative'] = []
                        alt[0]['words'][position-1]['alternative'].append(diff[k][2:])

    return res

def streamline(inp):
    outp = []
    sid = 0
    for i in inp:
        alt = i['alternatives']
                
        if len(alt) == 0 or 'words' not in alt[0] or len(alt[0]['words']) == 0:
            continue
            
        for i in alt[0]['words']:
            i['sentence'] = sid
        outp.extend(alt[0]['words'])
        sid +=1
    return outp
                                    