import preprocessing.preglobal as pg
import numpy as np
import cv2
import pickle
import matplotlib.pyplot as plt
import json
import datetime
import os
from pymongo import MongoClient, UpdateMany, UpdateOne, InsertOne
import ipywidgets as widgets
from ipywidgets import interact, interactive, fixed, interact_manual
import pandas as pd
import math
    
def calc_fontsize(box):
    box = [{'x':a['x'] if 'x' in a else 0, 'y':a['y'] if 'y' in a else 0} for a in box]
    d = []
    for i in range(0,4):
        d.append(np.sqrt(math.pow(box[i]['x']-box[(i+1)%4]['x'],2) + math.pow(box[i]['y']-box[(i+1)%4]['y'],2)))
    return np.min(d)


def start_one(tbl, ii):
    ocr_folder = pg.folder['ocr']
    
    
    tt = pg.get_db_client()[ii['id']]['ocr']
    
    
    
    print('Import OCR',ii['id'])

    # Import OCR file of video
    j = ""
    with open(ocr_folder+'/'+ii['id']+'.json', 'r') as handle:
        j = json.load(handle)


    results = []
    iii = 0
    # Flatten OCR complex json
    for a in j['response']['annotationResults']:
        for textid in range(0,len(a['textAnnotations'])):
            b = a['textAnnotations'][textid]
            text = b['text']
            for segmentid in range(0,len(b['segments'])):
                c = b['segments'][segmentid]
                #print(c)
                segmentlength = float(c['segment']['endTimeOffset'].replace('s',''))-float(c['segment']['startTimeOffset'].replace('s',''))
                for frameid in range(0,len(c['frames'])):
                    d = c['frames'][ frameid]
                    time = d['timeOffset']
                    box = d['rotatedBoundingBox']['vertices']
                    results.append({'id':iii,'textid':textid, 'segmentid':segmentid,'frameid':frameid, 'text':text, 'time':float(time.replace('s','')), 'segmentlength':segmentlength,'box':box})
                    iii+=1
    mresults = []
    for i in results:
        i['fontsize'] = calc_fontsize(i['box'])
        mresults.append(InsertOne(i))
        
    tt.drop()
    tt.bulk_write(mresults, ordered=False)
    
    pg.get_db_client()[ii['id']].command(
            {
                "createIndexes" : 'ocr',
                "indexes" : [
                    {
        "v" : 2,
        "key" : {
            "_fts" : "text",
            "_ftsx" : 1
        },
        "name" : "ocr",
        "weights" : {
            "text" : 1
        }
                    }
                        ]})
    
    tbl.update_one({ '_id': ii['_id'] },{ '$set': { 'ocr_imported':1} }, upsert=False)
                            
    return True
    
def start():
    return pg.start_step(start_one, {'ocr_downloaded':1, 'ocr_imported':None}) 