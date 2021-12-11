from pymongo import MongoClient, UpdateMany, UpdateOne, InsertOne, DeleteMany
import os
import urllib
from google.cloud import storage
import subprocess
import time
import requests
import redis

basefolder = os.environ['THDATA'] if 'THDATA' in os.environ else '../../data'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = basefolder+"/google.json"

client = None



folder = {
    'video_sd':basefolder+'/video_sd',
    'video_hd':basefolder+'/video_hd',
    'subtitle':basefolder+'/subtitle_raw',
    'timealignment':basefolder+'/timealignment',
    'audio':basefolder+'/audio',
    'ocr':basefolder+'/ocr',
    'lob':basefolder+'/tradphy_raw'
}

bucket = {
    'audio':'keynotes_audio',
    'ocr':'keynotes_ocr'
}


v_storage_client = None

def storage_client():
    try:
        global v_storage_client
        if v_storage_client is None:
            v_storage_client = storage.Client()
        return v_storage_client
    except Exception as e:
        print(e)
        return None

def get_redis_client():
    
    url = os.environ['REDIS_SERVER'] if 'REDIS_SERVER' in os.environ else '192.168.0.94'
    print('redis',url)
    return redis.Redis(host=url, port=6379, db=0)
    
def connect_mongo():
    global client
    url = 'mongodb://'+os.environ['MONGO_SERVER']+':27017/' if 'MONGO_SERVER' in os.environ else 'mongodb://192.168.0.94:27017/'
    print('mongo',url)
    client = MongoClient(url)
    
def get_db_client():
    global client
    if not client:
        connect_mongo()  
    return client
        
def get_time_tbl():
    return get_db_client()['global']['timekeys']

def get_keynote_tbl():
    return get_db_client()['global']['keynotes']

def get_kn_entries(filte={}):
    tbl = get_keynote_tbl()
    return list(tbl.find(filte).sort('id',-1))

def start_step(func, filte):
    tbl = get_keynote_tbl()
    filte['selected'] = 1
    
    ls = list(tbl.find(filte))
    for i in ls:
        if not func(tbl, i):
            print('WARNING SKIPPING STEP!!')
        
    
    return True

def start():
    for _,i in folder.items():
        print ('Check if folder exists',i)
        if not os.path.exists(i):
            os.makedirs(i)
    for _,i in bucket.items():
        print ('Check if bucket exists',i)
        create_bucket(i)
            
def download(filename,url):
    if os.path.isfile(filename):
        print('WARNING: File already exists. Check if download was successful')
        return False
    print('Download',url,'to',filename)
    urllib.request.urlretrieve (url, filename)
    return True

def create_bucket(name):
    if storage_client() is None:
        print('Skip check, no connection to google cloud possible.')
        return
    if name not in [b.name for b in storage_client().list_buckets()]:
        # Creates the new bucket
        bucket = storage_client().create_bucket(name)
        print('Bucket {} created.'.format(bucket.name))
    else:
        print('Info: Bucket already exists')
        
def upload_file(file, filename, bucket_name, overwrite=False):
    bucket = storage_client().get_bucket(bucket_name)
    if overwrite or filename not in [b.name for b in bucket.list_blobs()]:
        blob = bucket.blob(filename)
        print('uploading',filename)
        blob.upload_from_filename(file)
        print('file uploaded',filename)
    else:
        assert input('Audiofile already exists, enter anything to abort')==''
        
def delete_file(filename, bucket_name):
    bucket = storage_client().get_bucket(bucket_name)
    if filename in [b.name for b in bucket.list_blobs()]:
        blob = bucket.blob(filename)
        blob.delete()
    else:
        assert False, ('Cant delete file that does not exist', filename, bucket_name)
        
def google_status(operation_url, save_path, checkuntildone=True):
    js = {}
    lastpct = -1
    while not 'done' in js or not js['done']:
        time.sleep(10)
        # Get token
        out = subprocess.Popen(['gcloud', 'auth', 'application-default', 'print-access-token'], 
               stdout=subprocess.PIPE, 
               stderr=subprocess.STDOUT)
        token,_ = out.communicate()
        token = token.decode('ascii').split('\n')[0]

        r = requests.get(operation_url,
        headers={'Authorization':'Bearer '+token},
        timeout=50)

        js = r.json()
        if 'progressPercent' in js['metadata'] and lastpct != js['metadata']['progressPercent']:
            lastpct = js['metadata']['progressPercent']
            print('progress (%)',lastpct)
        
        if not checkuntildone and (not 'done' in js or not js['done']):
            return False
        
        
    print('Operation finished')
    with open(save_path, 'w') as f:
        f.write(r.text)
    
    return True
        