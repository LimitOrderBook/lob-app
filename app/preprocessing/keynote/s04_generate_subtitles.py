import preprocessing.preglobal as pg
from google.cloud import speech_v1p1beta1 as speech
import os
import subprocess

def start():
    return pg.start_step(start_one, {'downloaded':1,'subtitles':None,'subtitle_generated':None, 'subtitle_downloaded':None})


def extract_audio(file, out):
    # alternative configuration - if config is not working!
    # command = 'ffmpeg -i "'+file+'" -ab 160k -ac 2 -ar 16000 -vn "'+out+'"'
    command = 'ffmpeg -i "'+file+'" -acodec flac -vn "'+out+'"'
    # Use ffprobe to get code information
    # Use this command to extract 50 seconds: ffmpeg -i Sept_2017_HD_720.flac -ss 900 -t 50 1min001.flac
    return subprocess.check_output(command, shell=True)


alwaysimport = False
def start_one(tbl,i):
    global alwaysimport
    # Step 1: Extract audio from HD video
    
    filename = pg.folder['video_hd']+'/'+i['id']+'.mp4'
    audiofile = pg.folder['audio']+'/'+i['id']+'.flac'
    bucket_name = pg.bucket['audio']
    
    if not os.path.isfile(audiofile):
        extract_audio(filename, audiofile)
    else:
        print('WARNING audio file already exists', audiofile)
        
    # Step 2: Upload audiofile to google cloud  
    pg.upload_file(audiofile, i['id']+'.flac', bucket_name)
    
    # Delete local audio file
    os.remove(audiofile)
    
    # Step 3: Run Google Speech Recognition
    client = speech.SpeechClient()
    audiofile = i['id']+'.flac'

    audio = speech.types.RecognitionAudio(uri='gs://'+bucket_name+'/'+audiofile)

    # alternative configuration - if config is not working!
    #config = speech.types.RecognitionConfig(
    #    encoding=speech.enums.RecognitionConfig.AudioEncoding.FLAC,
    #    sample_rate_hertz=16000,
    #    enable_separate_recognition_per_channel=False,
    #    max_alternatives=10,
    #    audio_channel_count=2,
    #    use_enhanced=True,
    #    language_code='en-US',
    #    enable_word_confidence=True,
    #    enable_word_time_offsets=True,
    #    model='video',
    #    enable_automatic_punctuation=True)
        
    config = speech.types.RecognitionConfig(
        encoding=speech.enums.RecognitionConfig.AudioEncoding.FLAC,
        sample_rate_hertz=48000,
        enable_separate_recognition_per_channel=True,
        max_alternatives=10,
        audio_channel_count=2,
        use_enhanced=True,
        language_code='en-US',
        enable_word_confidence=True,
        enable_word_time_offsets=True,
        model='video',
        enable_automatic_punctuation=True)

    assert alwaysimport or input(['confirm speech recognition of',audiofile]) == '' 
    alwaysimport = True
    
    operation = client.long_running_recognize(config, audio)
    print(audiofile, operation.operation)

    tbl.update_one({ '_id': i['_id'] },{ '$set': { 'subtitle_generated':1,'subtitle_operation': operation.operation.name } }, upsert=False)
    
    return True
    
    