from logger import LOG
import os
import sys
from enzyme.mkv import MKV, VIDEO_TRACK, AUDIO_TRACK, SUBTITLE_TRACK
import io
import threading
#import os.path
#import requests
#import unittest
#import zipfile
#from datetime import timedelta, datetime

class Mkv:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.dispose()

    disposed = False
    def dispose(self):
        if self.disposed:
            return
        self.disposed = True
        LOG.info('Shutting down Mkv...')
        self.run_stream_reader = False

    lock = threading.Lock()
            
    def __init__(self,
                 stream,
                 ):
        try:
            LOG.info('starting Mkv stream reader')
            from threading import Thread
            stream_reader_thread = Thread(target = self.stream_reader, args = (stream, ))
            self.run_stream_reader = True
            stream_reader_thread.start()
        except:
            LOG.exception(sys.exc_info()[0])
        finally:
            pass

    run_stream_reader = True
    def stream_reader(self,
                      stream
                      ):
        try:
            LOG.info('Mkv stream reader started')

            READ_BUFFER_SIZE = 50 * 10**3
            data = stream.read(amt=READ_BUFFER_SIZE)

##            f = open('test.mkv', 'wb')
##            f.write(data)
##            f.close()
##            print('Kinesis: ' + format(len(data)))
##            exit()

            with io.BytesIO(data) as s:
                import matroska
                matroska.dump_tags(s)
                print("@@@@@@@@@@@@@@@@@@@")
            
                mkv = MKV(s)

                print(mkv.info.title)
                print(mkv.info.duration)
                print(mkv.info.date_utc)
                print(mkv.info.muxing_app)
                print(mkv.info.writing_app)
                print(len(mkv.video_tracks))
                print(mkv.video_tracks[0].type)
                print(mkv.video_tracks[0].number)
                print(mkv.video_tracks[0].name)
                print(mkv.video_tracks[0].language)
                print(mkv.video_tracks[0].enabled)
                print(mkv.video_tracks[0].default)
                print(mkv.video_tracks[0].forced)
                print(len(mkv.subtitle_tracks))
                print(len(mkv.chapters))
                print(len(mkv.tags))
                print(len(mkv.tags[0].simpletags))
		
        except:
            LOG.exception(sys.exc_info()[0])
        finally:
            LOG.info('exiting Mkv stream reader')
            self.dispose()
