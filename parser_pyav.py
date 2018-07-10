#by Sergey Stoyan: sergey.stoyan@gmail.com
from logger import LOG
import settings
import os
import sys
#import imp
import boto3
#import json
#from datetime import datetime
import time
import cv2#, platform
import io
import numpy as np
#import Image
import subprocess as sp
import signal
import threading
import ebml
import copy

    
class Tags:
    def __init__(self):
        self.AWS_KINESISVIDEO_FRAGMENT_NUMBER = None
        self.AWS_KINESISVIDEO_SERVER_TIMESTAMP = None
        self.AWS_KINESISVIDEO_PRODUCER_TIMESTAMP = None
        self.AWS_KINESISVIDEO_MILLIS_BEHIND_NOW = None
        self.AWS_KINESISVIDEO_CONTINUATION_TOKEN = None
        self.position = -1    

    def __str__(self):
        from pprint import pprint, pformat       
        return pformat(vars(self))

class Frame:
    def __init__(self,
                 image,
                 id,
                 tags,
                 file,
                 ):
        self.Image = image
        self.Id = id
        self.Tags = tags
        self.File = file
        self.Time = time.time()

    def __str__(self):
        return 'Id:%d\r\nTags:%s\r\nFile:%s\r\nTime:%d'%(self.Id, self.Tags, self.File, self.Time)

class Parser:
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.dispose()

    def __del__(self):
        self.dispose()
        
    #class Dispose(Exception):
    #    pass

    #def dispose_handler(signum, frame):
    #    raise Dispose

    def dispose(self):
        if self.disposed:
            return
        with self.lock:
            self.disposed = True
            
            LOG.info('Shutting down Parser...') 
            
            #signal.signal(signal.SIGALRM, self.dispose_handler)
            signal.alarm(3) # produce SIGALRM in ... seconds
            
            self.run_kinesis_stream_reader = False
            try:
                self.kinesis_stream.close()
            except:
                LOG.exception(sys.exc_info()[0])

            try:
            #if self.kinesis_stream_pipe_W:
                self.kinesis_stream_pipe_W.close()
            except:
                LOG.exception(sys.exc_info()[0])

            self.run_libav_parser = False
            try:
                #if self.kinesis_stream_pipe_R:
                    #self.kinesis_stream_pipe_R.close()
                pass
            except:
                LOG.exception(sys.exc_info()[0])

            exit()
            
        
    def __init__(self,
        stream_name,
        time_span_between_frames_in_secs = -1,
        frame_queue_max_length = 10,
        save_frames2directory = './_frames',
        catch_frames = True,
        ):
        try:
            LOG.info('STARTED')
            LOG.info('stream_name: ' + stream_name)
            self.lock = threading.Lock()
            self.disposed = False
            self.FrameQueueMaxLength = 10
            self.TimeSpanBetweenFramesInSecs = -1
            self.run_kinesis_stream_reader = True
            self.ffmpeg_process = None
            self.run_frame_parser = True
            self.Frames = []
            self.catch_frames = catch_frames  
            self.FrameQueueMaxLength = frame_queue_max_length
            self.TimeSpanBetweenFramesInSecs = time_span_between_frames_in_secs
            self.next_frame_time = time.time()
            
            if save_frames2directory == True:
                save_frames2directory = './_frames'                
            self.frame_directory = save_frames2directory
            if self.frame_directory:
                if os.path.exists(self.frame_directory):
                    import shutil
                    shutil.rmtree(self.frame_directory)
                os.makedirs(self.frame_directory)            
        
        # client = boto3.client(
               # service_name = 'kinesisvideo',
               # region_name = settings.REGION_NAME
        # )
        # LOG.info(
               # client.list_streams(
               # )
        # )

            LOG.info('requesting kinesis get_data_endpoint')
            client = boto3.client(
                service_name = 'kinesisvideo',
                region_name = settings.REGION_NAME
            )
            response = client.get_data_endpoint(
                StreamName = stream_name,
                APIName = 'GET_MEDIA'
            )
            LOG.info('response: ' + format(response))
            endpoint_url = response['DataEndpoint']
        
            LOG.info('requesting kinesis get_media')
            client = boto3.client(
                service_name = 'kinesis-video-media',
                endpoint_url = endpoint_url,
                region_name = settings.REGION_NAME,
            )
            response = client.get_media(
                StreamName = stream_name,
                StartSelector = {
                    'StartSelectorType': 'NOW',
                }
            )
            LOG.info('response: ' + format(response))
            self.kinesis_stream = response['Payload']
                        
            kinesis_stream_pipe = r'/tmp/testFifo'
            try:                                
                os.remove(kinesis_stream_pipe)
            except:
                pass
            os.mkfifo(kinesis_stream_pipe)
            
            self.next_frame_time = 0.0

            LOG.info('starting kinesis_stream_reader')
            from threading import Thread
            kinesis_stream_reader_thread = Thread(target = self.kinesis_stream_reader, args = (self.kinesis_stream, kinesis_stream_pipe,))
            self.run_kinesis_stream_reader = True
            kinesis_stream_reader_thread.start()  

            LOG.info('starting libav_parser')
            from threading import Thread
            libav_parser_thread = Thread(target = self.libav_parser, args = (kinesis_stream_pipe,))
            self.run_libav_parser = True
            libav_parser_thread.start()  
                
        except:
            LOG.exception(sys.exc_info()[0])
        finally:
            pass
            
    def kinesis_stream_reader(self,
        kinesis_stream,
        kinesis_stream_pipe
    ):
        try:  
            LOG.info('started kinesis_stream_reader')

            self.kinesis_stream_pipe_W = open(kinesis_stream_pipe, 'w')

            interestingElementNames = [
                 'Segment', 
                # 'Cluster',
                # 'Tags',
                # 'Tag',
                # 'SimpleTag',
                 'TagName',
                 'TagString',
                # 'CodecID',
                # 'CodecName',
                # 'PixelWidth',
                # 'PixelHeight',
                # 'Video',
                # 'TrackEntry',
                # 'Tracks',
    # 'Timecode',
    # 'Position',
    # 'PrevSize',
    #'CodecPrivate',
    #'BlockGroup', 
    #'Block', 
    #'BlockVirtual',
    #'SimpleBlock', 
    #'BlockDuration',
    #'Slices', 
    #'BlockAdditions',
    'DocTypeReadVersion'#last tag in segment
            ]
            #interestingElementNames = None
            er = ebml.EbmlReader(kinesis_stream, interestingElementNames)#, self.print_ebml_element_head)
                        
            self.tagLine = []
            tags = Tags()            
            er.CopyBuffer = io.BytesIO()
            lastTagName = None 
            while self.run_kinesis_stream_reader:    
                size, id, name, type_, value = er.ReadNextElement()
                #self.print_ebml_element(size, id, name, type_, value)
                if name == 'Segment': 
                    pass
                elif name == 'TagName':
                    lastTagName = value
                elif name == 'TagString' and hasattr(tags, lastTagName):
                    setattr(tags, lastTagName, value)
                elif name == 'DocTypeReadVersion': 
                    tags.position = er.position                    
                    with self.lock:
                        self.tagLine.append(tags)
                    tags = Tags()

                    bs = er.CopyBuffer.getvalue()
                    LOG.info('=====================CopyBuffer: %d, %d'%(len(bs), er.position))
                    self.kinesis_stream_pipe_W.write(bs)
                    self.kinesis_stream_pipe_W.flush()
                    er.CopyBuffer.close()
                    er.CopyBuffer = io.BytesIO()  

        except:
            LOG.exception(sys.exc_info()[0])
        finally:
            LOG.info('end of kinesis_stream_reader')
            self.dispose()

    def print_ebml_element_head(self, ebmlParser, size,  id, name, type_):
        LOG.info('position: %d, size:%d, id:%s, name:%s, type_:%s' % (ebmlParser.position, size, hex(id), name, type_))
            
    def print_ebml_element(self, size, id, name, type_, value):
        #if type_ != ebml.BINARY:
        #    LOG.info('value: %s' % (value))
        #else:
        #    LOG.info('value: %s' % ('<BINARY>')) 
        if type_ != ebml.BINARY:
            LOG.info('name:%s, size:%d, id:%s, type_:%s, value: %s' % (name, size, hex(id), type_, value))
        else:
            LOG.info('name:%s, size:%d, id:%s, type_:%s, value: %s' % (name, size, hex(id), type_, '<BINARY>'))             

            
    def libav_parser(self,
        kinesis_stream_pipe
    ):  
        try:
            LOG.info('started libav_parser')
            #packet_count = 0
            import av
            self.kinesis_stream_pipe_R = av.open(kinesis_stream_pipe)
            #print(self.kinesis_stream_pipe_R.dumps_format())  
            for packet in self.kinesis_stream_pipe_R.demux(video=0):
                if not self.run_libav_parser:
                    break
                #print('pos:%d'%packet.pos)
                with self.lock:
                    tags_i = -1
                    for i, t in enumerate(self.tagLine):
                        if t.position > packet.pos:    
                            tags_i = i
                            break
                    #print('len(self.tagLine):%d'%len(self.tagLine))
                    if tags_i < 0:
                        LOG.error('No tag for packet!')
                    else:
                        tags = self.tagLine[tags_i]
                        del self.tagLine[ : tags_i]
                #packet_count += 1
                #print('i:%d'%packet_count)#about 850 packet for 35 secs if no further processing
                if not self.catch_frames:  
                    continue            
                for frame in packet.decode(): 
                    #print('i:%d'%frame.index)#about 180 frames for 35 secs if no fursther processing
                    #self.catch_frame(tags, frame.to_image(), frame.index) #slow: processes about 70 frames for 35secs!!!
                    self.catch_frame(tags, frame.to_nd_array(format='bgr24'), frame.index) #a bit faster (about 100 frames for 35secs) but it slows down 2 times against that when it is absent 
                
        except:
            LOG.exception(sys.exc_info()[0])
        finally:
            LOG.info('end of libav_parser')
            self.dispose()

            
    def catch_frame(self,
        tags,
        image,
        frame_id
    ):          
        if not self.catch_frames:  
            return
            
        if self.TimeSpanBetweenFramesInSecs > 0:
            if self.next_frame_time > time.time():
                return
            self.next_frame_time = time.time() + self.TimeSpanBetweenFramesInSecs

        if self.frame_directory:
            frame_file = self.frame_directory + "/frame%d.png" % frame_id
        else:
            frame_file = None               

        with self.lock:
            f = Frame(image, frame_id, tags, frame_file)             
            #LOG.info(f)             
            self.Frames.append(f)

            if len(self.Frames) > self.FrameQueueMaxLength:
                i = self.Frames[0]
                try:# file can be in use or deleted
                    os.remove(i['file'])
                except:
                    pass
                del self.Frames[0]
                
            if self.frame_directory:
                #image.save(frame_file)                
                cv2.imwrite(frame_file, image)#slow!!!
                pass

                            
    def GetFrame(self,
                 index = -1  #last one
                 ):
        with self.lock:
            try:
                l = len(self.Frames)
                if index >= l or l < 1:
                    return
                if index < 0:
                    index = l - 1
                    return self.Frames[index]
                return None
            except:
                LOG.exception(sys.exc_info()[0])


    def StartCatchFrames(self,
                ):
        with self.lock:
            self.catch_frames = True
               
    def StopCatchFrames(self,
               ):
        with self.lock:
            self.catch_frames = False

    

if __name__ == '__main__':#not to run when this module is imported
    import sys
    if len(sys.argv) > 1:
        stream_name = sys.argv[1]
    with Parser(
        stream_name = 'test8',
        time_span_between_frames_in_secs = -0.3,
        frame_queue_max_length = 20,
        save_frames2directory = True,
        catch_frames = True,
        ) as p:
                
        time.sleep(3)
        f = p.GetFrame(0)#thread safe method
        print("First frame: %s" % f)

        time.sleep(3)
        f = p.GetFrame()#last frame
        print("Last frame: %s" % f)

        p.StopCatchFrames()        #p.Frames must be accessed only after StopCatchFrames() to avoid concurrency!!!
        for f in p.Frames:
            print("Frame: %s" % f)
            
        p.StartCatchFrames()
        time.sleep(30)
        f = p.GetFrame()#last frame
        print("Last frame: %s" % f)
    exit()
