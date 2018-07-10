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

class Parser:
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.dispose()

    def __del__(self):
        self.dispose()

    def dispose(self):
        if self.disposed:
            return
        with self.lock:
            self.disposed = True
            
            LOG.info('Shutting down Parser...') 
            
            self.run_kinesis_stream_reader = False

            if self.ffmpeg_process:
                #self.ffmpeg_process.stdin.close()
                self.ffmpeg_process.kill()
                self.ffmpeg_process.terminate()
                os.killpg(os.getpgid(self.ffmpeg_process.pid), signal.SIGTERM)
            
        
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
            kinesis_stream = response['Payload']

            LOG.info('starting kinesis_stream_reader')
            from threading import Thread
            kinesis_stream_reader_thread = Thread(target = self.kinesis_stream_reader, args = (kinesis_stream, ))
            self.run_kinesis_stream_reader = True
            kinesis_stream_reader_thread.start()  
                
        except:
            LOG.exception(sys.exc_info()[0])
        finally:
            pass
    
    def kinesis_stream_reader(self,
        kinesis_stream
    ):
        try:  
            interestingElementNames = [
                 'Segment', 
                # 'Cluster',
                # 'Tags',
                # 'Tag',
                # 'SimpleTag',
                 'TagName',
                 'TagString',
                 'CodecID',
                # 'CodecName',
                # 'PixelWidth',
                # 'PixelHeight',
                # 'Video',
                # 'TrackEntry',
                # 'Tracks',
     'Timecode',
    # 'Position',
    # 'PrevSize',
    'CodecPrivate',
    #'BlockGroup', 
    #'Block', 
    #'BlockVirtual',
    'SimpleBlock', 
    #'BlockDuration',
    #'Slices', 
    #'BlockAdditions',
    'DocTypeReadVersion'
            ]
            #interestingElementNames = None
            er = ebml.EbmlReader(kinesis_stream, interestingElementNames)#, self.print_ebml_element_head)
                        
            self.frame_count = 0
            self.next_frame_time = 0.0
            
            tagNames2string = {
                'AWS_KINESISVIDEO_FRAGMENT_NUMBER':'',
                'AWS_KINESISVIDEO_SERVER_TIMESTAMP':'',
                'AWS_KINESISVIDEO_PRODUCER_TIMESTAMP':'',
                'AWS_KINESISVIDEO_MILLIS_BEHIND_NOW':'',
                'AWS_KINESISVIDEO_CONTINUATION_TOKEN':'',
            }
            
            self.ffmpeg_process = self.create_ffmpeg_process()
            
            er.CopyBuffer = io.BytesIO()
            lastTagName = None
            currentTags = None 
            FRAME_SIZE = 1920 * 1080 * 3   
            while self.run_kinesis_stream_reader:    
                #LOG.info('@@@@@@@@@@@@@@@@@@@ %d'%i)
                while currentTags:#it is not the first segment  
                    try:
                        #print('#########')
                        self.ffmpeg_process.stdout.flush() 
                        frame = self.ffmpeg_process.stdout.read(FRAME_SIZE)
                        #LOG.info('frame length:%d'%len(frame))
                    except IOError:# the os throws an exception if there is no data
                        #LOG.exception('')
                        LOG.info('!no frame from ffmpeg')
                        break                                                          
                    self.catch_frame(currentTags, frame, 1920, 1080)   
                    
                size, id, name, type_, value = er.ReadNextElement()
                self.print_ebml_element(size, id, name, type_, value)
                if name == 'DocTypeReadVersion': #the last element before next segment
                    bs = er.CopyBuffer.getvalue()
                    LOG.info('=====================CopyBuffer: %d, %d'%(len(bs), er.position))
                    self.ffmpeg_process.stdin.write(bs)
                    self.ffmpeg_process.stdin.flush()
                    print('position:%d'%er.position)
                    er.position = 0
                    er.CopyBuffer.close()
                    er.CopyBuffer = io.BytesIO()
                    
                    currentTags = copy.deepcopy(tagNames2string)  
                elif name == 'TagName':
                    lastTagName = value
                elif name == 'TagString' and lastTagName in tagNames2string:
                    tagNames2string[lastTagName] = value       
                #if self.catch_frames:
                    #tags = copy.deepcopy(tagNames2string)
                    #self.catch_frame(tags, frame)
        except:
            LOG.exception('')
        finally:
            LOG.info('exiting kinesis_stream_reader')
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

            
    def create_ffmpeg_process(self):  
        LOG.info('starting ffmpeg process')

                        #getting frame size
##                        cmd = [
##                                'ffprobe',
##                                '-i', 'pipe:0',
##                                '-v', 'error,
##                                '-show_entries', 'stream=width,height',
##                                '-f', 'image2pipe', '-',      # tell FFMPEG that it is being used with a pipe by another program
##                        ]
##                        LOG.info('starting ffprobe')
##                        ffprobe_process = sp.Popen(cmd,
##                                                       stdin=sp.PIPE,
##                                                       stdout=sp.PIPE,
##                                                       #stderr=sp.PIPE,
##                                                       bufsize=10**8,
##                                                       preexec_fn=os.setsid
##                                                )
##                        #self.ffmpeg_process.communicate()    
##                        
##                        READ_BUFFER_SIZE = 1000000
##                        total_bytes = 0
##                        data = kinesis_stream.read(amt=READ_BUFFER_SIZE)
##                        while data:
##                                total_bytes += len(data)
##                                print('Kinesis: ' + format(total_bytes))
##                                self.ffprobe_process.stdin.write(data)
##                                self.ffprobe_process.stdin.flush()
##                                
##                                data = kinesis_stream.read(amt=READ_BUFFER_SIZE)
##
##                                if not self.run_kinesis_stream_reader:
##                                        LOG.info('NOT self.run_kinesis_stream_reader')
##                                        break                     

        cmd = [
            'ffmpeg',
            '-i', 'pipe:0',
            #'-i -',
            '-pix_fmt', 'bgr24',      # opencv requires bgr24 pixel format.
            '-vcodec', 'rawvideo',
            '-an', '-sn',              # we want to disable audio processing (there is no audio)
            '-f', 'image2pipe', '-',      # tell FFMPEG that it is being used with a pipe by another program
            #'pipe:1'
            #"-ss", "0"
            #"-vframes", "1" #only once
            #"-vf fps", "1" #every 1 sec
            #"-v", "warning",
            #"-strict", "experimental",
            #"-vf", "{0}, {1}".format(box, draw_text),
            #"-y",
            #"-f", "mp4",
            #"-movflags", "frag_keyframe",
            #"output.png"
            #'http://localhost:8090/cam2.ffm'
        ]
        p = sp.Popen(cmd,
            stdin=sp.PIPE,
            stdout=sp.PIPE,
            #stderr=sp.PIPE,
            bufsize=10**7,
            preexec_fn=os.setsid
        ) 
        #set the O_NONBLOCK flag of p.stdout file descriptor:
        import fcntl        
        flags = fcntl.fcntl(p.stdout, fcntl.F_GETFL) # get current p.stdout flags
        fcntl.fcntl(p.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        return p 
            
    def catch_frame(self,
        tags,
        frame,
        width,
        height
    ):       
        self.frame_count += 1
        
        if not self.catch_frames:  
            return
            
        if self.TimeSpanBetweenFramesInSecs > 0:
            if self.next_frame_time > time.time():
                return
            self.next_frame_time = time.time() + self.TimeSpanBetweenFramesInSecs

        from pprint import pprint, pformat
        s = 'frame%d\r\ntags:\r\n%s'%(self.frame_count, pformat(tags))
        if self.frame_directory:
            frame_file = self.frame_directory + "/frame%d.png" % self.frame_count
            s = '%s\r\nfile:%s'%(s, frame_file)
        LOG.info(s)
        return
        image = np.fromstring(frame, np.uint8)
        image = image.reshape((height, width, 3))
        #image = image.reshape((1080,1920,3))
        #image2 = cv2.imdecode(image, cv2.CV_LOAD_IMAGE_COLOR)                                

        with self.lock:
            self.Frames.append({'image':image, 'time':time.time(), 'tags':tags, 'file':frame_file})

            if len(self.Frames) > self.FrameQueueMaxLength:
                i = self.Frames[0]
                try:# file can be in use or deleted
                    os.remove(i['file'])
                except:
                    pass
                del self.Frames[0]
                
            if self.frame_directory:
                cv2.imwrite(frame_file, image)
                print(frame_file)
                #image.save("frame_%d.jpg" % self.frame_count, image)   

                            
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
            except:
                LOG.exception(sys.exc_info()[0])

                

    def StartCatchFrames(self,
                ):
        self.catch_frames = True
               
    def StopCatchFrames(self,
               ):
        self.catch_frames = False

    

if __name__ == '__main__':#not to run when this module is being imported
    import sys
    if len(sys.argv) > 1:
        stream_name = sys.argv[1]
    with Parser(
        stream_name = 'test8',
        time_span_between_frames_in_secs = -0.3,
        frame_queue_max_length = 20,
        save_frames2directory = './_frames',
        catch_frames = True,
        ) as p:
                
        time.sleep(3)
        f = p.GetFrame(0)#thread safe method
        if f is not None:
            print("First frame: (%d), %s\r\n" % (f['time'], f['file']))

        time.sleep(3)
        f = p.GetFrame()#last frame
        if f is not None:
            print("Last frame: (%d), %s\r\n" % (f['time'], f['file']))

        #p.StopCatchFrames()        #p.Frames must be accessed only after StopCatchFrames() to avoid concurrency!!!
        #for f in p.Frames:
        #    print("Frame: (%d), %s\r\n" % (f['time'], f['file']))                
            
        #p.StartCatchFrames()
        time.sleep(3)
        f = p.GetFrame()#last frame
        if f is not None:
            print("Last frame: (%d), %s\r\n" % (f['time'], f['file']))
    exit()
