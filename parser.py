'''
by Sergey Stoyan: sergey.stoyan@gmail.com

AWS kinesis video stream consumer. 
Allows to extract video frames.
See the usage example in the end of parser.py

Based on PyAV (https://github.com/mikeboers/PyAV). 
'''

from logger import LOG
import settings
import ebml
import os
import sys
import boto3
import av
import time
import cv2 #can be replaced with Image lib
#import Image
import io
import numpy as np
import threading
#import copy
import traceback
from threading import Thread
import os.path

    
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
    '''ATTENTION: only methods and variables whose names start with capital letter are designed to be accessed from outside Parser.'''
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.Dispose()

    def __del__(self):
        self.Dispose()

    def Dispose(self):
        if self.disposing:
            return
        self.disposing = True

        with self.lock:    
            
            LOG.info('Shutting down Parser...\r\n%s' % '\r\n'.join(traceback.format_stack())) 
            
            self.run_kinesis_stream_reader = False
            self.run_libav_parser = False
            time.sleep(1)

            if self.kinesis_stream:
                try:
                    self.kinesis_stream.close()
                except:
                    LOG.exception(sys.exc_info()[0])
                self.kinesis_stream = None

            if self.libav_input_descriptor:
                try:
                    self.libav_input_descriptor.close()
                except:
                    LOG.exception(sys.exc_info()[0])
                self.libav_input_descriptor = None    
                
            self.libav_output_reader = None  
                        
            try:
                self.kinesis_stream_reader_thread.join(1)
                if self.kinesis_stream_reader_thread.isAlive():
                    LOG.error('kinesis_stream_reader_thread has not been stopped!')   
            except:
                pass

            try:
                self.libav_parser_thread.join(1)
                if self.libav_parser_thread.isAlive():
                    LOG.error('libav_parser_thread has not been stopped!')   
            except:
                pass         

            LOG.info("Parser has been disposed.") 
            
    def dispose(self):
        with self.lock:
            try:
                if self.dispose_thread and self.dispose_thread.is_alive() or self.disposing:
                    return
            except:
                pass
            self.dispose_thread = Thread(target = self.Dispose, args = ())
            self.dispose_thread.daemon = True
            self.dispose_thread.start() 
        
        
    def __init__(self,
        stream_name,
        time_span_between_frames_in_secs = -1,
        frame_queue_max_length = 10,
        save_frames2directory = './_frames',
        catch_frames = True,
        reconnect_max_count = 3,#ignored when refreshing connection due to AWS kinesis time limit
        ):
        try:
            LOG.info('Parser starting for %s' % stream_name)
            self.lock = threading.Lock()
            self.disposing = False
            self.stream_name = stream_name
            self.TimeSpanBetweenFramesInSecs = time_span_between_frames_in_secs
            self.FrameQueueMaxLength = frame_queue_max_length
            
            if save_frames2directory == True:
                save_frames2directory = './_frames'                
            self.frame_directory = save_frames2directory
            if self.frame_directory:
                if os.path.exists(self.frame_directory):
                    import shutil
                    shutil.rmtree(self.frame_directory)
                os.makedirs(self.frame_directory)  
            
            self.catch_frames = catch_frames  
            self.reconnect_max_count = reconnect_max_count

            self.next_frame_time = 0.0
            self.Frames = []      
            self.last_frame_id = 0
            self.tags_line = []
            self.last_packet_tags = None
            self.reconnect_count = 0
            self.refresh_count = 0
            self.kinesis_stream = None
            self.libav_input_descriptor = None
            self.kinesis_stream_reader_thread = None
            self.libav_parser_thread = None
            self.kinesis_stream_pipe = r'/tmp/AwsKinesisParserFifo'
            self.starter(False)
                
        except:
            LOG.exception(sys.exc_info()[0])
        finally:
            pass

    
    def starter(self,  
               count_attempt
    ):
        if self.disposing:
            return
        
        with self.lock:
            try:
                if self.starter_thread and self.starter_thread.is_alive():
                    return
            except:
                pass
        
            LOG.info('[Re-]starting connection to kinesis stream...\r\nreconnect_count=%d\r\nrefresh_count=%d\r\ncount_attempt=%s\r\n%s' % (self.reconnect_count, self.refresh_count, count_attempt, '\r\n'.join(traceback.format_stack())))
      
            self.starter_thread = Thread(target = self.starter_, args = (count_attempt,))
            self.starter_thread.daemon = True
            self.starter_thread.start()  

    def starter_(self,  
              count_attempt
    ):
        try:   
            if self.disposing:
                return                            

            #stop and clean everything before [re-]starting

            self.run_kinesis_stream_reader = False            
            if self.kinesis_stream:
                try:
                    self.kinesis_stream.close()
                except:
                    LOG.exception(sys.exc_info()[0])
                self.kinesis_stream = None

            if self.libav_input_descriptor:
                try:                    
                    #(it was checked:) after closing pipe input, libav will still read all the packets until EOF
                    LOG.info('Closing libav_input_descriptor...')
                    self.libav_input_descriptor.close()
                except:
                    LOG.exception(sys.exc_info()[0])
                self.libav_input_descriptor = None               
                
            if count_attempt:
                self.reconnect_count += 1
            else:
                self.refresh_count += 1
            if self.reconnect_count > self.reconnect_max_count:
                LOG.warning('Stopping because reconnect count exceeded %d...' % self.reconnect_max_count)
                return               
                        
            self.set_kinesis_stream()
                        
            if self.libav_parser_thread:
                LOG.info("Wating libav_parser_thread to read remaining packets and stop...") 
                self.libav_parser_thread.join()
                self.libav_parser_thread = None              
                LOG.info("libav_parser_thread has been stopped.")  
            self.libav_output_reader = None 

            if self.kinesis_stream_reader_thread:
                LOG.info("Wating kinesis_stream_reader_thread to stop...") 
                self.kinesis_stream_reader_thread.join()
                self.kinesis_stream_reader_thread = None              
                LOG.info("kinesis_stream_reader_thread has been stopped.") 
            
            self.tags_line = []

            if os.path.exists(self.kinesis_stream_pipe):
                os.remove(self.kinesis_stream_pipe)#clean the pipe if it remains with data after Parser interruption
            os.mkfifo(self.kinesis_stream_pipe) 
                            
            self.run_kinesis_stream_reader = True
            self.kinesis_stream_reader_thread = Thread(target = self.kinesis_stream_reader, args = ())
            self.kinesis_stream_reader_thread.daemon = True
            self.kinesis_stream_reader_thread.start()  

            self.run_libav_parser = True
            self.libav_parser_thread = Thread(target = self.libav_parser, args = ())
            self.libav_parser_thread.daemon = True
            self.libav_parser_thread.start() 

        except:
            LOG.exception(sys.exc_info()[0])
            self.dispose()

        finally:            
            pass


    def set_kinesis_stream(self):
        LOG.info('Requesting kinesis get_data_endpoint...')
        client = boto3.client(
            service_name = 'kinesisvideo',
            region_name = settings.REGION_NAME
        )
        response = client.get_data_endpoint(
            StreamName = self.stream_name,
            APIName = 'GET_MEDIA'
        )
        LOG.info('Response: ' + format(response))
        endpoint_url = response['DataEndpoint']
        
        LOG.info('Requesting kinesis get_media...')
        client = boto3.client(
            service_name = 'kinesis-video-media',
            endpoint_url = endpoint_url,
            region_name = settings.REGION_NAME,
        )
        
        with self.lock:
            if self.last_packet_tags:
                startSelector = {
                    'StartSelectorType': 'CONTINUATION_TOKEN',
                    'ContinuationToken': self.last_packet_tags.AWS_KINESISVIDEO_CONTINUATION_TOKEN,
                }
                LOG.info('Continue reading since: %s' % startSelector['ContinuationToken'])
            else:
                startSelector = {
                    'StartSelectorType': 'NOW',
                }
                LOG.info('Start reading since NOW')

        response = client.get_media(
            StreamName = self.stream_name,
            StartSelector = startSelector,
        )
        LOG.info('Response: ' + format(response))
        self.kinesis_stream = response['Payload']

    
    def kinesis_stream_reader(self,
    ):
        try: 
            LOG.info('kinesis_stream_reader started') 
            
            if not self.run_kinesis_stream_reader: 
                return
            
            interestingElementNames = [
                 'Segment', 
                # 'Cluster',
                 'TagName',
                 'TagString',
                'DocTypeReadVersion'#last tag in segment
            ]
            #interestingElementNames = None
            ebmlReader = ebml.EbmlReader(self.kinesis_stream, interestingElementNames)#, self.print_ebml_element_head)

            self.libav_input_descriptor = open(self.kinesis_stream_pipe, 'w') 

            tags = Tags()
            ebmlReader.CopyBuffer = io.BytesIO()
            lastTagName = None 
            while self.run_kinesis_stream_reader:    
                size, id, name, type_, value = ebmlReader.ReadNextElement()
                #self.print_ebml_element(size, id, name, type_, value)

                #TEST
                #if ebmlReader.Position > 30000000:
                #    f = self.GetLastFrame()
                #    LOG.info('>>>>>>>>>>>>>>>>>>>>>>>>>>>Last frame: %s' % f)
                #    raise Exception("restart test")

                if name == 'Segment': 
                    pass

                elif name == 'TagName':
                    lastTagName = value
                elif name == 'TagString':
                    if hasattr(tags, lastTagName):
                        setattr(tags, lastTagName, value)
                    elif lastTagName == 'AWS_KINESISVIDEO_ERROR_CODE':
                        LOG.error('AWS_KINESISVIDEO_ERROR_CODE: %s' % value)
                    elif lastTagName == 'AWS_KINESISVIDEO_ERROR_ID':
                        LOG.error('AWS_KINESISVIDEO_ERROR_ID: %s' % value)
                        
                elif name == 'DocTypeReadVersion': 
                    tags.position = ebmlReader.Position                    
                    with self.lock:
                        self.tags_line.append(tags)
                    tags = Tags()

                    bs = ebmlReader.CopyBuffer.getvalue()
                    #LOG.info('=====================CopyBuffer: %d, %d'%(len(bs), ebmlReader.Position))
                    self.libav_input_descriptor.write(bs)
                    self.libav_input_descriptor.flush()
                    #os.write(self.libav_input_descriptor, bs)
                    #os.flush(self.libav_input_descriptor)
                    ebmlReader.CopyBuffer.close()
                    ebmlReader.CopyBuffer = io.BytesIO()  

        except:      
            LOG.exception(sys.exc_info()[0])

        finally:
            LOG.info('kinesis_stream_reader exiting...:\r\nrun_kinesis_stream_reader=%s' % (self.run_kinesis_stream_reader))
            try:
                if ebmlReader and ebmlReader.Position > 1000000:
                    self.starter(False)
                    return
            except:
                pass
            self.starter(True)
        


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
    ):  
        try:
            LOG.info('libav_parser started')

            if not self.run_libav_parser:
                return

            self.libav_output_reader = av.open(self.kinesis_stream_pipe)
            LOG.info('kinesis_stream_pipe opened for reading')
            for packet in self.libav_output_reader.demux(video=0):
                if not self.run_libav_parser:
                    return

                with self.lock:
                    tags_i = -1
                    for i, t in enumerate(self.tags_line):
                        if t.position > packet.pos:    
                            tags_i = i
                            break
                    #print('len(self.tags_line):%d'%len(self.tags_line))
                    if tags_i < 0:
                        raise Exception('No tag for packet!')
                    else:
                        self.last_packet_tags = self.tags_line[tags_i]
                        del self.tags_line[ : tags_i]

                if not self.catch_frames:  
                    continue 
                
                for frame in packet.decode():
                    if not self.run_libav_parser:
                        break
                    self.last_frame_id += 1
                    #self.catch_frame(self.last_packet_tags, frame.to_image(), self.last_frame_id) #about 70/100 slower
                    self.catch_frame(self.last_packet_tags, frame.to_nd_array(format='bgr24'), self.last_frame_id) 
                
        except:
            LOG.exception(sys.exc_info()[0])

        finally:
            LOG.info('libav_parser exiting...:\r\nrun_libav_parser=%s' % (self.run_libav_parser, ))
            self.starter(True)

            
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
                try:# file might be in use or deleted
                    os.remove(i['file'])
                except:
                    pass
                del self.Frames[0]
                
            if self.frame_directory:
                #image.save(frame_file)                
                cv2.imwrite(frame_file, image)
                pass

                            
    def GetFrame(self,
                 index
                 ):
        '''This method is thread safe and can be used to access self.Frames anytime.'''
        with self.lock:
            try:
                l = len(self.Frames)
                if index < 0 or index >= l:
                    return None
                return self.Frames[index]
            except:
                LOG.exception(sys.exc_info()[0])
          
    def GetLastFrame(self
                 ):
        '''This method is thread safe and can be used to access self.Frames anytime.'''
        with self.lock:
            try:
                index = len(self.Frames) - 1
                if index < 0:
                    return None
                return self.Frames[index]
            except:
                LOG.exception(sys.exc_info()[0])

    def StartCatchFrames(self,
                ):
        with self.lock:
            self.catch_frames = True
               
    def StopCatchFrames(self,
               ):
        '''ATTENTION: before directly accessing Parser::Frames, this method must be called to avoid concurrency.'''
        with self.lock:
            self.catch_frames = False

    
#USAGE example
if __name__ == '__main__':#not to run when this module is imported
        
    import signal
    def signal_handler(sig, frame):
        print('Pressed Ctrl+C')
        signal.alarm(3) # produce SIGALRM in ... seconds
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)
    
    if len(sys.argv) > 1:
        stream_name = sys.argv[1]
    else:
        stream_name = 'test11'

    with Parser(
        stream_name = stream_name,
        time_span_between_frames_in_secs = -0.3,
        frame_queue_max_length = 20,
        save_frames2directory = False,
        catch_frames = True,
        ) as p:
                
        #time.sleep(1)
        #f = p.GetFrame(0)#thread safe method
        #print('First frame: %s' % f)

        time.sleep(3)
        f = p.GetLastFrame()
        print('Last frame: %s' % f)

        #p.StopCatchFrames()        #!!!p.Frames should be accessed only after StopCatchFrames() to avoid concurrency!!!
        #for f in p.Frames:
        #    print('Frame: %s' % f)
                 
        import datetime   
        last_time = datetime.datetime.now()
        if f:
            lastId = f.Id
        else:
            lastId = 0
        p.StartCatchFrames()
        for i in range(0, 360):
            time.sleep(10)
            f = p.GetLastFrame()
            if f:
                LOG.info('Caught frames for %f secs: %d' % ((datetime.datetime.now() - last_time).total_seconds(), f.Id - lastId))
                last_time = datetime.datetime.now()
                lastId = f.Id
                LOG.info('Last frame: %s' % f)
            else:
                LOG.info('No frame')
    exit()
