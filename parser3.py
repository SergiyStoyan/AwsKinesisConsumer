#by Sergey Stoyan: sergey.stoyan@gmail.com
from logger import LOG
import _settings as settings
import os
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

class Parser:
        
        def __enter__(self):
                return self

        def __exit__(self, exc_type, exc_value, traceback):
                LOG.info('Shutting down Parser...')
                
                self.run_kinesis_stream_reader = False
                
                self.run_frame_parser = False

        lock = threading.Lock()

        FrameQueueMaxLength = 10
        TimeSpanBetweenFramesInSecs = -1
        SaveFrames2Disk = True
            
	def __init__(self,
                     stream_name,
                     time_span_between_frames_in_secs = -1,
                     frame_queue_max_length = 10,
                     save_frames2disk = True
                     ):		                             
		try:
                        LOG.info('STARTED')
                        LOG.info('stream_name: ' + stream_name)
                        
                        self.FrameQueueMaxLength = frame_queue_max_length
                        self.TimeSpanBetweenFramesInSecs = time_span_between_frames_in_secs
                        self.SaveFrames2Disk = save_frames2disk
                        
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

        
        run_kinesis_stream_reader = True
        ffmpeg_process = None
        def kinesis_stream_reader(self,
                                kinesis_stream
                                ):              
		try:    
                        LOG.info('kinesis_stream_reader started')

                        pipe = r'/tmp/testFifo'
                        print pipe
                        try:                                
                                os.remove(pipe)
                        except:
                                pass
                        try:
                                os.mkfifo(pipe)
                        except OSError, e:
                                print "Failed to create FIFO: %s" % e
                                return

                        
                        LOG.info('starting frame_parser')
                        from threading import Thread
                        frame_parser_thread = Thread(target = self.frame_parser, args = (pipe, ))                
                        self.run_frame_parser = True
                        frame_parser_thread.start()
                        
                        #time.sleep(3)
                        print('opening pipe')
                        #fd = os.open(pipe, os.O_NONBLOCK | os.O_WRONLY)
                        #fifo = os.fdopen(fd, 'w')
                        fifo = open(pipe, 'w')
                        if fifo is None:
                                print('pipe NOT opened')
                                return
                        print('pipe opened')               
                        #time.sleep(10)
                        
                        READ_BUFFER_SIZE = 100000
                        total_bytes = 0
                        data = kinesis_stream.read(amt=READ_BUFFER_SIZE)
                        while data:
                                total_bytes += len(data)
                                print('Kinesis: ' + format(total_bytes))
                                fifo.write(data)
                                fifo.flush()
                                #time.sleep(5)
                                data = kinesis_stream.read(amt=READ_BUFFER_SIZE)

                                if not self.run_kinesis_stream_reader:
                                        LOG.info('NOT self.run_kinesis_stream_reader')
                                        break                     
		except:
			LOG.exception(sys.exc_info()[0])
		finally:
                        LOG.info('exiting kinesis_stream_reader')
                        run_frame_parser = False
                        if fifo is not None:
                                fifo.close()




        run_frame_parser = True                
        def frame_parser(self,
                      pipe
                      ):                
		try:
                        LOG.info('frame_parser started')
                        
                        frame_dir = '_frames'
                        if os.path.exists(frame_dir):
                                import shutil
                                shutil.rmtree(frame_dir) 
                        os.makedirs(frame_dir)

                        #s = self.ffmpeg_process.stderr.read(100)
                        #LOG.info(s)

                        frame_count = 0
                        next_frame_time = 0.0
                        LOG.info('Fifo: ' + pipe)
                        print('opening pipe2')
                        #fd = os.open(pipe, os.O_NONBLOCK | os.O_RDONLY)
                        fifo2 = open(pipe, 'r')
                        cap = cv2.VideoCapture(pipe)
                        #os.close(fd)
                        print('cap created!')
                        while True:
                                print('!!!pipe2 opened')
                                ret, frame = cap.read()
                                if ret == False:
                                        break
                                frame_count += 1
                                frame_file = frame_dir + "/frame%d.png" % frame_count
                                LOG.info('frame ' + str(frame_count) + ': ' + frame_file)
                                cv2.imwrite(frame_file, image)
                                continue

                                

                                
                                if self.catch_frames:
                                        frame_count += 1
                                        if self.TimeSpanBetweenFramesInSecs <= 0 or next_frame_time <= time.time():
                                                next_frame_time = time.time() + self.TimeSpanBetweenFramesInSecs

                                                if self.SaveFrames2Disk:
                                                        frame_file = frame_dir + "/frame%d.png" % frame_count
                                                LOG.info('frame ' + str(frame_count) + ': ' + frame_file)
                                                
                                                image = np.fromstring(frame, np.uint8)
                                                image = image.reshape((1080,1920,3))
                                                #image2 = cv2.imdecode(image, cv2.CV_LOAD_IMAGE_COLOR)                                

                                                with self.lock:
                                                        self.Frames.append({'image':image, 'time':time.time(), 'file':frame_file})                                        
                                                        if len(self.Frames) > self.FrameQueueMaxLength:
                                                                i = self.Frames[0]
                                                                try:# file can be in use or deleted
                                                                        os.remove(i['file'])
                                                                except:
                                                                        pass
                                                                del self.Frames[0]
                                                        
                                                if self.SaveFrames2Disk:
                                                        cv2.imwrite(frame_file, image)
                                                #image.save("frame_%d.jpg" % frame_count, image)
                                                
                                #frame = ffmpeg_process.stdout.read(FRAME_SIZE)

                                if cv2.waitKey(1) & 0xFF == ord('q'):# Press 'q' to quit
                                        LOG.info('Ctrl+C')
                                        break
                                if not self.run_frame_parser:
                                        LOG.info('NOT self.run_frame_parser')
                                        break                     
		except:
			LOG.exception(sys.exc_info()[0])
                LOG.info('exiting frame_parser')

        Frames = []
                                
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

        catch_frames = True
	
	def Suspend(self,
	):
               self.catch_frames = False
               
	def Resume(self,
	):
               self.catch_frames = True

        

if __name__ == '__main__':#not to run when this module is being imported
	import sys
	if len(sys.argv) > 1:
		stream_name = sys.argv[1]
	with Parser(
                stream_name = 'test8',
                time_span_between_frames_in_secs = 0.3,
                frame_queue_max_length = 20,
                save_frames2disk = True
                ) as p:
                
                time.sleep(3)
                f = p.GetFrame(0)#thread safe method
                if f is not None:
                        print("First frame: (%d), %s\r\n" % (f['time'], f['file']))

                time.sleep(3)
                f = p.GetFrame()#last frame
                if f is not None:
                        print("Last frame: (%d), %s\r\n" % (f['time'], f['file']))

                p.Suspend()
                #p.Frames must be accessed only after Suspend() to avoid concurrency!!!
                for f in p.Frames:
                        print("Frame: (%d), %s\r\n" % (f['time'], f['file']))                
                
                p.Resume()
                time.sleep(100)
                f = p.GetFrame()#last frame
                if f is not None:
                        print("Last frame: (%d), %s\r\n" % (f['time'], f['file']))
        exit()
