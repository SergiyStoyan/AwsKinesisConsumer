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

class Parser:
        
        def __enter__(self):
                return self

        def __exit__(self, exc_type, exc_value, traceback):
                LOG.info('Shutting down Parser...')
                
                self.run_kinesis_stream_reader = False
                
                self.run_frame_parser = False

                #self.ffmpeg_process.stdin.close()
                self.ffmpeg_process.kill()
                self.ffmpeg_process.terminate()
                os.killpg(os.getpgid(self.ffmpeg_process.pid), signal.SIGTERM)


        MaxFrameNumber = 10
        TimeSpanBetweenFramesInSecs = -1
        SaveFrames2Disk = True
            
	def __init__(self,
                     stream_name,
                     time_span_between_frames_in_secs = -1,
                     max_frame_number = 10,
                     save_frames2disk = True
                     ):		                             
		try:
                        LOG.info('STARTED')
                        LOG.info('stream_name: ' + stream_name)
                        
                        self.MaxFrameNumber = max_frame_number
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

                        client = boto3.client(
                                service_name = 'kinesisvideo',
                                region_name = settings.REGION_NAME
                        )
                        response = client.get_data_endpoint(
                                StreamName = stream_name,
                                APIName = 'GET_MEDIA'
                        )
                        endpoint_url = response['DataEndpoint']
                        LOG.info('endpoint_url: ' + endpoint_url)
                        
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
                        kinesis_stream = response['Payload']
                        LOG.info('kinesis_stream: ' + str(kinesis_stream))
                        
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
                        LOG.info('starting ffmpeg_process')
                        self.ffmpeg_process = sp.Popen(cmd, stdin=sp.PIPE, stdout = sp.PIPE, bufsize=10**8, preexec_fn=os.setsid)
                        
                        LOG.info('starting frame_parser')
                        from threading import Thread
                        frame_parser_thread = Thread(target = self.frame_parser, args = (self.ffmpeg_process, ))                
                        self.run_frame_parser = True
                        frame_parser_thread.start()                
                        
                        READ_BUFFER_SIZE = 1000000
                        total_bytes = 0
                        data = kinesis_stream.read(amt=READ_BUFFER_SIZE)
                        while data:
                                total_bytes += len(data)
                                print('Kinesis: ' + format(total_bytes))
                                self.ffmpeg_process.stdin.write(data)
                                self.ffmpeg_process.stdin.flush()
                                #self.ffmpeg_process.stdout.flush()
                                
                                data = kinesis_stream.read(amt=READ_BUFFER_SIZE)

                                if not self.run_kinesis_stream_reader:
                                        LOG.info('NOT self.run_kinesis_stream_reader')
                                        break                     
		except:
			LOG.exception(sys.exc_info()[0])
                LOG.info('exiting kinesis_stream_reader')




        run_frame_parser = True                
        def frame_parser(self,
                      ffmpeg_process
                      ):                
		try:
                        LOG.info('frame_parser started')
                        
                        frame_dir = '_frames'
                        if os.path.exists(frame_dir):
                                import shutil
                                shutil.rmtree(frame_dir) 
                        os.makedirs(frame_dir)

                        FRAME_SIZE = 1920 * 1080 * 3
                        frame = ffmpeg_process.stdout.read(FRAME_SIZE)
                        frame_count = 0
                        next_frame_time = 0.0
                        while frame:
                                frame_count += 1
                                if self.TimeSpanBetweenFramesInSecs <= 0 or next_frame_time <= time.time():
                                        next_frame_time = time.time() + self.TimeSpanBetweenFramesInSecs

                                        if self.SaveFrames2Disk:
                                                frame_file = frame_dir + "/frame%d.png" % frame_count
                                        LOG.info('frame ' + str(frame_count) + ': ' + frame_file)
                                        
                                        image = np.fromstring(frame, np.uint8)
                                        image = image.reshape((1080,1920,3))
                                        #image2 = cv2.imdecode(image, cv2.CV_LOAD_IMAGE_COLOR)                                
                                        
                                        self.Frames.append({'image':image, 'time':time.time(), 'file':frame_file})
                                        if len(self.Frames) > self.MaxFrameNumber:
                                                i = self.Frames[0]
                                                if os.path.isfile(i['file']):
                                                        os.remove(i['file'])
                                                del self.Frames[0]
                                                
                                        if self.SaveFrames2Disk:
                                                cv2.imwrite(frame_file, image)
                                        #image.save("frame_%d.jpg" % frame_count, image)
                                                
                                frame = ffmpeg_process.stdout.read(FRAME_SIZE)

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

                                
	def GetLastFrame(self, 
	):
		try:
                        l = len(self.Frames)
                        if l > 0:
                                return self.Frames[l - 1]                        
		except:
			LOG.exception(sys.exc_info()[0])

        

if __name__ == '__main__':#not to run when this module is being imported
	import sys
	if len(sys.argv) > 1:
		stream_name = sys.argv[1]
	with Parser(
                stream_name = 'test8',
                time_span_between_frames_in_secs = 0.3,
                max_frame_number = 20
                ) as p:
                time.sleep(10)
                f = p.GetLastFrame()
                print("Last frame: (%d), %s\r\n" % (f['time'], f['file']))
        exit()
