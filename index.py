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

                self.ffmpeg_process.stdin.close()
                self.ffmpeg_process.kill()
                self.ffmpeg_process.terminate()
                os.killpg(os.getpgid(self.ffmpeg_process.pid), signal.SIGTERM)
                
                self.run_frame_parser = False
                
            
	def __init__(self,
                     stream_name
                     ):		
               
                LOG.info('STARTED')
                LOG.info('stream_name: ' + stream_name)
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
                
                from threading import Thread
                thread = Thread(target = self.kinesis_stream_reader, args = (kinesis_stream, ))
                self.run_kinesis_stream_reader = True
                thread.start()

                

        
        run_kinesis_stream_reader = True
        ffmpeg_process = None
        def kinesis_stream_reader(self,
                                kinesis_stream
                                ):

##                import os, sys
##                pipe_name = '/tmp/test_pipe'
##                try:
##                        if os.path.exists(pipe_name):
##                                os.unlink(pipe_name)
##                        os.mkfifo(pipe_name)
##                except e:
##                        LOG.info('Failed to create FIFO')
##                        return
##                BUFFER_SIZE = 100000000
##                fd = os.open(pipe_name, os.O_RDWR) #non-blocking
##                pipeW = open(pipe_name, 'wb', buffering=BUFFER_SIZE)
##                if not pipeW:
##                        LOG.info('Failed 43')
##                        return
##                LOG.info('open pipeW')

        ##        r, w = os.pipe()
        ##        pipeW = os.fdopen(w, 'wb')
        ##        LOG.info('open pipeW')
        ##        #pipeR = os.fdopen(r, 'rb')
        ##        #LOG.info('open pipeR')

##                from threading import Thread
##                thread = Thread(target = self.read_pipe, args = (pipe_name, ))
##                self.run_read_pipe = True
##                thread.start()

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
                self.ffmpeg_process = sp.Popen(cmd, stdin=sp.PIPE, stdout = sp.PIPE, bufsize=10**8, preexec_fn=os.setsid)
                
                from threading import Thread
                frame_parser_thread = Thread(target = self.frame_parser, args = (self.ffmpeg_process, ))                
                self.run_frame_parser = True
                frame_parser_thread.start()                
                
                READ_BUFFER_SIZE = 10000
                total_bytes = 0
                data = kinesis_stream.read(amt=READ_BUFFER_SIZE)
                while data:
                        total_bytes += len(data)
                        LOG.info('Kinesis: ' + format(total_bytes))
                        self.ffmpeg_process.stdin.write(data)
                        #self.ffmpeg_process.stdin.flush()
                        #self.ffmpeg_process.stdout.flush()
                        data = kinesis_stream.read(amt=READ_BUFFER_SIZE)
                        if not self.run_kinesis_stream_reader:
                                LOG.info('self.run_kinesis_stream_reader')
                                break
                LOG.info('exiting kinesis_stream_reader')




        run_frame_parser = True                
        def frame_parser(self,
                      ffmpeg_process
                      ):
                
##                cap = cv2.VideoCapture(pipe_name)
##                #cap = cv2.VideoCapture(r)
##                LOG.info('cap created')
##                count = 0
##                success = True
##                while success:
##                        success, frame = cap.read()
##                        LOG.info('frame: ' + str(count))
##                        cv2.imwrite("frame%d.jpg" % count, frame) 
##                        count += 1
##                LOG.info('exit cap reading')
##                return
                                
                #READ_BUFFER_SIZE = 10000
                FRAME_SIZE = 1920 * 1080 * 3
                total_bytes = 0
                #data = ffmpeg_process.stdout.read(amt=READ_BUFFER_SIZE)
                data = ffmpeg_process.stdout.read(FRAME_SIZE)
                frame_count = 0
                while data:
                        total_bytes += len(data)
                        LOG.info('ffmpeg: ' + format(total_bytes))
                        #a = data.find('\xff\xd8')
                        #b = data.find('\xff\xd9')
                        #if a >= 0 and b >= 0:
                        frame_count += 1
                        LOG.info('frame: ' + str(frame_count))
                        #        frame = data[a:b+2]
                        #        data = data[b+2:]
                        frame = data
                        image = np.fromstring(frame, np.uint8)
                        image = image.reshape((1080,1920,3))
                                #image2 = cv2.imdecode(image, cv2.CV_LOAD_IMAGE_COLOR)                                
                        cv2.imwrite("frame%d.jpg" % frame_count, image)
                        image.save("frame_%d.jpg" % frame_count, image)
                                #with open('test_a1.jpg', 'wb') as output:
                                #        output.write(jpg)                      
                                #with open('test_a2.jpg', 'wb') as output:
                                #        output.write(frame)


        #                        image = Image.open(io.BytesIO(jpg))
        #                        image.save("test.jpg")
        #                       image = Image.open(io.BytesIO(frame))
        #                       image.save("test2.jpg")
                                #return
                        #data += ffmpeg_process.stdout.read(amt=READ_BUFFER_SIZE)
                        data = ffmpeg_process.stdout.read(FRAME_SIZE)
                        if cv2.waitKey(1) & 0xFF == ord('q'):# Press 'q' to quit
                                LOG.info('Ctrl+C')
                                break
                        if not self.run_frame_parser:
                                LOG.info('not self.run_frame_parser')
                                break
                LOG.info('exiting frame_parser')

                        
	def GetLastFrame(self, 
	):
		try:
                        LOG.info('GetLastFrame')
                        return
		except:
			LOG.exception(sys.exc_info()[0])

        

if __name__ == '__main__':#not to run when this module is being imported
	import sys
	stream_name = 'test8'
	if len(sys.argv) > 1:
		stream_name = sys.argv[1]
	with Parser(stream_name = stream_name) as p:
                time.sleep(5)
                p.GetLastFrame()
