#by Sergey Stoyan: sergey.stoyan@gmail.com
from logger import LOG
import _settings as settings
import os
#import imp
import boto3
#import json
#from datetime import datetime
#import time
import cv2#, platform
import io
import numpy as np
#import Image

class Parser:
        
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

                self.read_kinesis_stream(kinesis_stream)
                
                LOG.info('COMPLETED')

        def read_kinesis_stream(self,
                                kinesis_stream
                                ):

                import os, sys
                pipe_name = '/tmp/test_pipe'
                try:
                        if os.path.exists(pipe_name):
                                os.unlink(pipe_name)
                        os.mkfifo(pipe_name)
                except e:
                        LOG.info('Failed to create FIFO')
                        return
                BUFFER_SIZE = 100000000
                fd = os.open(pipe_name, os.O_RDWR) #non-blocking
                pipeW = open(pipe_name, 'wb', buffering=BUFFER_SIZE)
                if not pipeW:
                        LOG.info('Failed 43')
                        return
                LOG.info('open pipeW')

        ##        r, w = os.pipe()
        ##        pipeW = os.fdopen(w, 'wb')
        ##        LOG.info('open pipeW')
        ##        #pipeR = os.fdopen(r, 'rb')
        ##        #LOG.info('open pipeR')

                from threading import Thread
                thread = Thread(target = self.read_pipe, args = (pipe_name, ))
                self.run_read_pipe = True
                thread.start()
                
                READ_BUFFER_SIZE = 10000
                total_bytes = 0
                data = kinesis_stream.read(amt=READ_BUFFER_SIZE)
                while data:
                        total_bytes += len(data)
                        LOG.info('Read: ' + format(total_bytes))
                        pipeW.write(data);
                        pipeW.flush();
                        data = kinesis_stream.read(amt=READ_BUFFER_SIZE)

                self.run_read_pipe = False

        run_read_pipe = True                
        def read_pipe(self,
                      pipe_name
                      ):
                
                cap = cv2.VideoCapture(pipe_name)
                #cap = cv2.VideoCapture(r)
                LOG.info('cap created')
                count = 0
                success = True
                while success:
                        success, frame = cap.read()
                        LOG.info('frame: ' + str(count))
                        cv2.imwrite("frame%d.jpg" % count, frame) 
                        count += 1
                LOG.info('exit cap reading')
                return
                                
                MAX_BUFFER = 10000
                total_bytes = 0
                data = stream.read(amt=MAX_BUFFER)
                while data and self.run_read_pipe:
                        total_bytes += len(data)
                        LOG.info('Read: ' + format(total_bytes))
                        a = data.find('\xff\xd8')# 0x000001
                        b = data.find('\xff\xd9')
                        if a!=-1 and b!=-1:
                                LOG.info('Frame')
                                jpg = data[a:b+2]
                                data = data[b+2:]
                                frame = cv2.imdecode(np.fromstring(jpg, np.uint8), cv2.CV_LOAD_IMAGE_COLOR)                        
                                cv2.imwrite('test_a3.jpg', frame)
                                with open('test_a1.jpg', 'wb') as output:
                                        output.write(jpg)                      
                                #with open('test_a2.jpg', 'wb') as output:
                                #        output.write(frame)


        #                        image = Image.open(io.BytesIO(jpg))
        #                        image.save("test.jpg")
        #                       image = Image.open(io.BytesIO(frame))
        #                       image.save("test2.jpg")
                                #return
                        data += stream.read(amt=MAX_BUFFER)
                        
        ##bytes+=stream.read(1024)
        ##    a = bytes.find('\xff\xd8')
        ##    b = bytes.find('\xff\xd9')
        ##    if a!=-1 and b!=-1:
        ##        jpg = bytes[a:b+2]
        ##        bytes= bytes[b+2:]
        ##    frame = cv2.imdecode(np.fromstring(jpg, dtype=np.uint8),cv2.CV_LOAD_IMAGE_COLOR)
        ##    # we now have frame stored in frame.
        ##
        ##    cv2.imshow('cam2',frame)
        ##
        ##    # Press 'q' to quit 
        ##    if cv2.waitKey(1) & 0xFF == ord('q'):
        ##        break

                        
	def GetLastFrame(self, 
	):
		try:
                        return
		except:
			LOG.exception(sys.exc_info()[0])

        

if __name__ == '__main__':#not to run when this module is being imported
	import sys
	stream_name = 'test8'
	if len(sys.argv) > 1:
		stream_name = sys.argv[1]
	p = Parser(stream_name = stream_name)
	p.GetLastFrame()
