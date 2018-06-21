#by Sergey Stoyan: sergey.stoyan@gmail.com
from logger import LOG
import _settings as settings
import os
#import imp
import boto3
#import json
#from datetime import datetime
#import time

def Execute(stream_name):
        LOG.info('STARTED')
        LOG.info('stream_name' + stream_name)
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
        #LOG.info(endpoint_url)
        
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
        streaming_body = response['Payload']
        LOG.info(streaming_body)

        read_stream(streaming_body)
        
        LOG.info('COMPLETED')


def read_stream(streaming_body):
        total_bytes = 0L
        data = streaming_body.read(amt=10000)
        while data:
                total_bytes += len(data)
                LOG.info('Read: ' + format(total_bytes))
                data = streaming_body.read(amt=10000)
        

if __name__ == '__main__':#not to run when this module is being imported
	import sys
	stream_name = 'test8'
	if len(sys.argv) > 1:
		stream_name = sys.argv[1]
	Execute(stream_name = stream_name)
