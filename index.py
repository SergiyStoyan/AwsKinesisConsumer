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
        LOG.info('STARTED: ' + os.path.basename(__file__))
        
        client = boto3.client(
                service_name = 'kinesis-video-media',
                aws_access_key_id = settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY,
                region_name = settings.REGION_NAME
        )

        response = client.get_media(
                StreamName = stream_name,
                StartSelector = {
                        'StartSelectorType': 'NOW',
                }
        )
        LOG.info(response)
        
        LOG.info('COMPLETED')

if __name__ == '__main__':#not to run when this module is being imported
	import sys
	stream_name = 'test8'
	if len(sys.argv) > 1:
		stream_name = sys.argv[1]
	Execute(stream_name = stream_name)
