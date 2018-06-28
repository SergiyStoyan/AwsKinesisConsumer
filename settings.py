import _settings
import os

try:
	initialized
except:	
	initialized = True
	os.environ["AWS_ACCESS_KEY_ID"] = _settings.AWS_ACCESS_KEY_ID
	os.environ["AWS_SECRET_ACCESS_KEY"] = _settings.AWS_SECRET_ACCESS_KEY
	#os.environ["REGION_NAME"] = _settings.REGION_NAME
	REGION_NAME = _settings.REGION_NAME
