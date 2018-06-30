import logging
import os

# def get_start_module_name():
	# import inspect
	# caller = inspect.currentframe()
	# print caller.f_globals['__file__']
	# while caller.f_back:
		# print caller.f_globals['__file__']
		# caller = caller.f_back
#get_start_module_name()

log_dir = '_logs'
import os
import sys
if not os.path.exists(log_dir):
	os.makedirs(log_dir)
log_path = log_dir + '/' + os.path.basename(sys.argv[0]) + '.log'		

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

formatter = logging.Formatter(fmt='%(asctime)s %(filename)s(%(lineno)d) %(levelname)s %(message)s', datefmt='%Y-%m-%d,%H:%M:%S')

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
LOG.addHandler(ch)

import logging.handlers

#fh = logging.FileHandler(log_path)
#fh.setLevel(logging.INFO)
#fh.setFormatter(formatter)

fh = logging.handlers.RotatingFileHandler(log_path, mode='a', maxBytes=None, backupCount=9)
fh.doRollover()
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

#fh = logging.handlers.TimedRotatingFileHandler(log_path, when='D', interval=1, backupCount=9)
#fh.setLevel(logging.INFO)
#fh.setFormatter(formatter)

LOG.addHandler(fh)


