#AWS Kinesis stream parser
#By Sergey Stoyan <sergey.stoyan@gmail.com>

from logger import LOG
from ebml_reader import EbmlReader, EbmlException
import os
import sys
import io
import threading
#import os.path
#from datetime import timedelta, datetime

EbmlElementIds2NameType = {
    0x18538067: ('Segment', MASTER),
    # Tagging
    0x1254C367: ('Tags', MASTER),
    0x7373: ('Tag', MASTER),
    0x63C0: ('Targets', MASTER),
    0x68CA: ('TargetTypevalue', UINT),
    0x63CA: ('TargetType', STRING),
    0x63C5: ('TagTrackUID', UINT),
    0x63C9: ('TagEditionUID', UINT),
    0x63C4: ('TagChapterUID', UINT),
    0x67C8: ('SimpleTag', MASTER),
    0x45A3: ('TagName', UTF8),
    0x447A: ('TagLanguage', STRING),
    0x4484: ('TagDefault', UINT),
    0x4487: ('TagString', UTF8),
    0x4485: ('TagBinary', BINARY),
}

class KinesisReader(EbmlReader):
            
    def __init__(self, source, elementIds2nameType):
        super(EbmlReader, self).__init__( source, elementIds2nameType)

    def ReadNextSegment(self):
        self.position = 0                
        while name != 'Segment':
            (name, value, newSegmentStarted) = self._readElement()
        return value
        
    def _readElement(self):
        ''' https://matroska.org/technical/specs/rfc/index.html
        The element with unknown size MUST be an element
        with an element list as data payload. The end of the element list is
        determined by the ID of the element. When an element that isn't a
        sub-element of the element with unknown size arrives, the element
        list is ended. '''                
        #In Kinesis stream Segment elements are unknown-sized.
        #We will detect end of unknown-size Segment when new Segment begins   
        newSegmentStarted = False
            
        size, id, name, type_ = self.readElementHead()
        LOG.info('element id:%x'%id)
        LOG.info('-------size:%d'%size)
        if name is None:
            return (name, None, newSegmentStarted)
        if size < 0:#'unknown-size' element
            raise EbmlException("Element '%s' is 'unknown-size' element. In Kinesis stream only 'Segment' is expected to be 'unknown-size' element."%name) 
        if type_ is SINT:
            LOG.info('SINT')
            value = self.readInteger(size, True)
        elif type_ is UINT:
            LOG.info('UINT')
            value = self.readInteger(size, False)
        elif type_ is FLOAT:
            LOG.info('FLOAT')
            value = self.readFloat(size)
        elif type_ is STRING:
            LOG.info('STRING')
            value = self.read(size).decode('ascii')
        elif type_ is UTF8:
            LOG.info('UTF8')
            value = self.read(size).decode('utf-8')
        elif type_ is DATE:
            LOG.info('DATE')
            us = self.readInteger(size, True) / 1000.0  # ns to us
            from datetime import datetime, timedelta
            value = datetime(2001, 1, 1) + timedelta(microseconds=us)
        elif type_ is MASTER:
            LOG.info('MASTER')
            value = {}
            endP = self.position + size
            while self.position < endP:                        
                (n, v, new_segment_started) = self._readElement()                
                if n not in value:
                    value[n] = []
                value[n].append(v)
                newSegmentStarted = new_segment_started or n == 'Segment'
        elif type_ is BINARY:
            LOG.info('BINARY')
            value = BinaryData(self.read(size))
        else:
            LOG.exception('!!!!!!!!!!!!!!!!!!!!!!!!!!unknown type')
            assert False, type_
        return (name, value, newSegmentStarted)
