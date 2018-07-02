#AWS Kinesis stream parser
#By Sergey Stoyan <sergey.stoyan@gmail.com>

from logger import LOG
from ebml import EbmlReader, EbmlException, EbmlUnknownSizeException, EbmlInconsistentEmbeddingException, SINT, UINT, FLOAT, STRING, UTF8, DATE, MASTER, BINARY
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
            
    def __init__(self, source, elementIds2nameType=EbmlElementIds2NameType):
        super(KinesisReader, self).__init__(source)#elementIds2nameType

    def ReadNextSegment(self):  
        ''' https://matroska.org/technical/specs/rfc/index.html
        The element with unknown size MUST be an element
        with an element list as data payload. The end of the element list is
        determined by the ID of the element. When an element that isn't a
        sub-element of the element with unknown size arrives, the element
        list is ended. '''                
        #In Kinesis stream Segment elements are unknown-sized.
        #We will detect end of unknown-size Segment when new Segment begins 
        self.segmentCount = 0
        name = None
        #while name != 'Segment':
        self.position = 0            
        names2value = self._readMasterChildElements(-1)        
        return names2value
       
    def _readMasterChildElements(self, parentSize):
        LOG.info('position1:%d'%self.position) 
        names2value = {}
        
        while parentSize < 0 or self.position < parentSize:
            #LOG.info('position2:%d'%self.position) 
            size, id, name, type_ = self.readElementHead()

            if name == 'Segment':        
                self.segmentCount += 1
                if self.segmentCount > 1:
                    return names2value
                    
            if id is None:#'malformed header    
                if parentSize < 0:#'unknown-size' element
                    raise EbmlUnknownSizeException("Malformed element header while parent is unknown-size element.")
                self.read(parentSize - self.position)             
                return names2value
            
            if size < 0 and type_ is not MASTER:#'unknown-size' element
                if parentSize < 0:#'unknown-size' element
                    if name is None:#Unknown element
                        raise EbmlUnknownSizeException("Unknown element (id=%x) with unknown-size while parent is unknown-size element."%id)
                    else:
                        raise EbmlUnknownSizeException("Element (id=%x, name-%s) with unknown-size while parent is unknown-size element."%id,name)
                self.read(parentSize - self.position)           
                return names2value
            
            if type_ is None:#Unknown element
                self.read(size)
                continue
                
            if type_ is SINT:
                #LOG.info('SINT')
                value = self.readInteger(size, True)
            elif type_ is UINT:
                #LOG.info('UINT')
                value = self.readInteger(size, False)
            elif type_ is FLOAT:
                #LOG.info('FLOAT')
                value = self.readFloat(size)
            elif type_ is STRING:
                #LOG.info('STRING')
                value = self.read(size).decode('ascii')
            elif type_ is UTF8:
                #LOG.info('UTF8')
                value = self.read(size).decode('utf-8')
            elif type_ is DATE:
                #LOG.info('DATE')
                us = self.readInteger(size, True) / 1000.0  # ns to us
                from datetime import datetime, timedelta
                value = datetime(2001, 1, 1) + timedelta(microseconds=us)
            elif type_ is MASTER:
                #LOG.info('MASTER')
                value = self._readMasterChildElements(size) 
                if self.segmentCount > 1:
                    break
            elif type_ is BINARY:
                #LOG.info('BINARY')
                value = BinaryData(self.read(size))
            else:
                assert False, type_
                
            if name not in names2value:
                names2value[name] = {}
            names2value[name] = value
            
        return names2value
      
       