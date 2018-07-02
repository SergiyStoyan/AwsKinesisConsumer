#Non-seekable EBML (matroska) stream parser
#By Sergey Stoyan <sergey.stoyan@gmail.com>
#This code is based on this Matroska parser https://github.com/exaile/exaile/blob/master/xl/metadata/_matroska.py
#It was upgraded to parse non-seekable streams. Also, several mistakes were fixed.

#from __future__ import print_function
from logger import LOG
import sys
from struct import pack, unpack

SINT, UINT, FLOAT, STRING, UTF8, DATE, MASTER, BINARY = range(8)

class EbmlException(Exception):
    pass

class EbmlWarning(Warning):
    pass

class BinaryData(bytes):
    def __repr__(self):
        return "<BinaryData>"

def bchr(n):
    """chr() that always returns bytes in Python 2 and 3"""
    return pack('B', n)

class EbmlReader:

    def __init__(self, source, elementIds2nameType):
        self.elementIds2nameType = elementIds2nameType
        try:
            self.stream = open(source, 'rb')
        except:
            self.stream = source

    def __del__(self):
        try:
            self.stream.close()
        except:
            pass

    def read(self, length):
        self.position += length
        return self.read(length)
        
    def readElementId(self):
        b = self.read(1)
        b1 = ord(b)
        if b1 & 0b10000000:  # 1 byte
            return unpack(">H", b"\0" + b)[0]
        elif b1 & 0b01000000:  # 2 bytes
            return unpack(">H", b + self.read(1))[0]
        elif b1 & 0b00100000:  # 3 bytes
            return unpack(">L", b"\0" + b + self.read(2))[0]
        elif b1 & 0b00010000:  # 4 bytes
            return unpack(">L", b + self.read(3))[0]
        else:
            raise EbmlException("invalid element ID (leading byte 0x%02X)" % b1)
    
    unknownSizes = [
            2**7 - 1,
            2**14 - 1,
            2**21 - 1,
            2**28 - 1,
            2**35 - 1,
            2**42 - 1,
            2**49 - 1,
            2**56 - 1,
    ]
    
    def readElementSize(self):
        b1 = ord(self.read(1))
        if b1 & 0b10000000:  # 1 byte
            s = unpack(">H", b"\0" + bchr(b1 & 0b01111111))[0]
            if s == self.unknownSizes[0]:#'unknown-sized' element
                s = -1
        elif b1 & 0b01000000:  # 2 bytes
            s = unpack(">H", bchr(b1 & 0b00111111) + self.read(1))[0]
            if s == self.unknownSizes[1]:#'unknown-sized' element
                s = -1
        elif b1 & 0b00100000:  # 3 bytes
            s = unpack(">L", b"\0" + bchr(b1 & 0b00011111) + self.read(2))[0]
            if s == self.unknownSizes[2]:#'unknown-sized' element
                s = -1
        elif b1 & 0b00010000:  # 4 bytes
            s = unpack(">L", bchr(b1 & 0b00001111) + self.read(3))[0]
            if s == self.unknownSizes[3]:#'unknown-sized' element
                s = -1
        elif b1 & 0x00001000:  # 5 bytes
            s = unpack(">Q", b"\0\0\0" + bchr(b1 & 0b00000111) + self.read(4))[0]
            if s == self.unknownSizes[4]:#'unknown-sized' element
                s = -1
        elif b1 & 0b00000100:  # 6 bytes
            s = unpack(">Q", b"\0\0" + bchr(b1 & 0b0000011) + self.read(5))[0]
            if s == self.unknownSizes[5]:#'unknown-sized' element
                s = -1
        elif b1 & 0b00000010:  # 7 bytes
            s = unpack(">Q", b"\0" + bchr(b1 & 0b00000001) + self.read(6))[0]
            if s == self.unknownSizes[6]:#'unknown-sized' element
                s = -1
        elif b1 & 0b00000001:  # 8 bytes
            s = unpack(">Q", b"\0" + self.read(7))[0]
            if s == self.unknownSizes[7]:#'unknown-sized' element
                s = -1
        else:
            assert b1 == 0
            raise EbmlException("undefined element size")
        return s

    def readInteger(self, length, signed):
        if length == 1:
            value = ord(self.read(1))
        elif length == 2:
            value = unpack(">H", self.read(2))[0]
        elif length == 3:
            value = unpack(">L", b"\0" + self.read(3))[0]
        elif length == 4:
            value = unpack(">L", self.read(4))[0]
        elif length == 5:
            value = unpack(">Q", b"\0\0\0" + self.read(5))[0]
        elif length == 6:
            value = unpack(">Q", b"\0\0" + self.read(6))[0]
        elif length == 7:
            value = unpack(">Q", b"\0" + (self.read(7)))[0]
        elif length == 8:
            value = unpack(">Q", self.read(8))[0]
        else:
            raise EbmlException("don't know how to read %r-byte integer" % length)
        if signed:
            nbits = (8 - length) + 8 * (length - 1)
            if value >= (1 << (nbits - 1)):
                value -= 1 << nbits
        return value

    def readFloat(self, length):
        if length == 4:
            return unpack('>f', self.read(4))[0]
        elif length == 8:
            return unpack('>d', self.read(8))[0]
        else:
            raise EbmlException("don't know how to read %r-byte float" % length)
  
    def ReadLevel0Element(self):
        '''!!!ATTENTION: this methos is not appropriate for parsing unknown-size elements
        because it does not return to upper level'''
        self.position = 0
        size, id, name, type_ = self.readElementHead()
        return self._readElement(0, size)
        
    def readElementHead(self)    
        try:
            id = self.readElementId()
        except EbmlException as e:
            # Invalid EBML header. We can't reliably get any more data from
            # this level, so just return anything we have.
            raise EbmlException("Invalid EBML header")
        size = self.readElementSize()       
        try:
            name, type_ = self.elementIds2nameType[id]
        except:
            if size < 0:#'unknown-size' element
                raise EbmlException("Unknown element (id=%x) with unknown-size. Parsing is impossible."%id)
        return (size, id, name, type_)
        
    def _readElement(self, level, parentSize):
        '''!!!ATTENTION: this methos is not appropriate for parsing unknown-size elements
        because it does not return to upper level'''
        node = {}
        # Iterate over current node's children.
        while self.position < parentSize:
            size, id, name, type_ = self.readElementHead()
            if(size < 0):#'unknown-size' element
                read(size)
                continue
            try:
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
                    value = self.stream.read(size).decode('ascii')
                elif type_ is UTF8:
                    #LOG.info('UTF8')
                    value = self.stream.read(size).decode('utf-8')
                elif type_ is DATE:
                    #LOG.info('DATE')
                    us = self.readInteger(size, True) / 1000.0  # ns to us
                    from datetime import datetime, timedelta
                    value = datetime(2001, 1, 1) + timedelta(microseconds=us)
                elif type_ is MASTER:
                    #LOG.info('MASTER')
                    value = self._readElement(level + 1, size)
                elif type_ is BINARY:
                    #LOG.info('BINARY')
                    value = BinaryData(self.stream.read(size))
                else:
                    assert False, type_
            except (EbmlException, UnicodeDecodeError) as e:
                #LOG.exception(sys.exc_info()[0])
                print(sys.exc_info()[0])
            else:
                if n not in value:
                    value[n] = []
                node[name].append(value)                
        return node
      
       

# Matroska (EBML) elements to be parsed. Elements not defined here will be skipped.
EbmlElementIds2NameType = {
    0x1a45dfa3: ('EBML', MASTER),
    # Segment
    0x18538067: ('Segment', MASTER),
    # Segment Information
    0x1549A966: ('Info', MASTER),
    0x7384: ('SegmentFilename', UTF8),
    0x2AD7B1: ('TimecodeScale', UINT),
    0x4489: ('Duration', FLOAT),
    0x4461: ('DateUTC', DATE),
    0x7BA9: ('Title', UTF8),
    0x4D80: ('MuxingApp', UTF8),
    0x5741: ('WritingApp', UTF8),
    # Track
    0x1654AE6B: ('Tracks', MASTER),
    0xAE: ('TrackEntry', MASTER),
    0xD7: ('TrackNumber', UINT),
    0x83: ('TrackType', UINT),
    0xB9: ('FlagEnabled', UINT),
    0x88: ('FlagDefault', UINT),
    0x23E383: ('DefaultDuration', UINT),
    0x536E: ('Name', UTF8),
    0x22B59C: ('Language', STRING),
    0x86: ('CodecID', STRING),
    0x258688: ('CodecName', UTF8),
    # Video
    0xE0: ('Video', MASTER),
    # Audio
    0xE1: ('Audio', MASTER),
    0xB5: ('SamplingFrequency', FLOAT),
    0x78B5: ('OutputSamplingFrequency', FLOAT),
    0x9F: ('Channels', UINT),
    0x6264: ('BitDepth', UINT),
    # Attachment
    0x1941A469: ('Attachments', MASTER),
    0x61A7: ('AttachedFile', MASTER),
    0x466E: ('FileName', UTF8),
    0x465C: ('FileData', BINARY),
    # Chapters
    0x1043A770: ('Chapters', MASTER),
    0x45B9: ('EditionEntry', MASTER),
    0x45BC: ('EditionUID', UINT),
    0x45BD: ('EditionFlagHidden', UINT),
    0x45DB: ('EditionFlagDefault', UINT),
    0x45DD: ('EditionFlagOrdered', UINT),
    0xB6: ('ChapterAtom', MASTER),
    0x73C4: ('ChapterUID', UINT),
    0x91: ('ChapterTimeStart', UINT),
    0x92: ('ChapterTimeEnd', UINT),
    0x98: ('ChapterFlagHidden', UINT),
    0x4598: ('ChapterFlagEnabled', UINT),
    0x63C3: ('ChapterPhysicalEquiv', UINT),
    0x8F: ('ChapterTrack', MASTER),
    0x89: ('ChapterTrackNumber', UINT),
    0x80: ('ChapterDisplay', MASTER),
    0x85: ('ChapString', UTF8),
    0x437C: ('ChapLanguage', STRING),
    0x437E: ('ChapCountry', STRING),
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
    # Cluster Information
    0x1f43b675: ('Cluster', MASTER),
    0xe7 : ('Timecode', UINT),
    0xa7  : ('Position', UINT),
    0xab  : ('PrevSize', UINT),
    #0xa0  : ('BlockGroup',     ),
    0xa1 : ('Block', BINARY),
    0xa2  : ('BlockVirtual', BINARY),
    #0x75a1  : ('BlockAdditions', ),
}
            


    
