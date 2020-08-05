from base64 import b64encode

import logging
import struct
import sys
import io

class JsonEncoder(object):
    def __init__(self,o):
        size = sys.getsizeof(o)
        size += (size * .5)
        size = int(size)
        self._data = io.BytesIO();
        self._index = 0
        self._debug = False
        self.encode(o,None)

    def encode(self,o,name):
        if isinstance(o,list):
            self.writeName(name)
            self.beginArray()
            for item in o:
                self.encode(item,None)
            self.endArray()
        elif isinstance(o,dict):
            self.writeName(name)
            self.beginObject()
            for x in o:
                self.encode(o[x],x)
            self.endObject()
        elif isinstance(o,bytes):
            self.writeBuffer(o,name)
        else:
            self.writeValue(o,name)

    def writeValue(self,value,name):
        s = str(value)
        self.writeName(name);
        self.writeType('S');
        self.writeLength(len(s));

        if self._debug:
            tmp = s
            if len(tmp) > 50:
                tmp = tmp.substr(0,20) + "..."
            print("index: " + str(self._index) + " write string: " + str(tmp))

        self._data.write(s.encode("utf-8"))
        self._index += len(s)

    def writeType(self,type):
        if self._debug:
            print("index: " + str(self._index) + " write type: " + str(type))
        self._data.write(type.encode("utf-8"))
        self._index += 1

    def writeLength(self,length):
        if self._debug:
            print("index: " + str(self._index) + " write length: " + str(length))

        bytes = length.to_bytes(4,byteorder="big")

        self._data.write(bytes)
        self._index += 4

    def writeBuffer(self,value,name):
        self.writeName(name)
        self.writeType('B')
        self.writeLength(len(value))
        self._data.write(value)
        self._index += len(value)

    def writeName(self,name):
        if name != None and len(name) > 0:
            self.writeLength(len(name))

            if self._debug:
                print("index: " + str(self._index) + " write name: " + str(name))

            self._data.write(name.encode("utf-8"))

            self._index += len(name)

    def beginObject(self):
        self.writeType('{')

    def endObject(self):
        self.writeType('}')

    def beginArray(self):
        self.writeType('[')

    def endArray(self):
        self.writeType(']')

    @property
    def data(self):
        return(self._data.getvalue())

class JsonDecoder(object):

    def __init__(self,data):
        self._data = data
        self._size = len(self._data)
        self._index = 0
        self._o = None
        self._debug = False
        self.addTo(None,None)

    def addTo(self,name,to):

        type = self.getType(True)

        if type == '{':
            self.addObject(name,to)
        elif type == '[':
            self.addArray(name,to)
        else:
            self.addValue(name,to)

    def addObject(self,name,to):
        if self._debug:
            print("index: " + str(self._index) + " add object: " + str(name) + " to " + str(to))

        o = {}

        if self._o == None:
            self._o = o
        elif isinstance(to,list):
            to.append(o)
        else:
            to[name] = o

        while self._index < self._size:
            type = self.getType(False)

            if type == '}':
                self._index += 1
                break
            else:
                length = self.getLength()
                name = self.getString(length)
                type = self.getType(False)

                if type == '{':
                    self.addTo(name,o)
                elif type == '[':
                    self.addTo(name,o)
                else:
                    self.addValue(name,o)

        if self._debug:
            print("index: " + str(self._index) + " add object: " + str(name) + " to " + str(to) + " complete")

    def addArray(self,name,to):

        if self._debug:
            print("index: " + str(self._index) + " add array: " + str(name) + " to " + str(to))

        a = []

        if self._o == None:
            self._o = a
        elif isinstance(to,list):
            to.append(a)
        else:
            to[name] = a

        while self._index < self._size:
            type = self.getType(False)

            if (type == '['):
                self.addTo("",a)
            elif type == ']':
                self._index += 1
                break
            elif type == '{':
                self.addTo("",a)
            else:
                self.addValue("",a)

        if self._debug:
            print("index: " + str(self._index) + " add array: " + str(name) + " to " + str(to) + " complete")

    def addValue(self,name,to):
        if self._debug:
            print("index: " + str(self._index) + " add value: " + str(name) + " to " + str(to))

        type = self.getType(True)
        value = None

        if type == 'S':
            length = self.getLength()
            if length > 0:
                if self._debug:
                    print("index: " + str(self._index) + " get string of " + str(length) + " bytes")

                value = self._data[self._index:self._index + length].decode("utf-8")
                self._index += length
        elif type == 'B':
            length = self.getLength()
            if length > 0:
                if self._debug:
                    print("index: " + str(self._index) + " get blob of " + str(length) + " bytes")

                value = self._data[self._index:self._index + length]
                value = b64encode(value).decode("utf-8")
                self._index += length
        elif type == 'l':
            value = self.getI32()
        elif type == 'L':
            value = self.getI64()
        elif type == 'D':
            value = self.getDouble()

        if value != None:
            if isinstance(to,list):
                to.push(value)
            else:
                to[name] = value

    def getType(self,increment):
        type = chr(self._data[self._index])

        if self._debug:
            print("index: " + str(self._index) + " get type: " + str(type))

        if increment:
            self._index += 1

        return(type)

    def getLength(self):
        length = int.from_bytes(self._data[self._index:self._index + 4],byteorder="big")

        if self._debug:
            print("index: " + str(self._index) + " get length: " + str(length))

        self._index += 4

        return(length)

    def getString(self,length):
        value = self._data[self._index:self._index + length].decode("utf-8")

        if self._debug:
            print("index: " + str(self._index) + " get string: " + str(value))

        self._index += length
        return(value)

    def getI32(self):
        value = int.from_bytes(self._data[self._index:self._index + 4],byteorder="big")

        if self._debug:
            print("index: " + str(self._index) + " get i32: " + str(value))

        self._index += 4

        return(value)

    def getI64(self):
        value = int.from_bytes(self._data[self._index:self._index + 8],byteorder="big")

        if self._debug:
            print("index: " + str(self._index) + " get i64: " + str(value))

        self._index += 8

        return(value)

    def getDouble(self):
        value = struct.unpack(">d",self._data[self._index:self._index + 8])[0]

        if self._debug:
            print("index: " + str(self._index) + " get double: " + str(value))

        self._index += 8

        return(value)

    @property
    def data(self):
        return(self._o)


