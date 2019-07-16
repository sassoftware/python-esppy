import logging
import uuid

import numpy as np

class Options(object):
    def __init__(self,**kwargs):
        self._options = {}

        for name,value in kwargs.items():
            self.set(name,value)

    def has(self,name):
        code = False

        if isinstance(name,list):
            for n in name:
                if n.lower() in self._options:
                    code = True
                    break
        else:
            code = name.lower() in self._options

        return(code)

    def get(self,name,dv = None,clear = False):
        value = None
        s = name.lower()

        if s in self._options:
            value = self._options[s]
            if clear:
                self.clear(s)

        if value == None and dv != None:
            value = dv
        
        return(value)

    def set(self,name,value):
        s = name.lower()
        if value == None:
            if s in self._options:
                del self._options[s]
        else:
            self._options[s] = value

    def clear(self,name):
        s = name.lower()
        if s in self._options:
            del self._options[s]

    def setOptions(self,**kwargs):
        for name,value in kwargs.items():
            self.set(name,value)

    def items(self):
        return(self._options.items())

    @property
    def options(self):
        return(self._options)

    @options.setter
    def options(self,options):
        for name,value in six.iteritems(options):
            self.set(name,value)

class Gradient(object):
    def __init__(self,color,**kwargs):
        if len(color) != 7:
            raise Exception("invalid color: " + str(color))

        self._color = color
        self._options = Options(**kwargs)
        self._levels = self._options.get("levels",100)

        minv = self._options.get("min",0)
        maxv = self._options.get("max",100)

        self._a = np.arange(minv,maxv,(maxv - minv) / self._levels)

    def darken(self,value):
        a = np.where(value >= self._a)[0]
        level = len(a) - 1

        rgbHex = [self._color[x:x + 2] for x in [1, 3, 5]]
        rgbInt = [int(v, 16) - level for v in rgbHex]
        rgbInt = [min([255, max([0,i])]) for i in rgbInt]

        c = "#" + "".join([hex(i)[2:] for i in rgbInt])

        return(c)

    def brighten(self,value):
        a = np.where(value >= self._a)[0]
        level = len(a) - 1

        rgbHex = [self._color[x:x + 2] for x in [1, 3, 5]]
        rgbInt = [int(value, 16) + level for value in rgbHex]
        rgbInt = [min([255, max([0,i])]) for i in rgbInt]

        c = "#" + "".join([hex(i)[2:] for i in rgbInt])

        return(c)

def supports(o,method):
    code = False
    if (o != None):
        try:
            value = getattr(o,method)
            if (value != None):
                code = callable(value)
        except:
            pass
    return(code)

def contains(list,o):
    for item in list:
        if item == o:
            return(True)

    return(False)

def indexOf(list,o):
    i = 0
    for item in list:
        if item == o:
            return(i)
        i += 1

    return(-1)

def addTo(list,o):
    if contains(list,o) == False:
        list.append(o);
        return(True);

    return(False);

def removeFrom(list,o):
    index = indexOf(list,o)
    if index >= 0:
        list.pop(index)

def guid():
    return(str(uuid.uuid4()).replace('-', '_'))

def brighten(color,offset):
    if len(color) != 7:
        return("#ffffff")

    rgbHex = [color[x:x + 2] for x in [1, 3, 5]]
    rgbInt = [int(value, 16) + offset for value in rgbHex]
    rgbInt = [min([255, max([0,i])]) for i in rgbInt]

    c = "#" + "".join([hex(i)[2:] for i in rgbInt])

    return(c)

def darken(color,offset):
    if len(color) != 7:
        return("#ffffff")

    rgbHex = [color[x:x + 2] for x in [1, 3, 5]]
    rgbInt = [int(value, 16) - offset for value in rgbHex]
    rgbInt = [min([255, max([0,i])]) for i in rgbInt]

    c = "#" + "".join([hex(i)[2:] for i in rgbInt])

    return(c)
