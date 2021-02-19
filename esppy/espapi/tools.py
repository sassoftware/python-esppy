import logging
import uuid

class Options(object):
    def __init__(self,**kwargs):
        self._opts = {}

        for name,value in kwargs.items():
            self.setOpt(name,value)

    def hasOpt(self,name):
        code = False

        if isinstance(name,list):
            for n in name:
                if n.lower() in self._opts:
                    code = True
                    break
        else:
            code = name.lower() in self._opts

        return(code)

    def hasOpts(self,opts):
        code = True

        for o in opts:
            if self.hasOpt(o) == False:
                code = False
                break

        return(code)

    def getOpt(self,name,dv = None,clear = False):
        value = None
        s = name.lower()

        if s in self._opts:
            value = self._opts[s]
            if clear:
                self.clear(s)

        if value == None and dv != None:
            value = dv
        
        return(value)

    def getInt(self,name,dv = None,clear = False):
        value = self.getOpt(name,dv,clear)
        if value != None:
            value = int(value)
        return(value)

    def setOpt(self,name,value):
        s = name.lower()
        if value == None:
            if s in self._opts:
                del self._opts[s]
                self.optionSet(name,None)
        else:
            self._opts[s] = value
            self.optionSet(s,value)

    def optionSet(self,name,value):
        pass

    def clear(self,name):
        s = name.lower()
        if s in self._opts:
            del self._opts[s]

    def setOpts(self,**kwargs):
        for name,value in kwargs.items():
            self.setOpt(name,value)

    def items(self):
        return(self._opts.items())

    def __str__(self):
        i = 0
        s = ""
        for name,value in self._opts.items():
            if i > 0:
                s += ","
            s += name
            s += "="
            s += str(value)
            i = i + 1
        return(s)

    @property
    def options(self):
        return(self._opts)

    @options.setter
    def options(self,options):
        for name,value in six.iteritems(options):
            self.setOpt(name,value)

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
        list.append(o)
        return(True)

    return(False)

def setItem(list,o):
    item = None

    for i in list:
        if i == o:
            item = i
            break

    if item == None:
        item = o
        list.append(o)
    else:
        for x in o:
            item[x] = o[x]

    return(item)

def removeFrom(list,o):
    code = False
    index = indexOf(list,o)
    if index >= 0:
        list.pop(index)
        code = True
    return(code)

def guid():
    return(str(uuid.uuid4()).replace('-', '_'))

def build(o,fields):
    current = o

    for f in fields:
        if f in "current" == False:
            current[f] = {}
            current = current[f]
