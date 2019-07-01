import uuid

class Options(object):
    def __init__(self,**kwargs):
        self._options = {}

        for name,value in kwargs.items():
            self.set(name,value)

    def has(self,name):
        return(name.lower() in self._options)

    def get(self,name,dv = None):
        value = None
        s = name.lower()

        if s in self._options:
            value = self._options[s]

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

    def setOptions(**kwargs):
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
