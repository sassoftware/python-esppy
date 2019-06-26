import uuid

class Options(object):
    def __init__(self,options = None):
        self._options = {}

        if options != None:
            for name,value in options.items():
                self._options[name] = value

    def get(self,name,dv = None):
        value = None

        if self._options != None:
            if name in self._options:
                value = self._options[name]

        if value == None and dv != None:
            value = dv
        
        return(value)

    def set(self,name,value):
        if value == None:
            if name in self._options:
                del self._options[name]
        else:
            self._options[name] = value

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
