import threading
import requests
import datetime
import logging
import time

from urllib.parse import urlparse

import esppy.espapi.tools as tools

from xml.etree import ElementTree

class EventSources(object):
    def __init__(self,connection,delegate = None):
        self._connection = connection
        self._delegate = delegate
        self._eventsources = {}
        self._running = False
        self._paused = False
        self._restart = False
        self._config = None
        self._connection.addDelegate(self)

    def configure(self,config,**kwargs):
        self._config = config

        content = None

        if isinstance(config,str):
            xml = ElementTree.fromstring(config)
        else:
            xml = config

        if len(kwargs) > 0:
            opts = tools.Options(**kwargs)
            content = opts.resolve(content)

        for node in xml.findall("./event-sources/*"):
            name = ""
            w = ""

            if "name" in node.attrib:
                name = node.get("name")
            if "window" in node.attrib:
                w = node.get("window")

            if len(name) == 0 or len(w) == 0:
                raise Exception("you must specify name and window for each event source")

            type = node.tag
            options = {"name":name,"window":w}
            eventsource = None

            if type == "url-event-source":
                eventsource = UrlEventSource(self,options)
            elif type == "code-event-source":
                eventsource = CodeEventSource(self,options)
            elif type == "csv-event-source":
                eventsource = CsvEventSource(self,name=name,window=w)

            if eventsource == None:
                raise Exception("failed to create event source from \n\n" + xpath.xmlString(node) + "\n")

            self.addEventSource(eventsource)

            eventsource.configure(node)

        for node in xml.findall("./edges/edge"):
            sources = node.get("source").split(" ")
            targets = node.get("target").split(" ")

            for s in sources:
                for t in targets:
                    self.addEdge(s,t)

    def addEventSource(self,eventsource):
        if hasattr(eventsource,"name") == False:
            raise Exception("the event source must have a name property")

        if eventsource.name in self._eventsources:
            raise Exception("event source " + eventsource.name + " already exists")

        self._eventsources[eventsource.name] = eventsource

    def createEventSource(self,**kwargs):
        opts = tools.Options(**kwargs);
        type = opts.getOpt("type","");
        name = opts.getOpt("name","");
        w = opts.getOpt("window","");

        if len(type) == 0 or len(name) == 0 or len(w) == 0:
            raise Exception("you must specify type, name and window for each event source")

        eventsource = None
        options = {"name":name,"window":w}

        if type == "url":
            #eventsource = UrlEventSource(self,name=name,window=w)
            pass
        elif type == "code":
            #eventsource = CodeEventSource(self,name=name,window=w)
            pass
        elif type == "csv":
            eventsource = CsvEventSource(self,name=name,window=w)

        if eventsource == None:
            raise Exception("invalid event source type: " + type)

        eventsource.setOpts(**kwargs)

        self.addEventSource(eventsource);

        return(eventsource);

    def ready(self,conn):
        if self._restart:
            self._restart = False
            self.start()

    def start(self):
        for es in self._eventsources.values():
            es.init()

        self._running = True

        threading.Timer(1,self.run).start()

    def run(self):
        while self._running:

            if self._paused:
                while True:
                    time.sleep(1)
                    if self._paused == False:
                        break

            current = datetime.datetime.now()
            eventsources = []
            depsPending = False
            minInterval = 5000
            completed = 0

            for es in self._eventsources.values():
                if es.repeat >= 0 and es.done == False:
                    if es.sending == False:
                        if es.checkDependencies():
                            diff = current.timestamp() - es.timestamp
                            interval = es.interval

                            if diff > interval:
                                if interval < minInterval:
                                    minInterval = interval
                                eventsources.append(es)
                            else:
                                diff = current.getTime() - es.timestamp + interval

                                if diff < minInterval:
                                    minInterval = diff
                        else:
                            depsPending = True
                else:
                    completed += 1

            for es in eventsources:
                if es.repeat >= 0:
                    es.process()

            if completed == len(self._eventsources.values()):
                self._running = False
            else:
                if depsPending:
                    interval = 1
                else:
                    interval = minInterval / 1000

                time.sleep(interval)

        if tools.supports(self._delegate,"complete"):
            threading.Timer(1,self._delegate.complete(self)).start()

    def togglePlay(self):

        if self._running == False:
            self.start()
            self._paused = False
        elif self._paused:
            self._paused = False
        else:
            self._paused = True

        return(self._paused == False)

    @property
    def connect(self):
        return(self._connection)

    @property
    def running(self):
        return(self._running)

    @property
    def configuration(self):
        return(self._config)

    @property
    def paused(self):
        return(self._paused)

    @paused.setter
    def paused(self,value):
        self._paused = value

        if self._paused == False:
            if self._running == False:
                self.start()

class EventSource(tools.Options):
    def __init__(self,eventsources,**kwargs):
        tools.Options.__init__(self,**kwargs)

        self._window = self.getOpt("window")

        if self._window == None:
            raise Exception("no window specified")

        self._eventsources = eventsources
        self._connection = eventsources._connection
        self._ready = False
        self._done = False
        self._sources = []

        self._times = 0
        self._timestamp = 0

        self._senders = []

        self._publisher = None

        self._interval = 30000

    def configure(self,config):
        xml = None

        if isinstance(config,str):
            xml = ElementTree.fromstring(config)
        else:
            xml = config

        for node in xml.findall("./options/option"):
            name = node.get("name")
            value = node.text
            self.setOpt(name,value)

            if name == "interval":
                self.interval = value

    def init(self):
        self._times = 0
        self._timestamp = 0
        self.done = False
        self.checkCycles()

        opts = {}
        if self.hasOpt("dateformat"):
            opts["dateformat"] = self.getOpt("dateformat")
        if self._connection.version < 7:
            opts["format"] = "json"
        self._publisher = self._connection.getPublisher(self._window,**opts)
        #self._publisher.addSchemaDelegate(self)

    def checkCycles(self):
        for source in self._sources:
            if source.dependsOn(self):
                raise Exception("cyclical dependency detected on " + source.name + " to " + self.name)
            source.checkCycles()

    def checkDependencies(self):
        code = True

        for source in self._sources:
            if source.done == False:
                code = False
                break

            if source.checkDependencies() == False:
                code = False
                break

        return(code)

    def process(self,**kwargs):
        if self.run(**kwargs):
            self._timestamp = datetime.datetime.now()
            self._times += 1

            if self.repeat > 0 and self._times >= self.repeat:
                self._done = True
 
    def send(self,data):
        #Sender(self,data).run()
        Sender(self,data)

    def run(self,**kwargs):
        return(False)

    @property
    def esp(self):
        return(self._eventsources.connect)

    @property
    def publisher(self):
        return(self.publisher)

    @property
    def repeat(self):
        return(self.getInt("repeat",1))

    @property
    def interval(self):
        return(self._interval)

    @interval.setter
    def interval(self,value):
        a = value.split(" ")
        value = float(a[0])
        if len(a) == 2:
            unit = a[1]
        else:
            unit = "milliseconds"

        if unit == "second" or unit == "seconds":
            value *= 1000
        elif unit == "minute" or unit == "minutes":
            value *= (1000 * 60)
        elif unit == "hour" or unit == "hours":
            value *= (1000 * 60 * 60)

        self._interval = value / 1000

    @property
    def timestamp(self):
        return(self._timestamp)

    @timestamp.setter
    def timestamp(self,value):
        self._timestamp = value

    @property
    def name(self):
        return(self.getOpt("name",""))

    @name.setter
    def name(self,value):
        self.setOpt("name",value)

    @property
    def done(self):
        return(self._done and self.sending == False)

    @done.setter
    def done(self,value):
        self._done = value

    @property
    def sending(self):
        return(len(self._senders) > 0)

class CsvEventSource(EventSource):
    def __init__(self,eventsources,**kwargs):
        EventSource.__init__(self,eventsources,**kwargs)
        self._data = None
        self._filter = None
        self._supplement = None

    def init(self):
        EventSource.init(self)

        if self.hasOpt("csv") == False and self.hasOpt("url") == False:
            raise Exception("you must specify CSV data for the event source with either the csv or url option")

        if self.hasOpt("csv"):
            self._data = self.getOpt("csv")
        else:
            data = None
            url = urlparse(self.getOpt("url"))
            if url.scheme == "file":
                with open(url.netloc) as reader:
                    data = reader.read()
            else:
                response = requests.get(self.getOpt("url"))
                data = response.text

            #self._data = data.split("\n")
            self._data = data

        #if self.hasOpt("filter"):
            #self._filter = Function("o",self.getOpt("filter"))

        #if self.hasOpt("supplement"):
            #self._supplement = Function("o",self.getOpt("supplement"))

    def run(self,**kwargs):
        code = False

        if self._publisher.schema.size > 0:
            if self._data != None:
                code = True

                delegate = {}

                if self._filter != None:
                    delegate["filter"] = self._filter
                if self._supplement != None:
                    delegate["supplement"] = self._supplement
                opts = self.options.copy()
                opts["delegate"] = delegate
                data = self._publisher._schema.createDataFromCsv(self._data,**opts)
                self.send(data)

        return(code)

class Sender(object):
    def __init__(self,eventsource,data):

        if isinstance(data,list) == False:
            raise Exception("data must be an array")

        self._eventsource = eventsource
        self._data = data
        self._opcode = eventsource.getOpt("opcode","upsert")
        self._delay = eventsource.getInt("delay",0)
        self._chunksize = eventsource.getInt("chunk_size",1)

        tools.addTo(self._eventsource._senders,self)

        thread = threading.Thread(target = self.run)
        thread.daemon = True
        thread.start()

    def run(self):
        if self._eventsource._publisher == None:
            tools.removeFrom(self._eventsource._senders,self)
            return

        index = self._eventsource.getInt("start",0)

        target = self._eventsource.getOpt("maxevents",0)

        if target == 0:
            target = len(self._data)

        if self._delay == 0:
            for o in self._data:
                o.opcode = self._opcode
                self._eventsource._publisher.add(o)

            self._eventsource._publisher.publish()
        else:
            while index < target:
                if self._eventsource._eventsources.paused:
                    while True:
                        time.sleep(1)
                        if self._eventsource._eventsources.paused == False:
                            break
                self._data[index]["opcode"] = self._opcode
                self._eventsource._publisher.add(self._data[index])

                self._eventsource._publisher.publish()

                index += 1

                delay = self._delay / 1000

                time.sleep(delay)

        tools.removeFrom(self._eventsource._senders,self)
