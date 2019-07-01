from xml.etree import ElementTree
import esppy.espapi.api as api
import esppy.espapi.tools as tools
import threading
import logging
import esppy
import time
import six
import re

logging.basicConfig(filename="/tmp/py.log",level=logging.INFO)

class Connection(object):
    def __init__(self,host,port,secure,**kwargs):
        self._host = host
        self._port = port
        self._secure = secure
        self._websocket = None
        self._handshakeComplete = False
        self._headers = None
        self._options = tools.Options(**kwargs)
        self._authorization = None

    def start(self,readyCb = None):
        if (self.isConnected):
            return

        self.clear()

        url = self.getUrl()

        if (url == None):
            raise Exception("invalid url")

        ws = esppy.websocket.WebSocketClient(url,on_message=self.on_message,on_error=self.on_error,on_open=self.on_open,on_close=self.on_close)
        ws.connect()

    def stop(self):
        if (self.isConnected):
            self.clear()
            return(True)
        return(False)

    def restart(self):
        self.clear()
        self.start()

    def send(self,data):
        if self._websocket != None:
            logging.debug("SEND: " + str(data))
            self._websocket.send(str(data))

    def getUrl(self):
        return(None)

    def message(self,message):
        if (self.isHandshakeComplete):
            return

        name = ""
        value = None

        for i in range(0,len(message)):
            c = message[i]
            if (c == '\n'):
                if (len(name) == 0):
                    break
                if (self._headers == None):
                    self._headers = {}
                if (value != None):
                    self._headers[name] = value.strip()
                else:
                    self._headers[name] = ""

                name = ""
                value = None
            elif (value != None):
                value += c
            elif (c == ':'):
                value = ""
            else:
                name += c

        status = self.getHeader("status")

        if (status != None):
            value = int(status)
            if (value == 200):
                self._handshakeComplete = True
                self.handshakeComplete()

                if (tools.supports(self._delegate,"connected")):
                    self._delegate.connected(self)
            elif (value == 401):
                if (self._authorization != None):
                    self._websocket.send(self._authorization)
                elif (tools.supports(self._delegate,"authenticate")):
                    scheme = self.getHeader("www-authenticate")
                    self._delegate.authenticate(self,scheme)

    def close(self):
        pass

    def error(self):
        pass

    def on_open(self,ws):
        self._websocket = ws

    def on_close(self,ws,code,reason):
        self.clear()
        self.closed()

    def on_error(self,ws,error):
        self.clear()
        self.error()

    def on_message(self,ws,message):
        self.message(message)

    def setOption(self,name,value):
        self._options.set(name,value)

    def getOption(self,name,dv = None):
        return(self._options.get(name,dv))

    def setAuthorization(self,value):
        self._authorization = value

        if self.isConnected and self.isHandshakeComplete == False:
            self._websocket.send(self._authorization)

    def clear(self):
        if (self._websocket != None):
            self._websocket.close()
            self._websocket = None

        self._handshakeComplete = False
        self._headers = None

    def getHeader(self,name):
        value = None
        if (self._headers != None):
            value = self._headers[name]
        return(value)

    def getHost(self):
        return(self._host)

    def getPort(self):
        return(self._port)

    def getProtocol(self):
        if self._secure:
            return("wss")
        else:
            return("ws")

    def isSecure(self):
        return(self._secure)

    def handshakeComplete(self):
        pass

    @property
    def isConnected(self):
        return(self._websocket != None)

    @property
    def isHandshakeComplete(self):
        return(self._handshakeComplete)

class ServerConnection(Connection):

    _windowClasses = {
        "source":"input",
        "filter":"transformation",
        "aggregate":"transformation",
        "compute":"transformation",
        "union":"transformation",
        "join":"transformation",
        "copy":"transformation",
        "functional":"transformation",
        "notification":"utility",
        "pattern":"utility",
        "counter":"utility",
        "geofence":"utility",
        "procedural":"utility",
        "model-supervisor":"analytics",
        "model-reader":"analytics",
        "train":"analytics",
        "calculate":"analytics",
        "score":"analytics",
        "text-context":"textanalytics",
        "text-category":"textanalytics",
        "text-sentiment":"textanalytics",
        "text-topic":"textanalytics"
    }

    def __init__(self,host,port,secure,delegate,**kwargs):
        Connection.__init__(self,host,port,secure,**kwargs)
        self._delegate = delegate
        self._collections = {}
        self._streams = {}
        self._stats = Stats(self)
        self._log = Log(self)
        self._modelDelegates = {};
        self._autoReconnect = True

    def message(self,message):
        if self.isHandshakeComplete == False:
            Connection.message(self,message)
            return

        xml = ElementTree.fromstring(str(message))

        #logging.info("MSG: " + message)

        datasource = None

        if "stream" in xml.attrib:
            id = xml.get("stream")
            if id in self._streams:
                datasource = self._streams[id]
        elif "collection" in xml.attrib:
            id = xml.get("collection")
            if id in self._collections:
                datasource = self._collections[id]

        if xml.tag == "events":
            if datasource != None:
                datasource.events(xml)
        elif xml.tag == "info":
            if datasource != None:
                datasource.info(xml)
        elif xml.tag == "schema":
            if datasource != None:
                datasource.setSchema(xml)
        elif xml.tag == "stats":
            self._stats.process(xml)
        elif xml.tag == "log":
            self._log.process(xml)
        elif (xml.tag == "model"):
            if "id" in xml.attrib:
                id = xml.get("id");

                if id in self._modelDelegates:
                    delegate = self._modelDelegates[id];
                    delegate.deliver(xml);
                    del self._modelDelegates[id];
        else:
            print("THE MSG: " + message)

    def getUrl(self):
        url = ""
        url += self.getProtocol()
        url += "://"
        url += self.getHost()
        url += ":"
        url += self.getPort()
        url += "/eventStreamProcessing/v2/connect"
        return(url)

    def getEventCollection(self,path,**kwargs):
        ec = EventCollection(self,path,**kwargs)
        self._collections[ec._id] = ec
        if self.isHandshakeComplete:
            ec.open()
        return(ec)

    def getEventStream(self,path,**kwargs):
        es = EventStream(self,path,**kwargs)
        self._streams[es._id] = es
        if self.isHandshakeComplete:
            es.open()
        return(es)

    def getStats(self):
        return(self._stats)

    def getLog(self):
        return(self._log)

    def loadModel(self,delegate):
        if tools.supports(delegate,"modelLoaded") == False:
            raise Exception("The stats delegate must implement the modelLoaded method")

        id = tools.guid()
        self._modelDelegates[id] = ModelDelegate(self,delegate)

        o = {}
        o["request"] = "model"
        o["id"] = id
        o["schema"] = True
        o["index"] = True
        o["xml"] = True

        self.send(o)

    def handshakeComplete(self):

        for c in self._collections.values():
            c.open()

        for s in self._streams.values():
            s.open()

        if tools.supports(self._delegate,"connected"):
            self._delegate.connected(self)

    def closed(self):
        print("server conn closed")

        for c in self._collections.values():
            c.clear()

        for s in self._streams.values():
            s.clear()

        if tools.supports(self._delegate,"closed"):
            self._delegate.closed(self)

        if self._autoReconnect:
            thread = threading.Thread(target = self.reconnect)
            thread.daemon = True
            thread.start()

    def reconnect(self):
        while self.isConnected == False:
            #time.sleep(1)
            time.sleep(30)
            try:
                self.start()
            except:
                pass

class Datasource(object):
    def __init__(self,conn,path,**kwargs):
        self._conn = conn
        self._id = tools.guid()
        self._path = path
        self._fields = None
        self._keyFields = None
        self._schema = Schema()
        self._delegates = []
        self._options = tools.Options(**kwargs)

    def setSchema(self,xml):
        self._schema.fromXml(xml)
        for d in self._delegates:
            if tools.supports(d,"schemaSet"):
                d.schemaSet(self)

    def setFilter(self,value):
        self._options.set("filter",value)
        self.set()

    def getFilter(self):
        return(self._options.get("filter",""))

    def getKey(self,o):
        key = ""

        #for j in range(0,len(self._schema._keyFields)):
            #f = self._schema._keyFields[j]
        for f in self._schema._keyFields:
            try:
                value = o[f["name"]]
                if len(key) > 0:
                    key += "-"
                key += value
            except KeyError:
                key = None
                break

        return(key)

    def getData(self):
        return({})

    def getInfo(self):
        return({})

    def getValues(self,names):
        return(None)

    def addDelegate(self,delegate):
        if tools.supports(delegate,"dataChanged") == False:
            raise Exception("the delegate must implement the dataChanged method")

        tools.addTo(self._delegates,delegate)

    def removeDelegate(self,delegate):
        tools.removeFrom(self._delegates,delegate)

    def clear(self):
        pass

    def deliverDataChange(self):
        for d in self._delegates:
            d.dataChanged(self)

    def deliverInfoChange(self):
        for d in self._delegates:
            if tools.supports(d,"infoChanged"):
                d.infoChanged(self)

    def handleMessage(self,msg):
        pass

    def events(self,xml):
        pass

    def info(self,xml):
        pass

    @property
    def schema(self):
        return(self._schema)

class EventCollection(Datasource):
    def __init__(self,conn,path,**kwargs):
        Datasource.__init__(self,conn,path,**kwargs)
        self._page = 0
        self._pages = 0
        self._events = {}

    def open(self):
        o = {}
        o["request"]= "event-collection"
        o["id"]= self._id
        o["action"]= "open"
        o["window"]= self._path
        o["schema"]= True
        o["info"]= 5
        o["format"]= "xml"

        for key,value in self._options.options.items():
            o[key] = value

        if self._options.has("filter") == False:
            o["filter"]= ""

        self._conn.send(o)

    def set(self):
        o = {}
        o["request"]= "event-collection"
        o["id"]= self._id
        o["action"]= "set"

        for key,value in self._options.options.items():
            o[key] = value

        if self._options.has("filter") == False:
            o["filter"]= ""

        self._conn.send(o)

    def close(self):
        o = {}
        o["request"] = "event-collection"
        o["id"] = self._id
        o["action"] = "close"
        self._conn.send(o)

    def handleMessage(self,msg):
        self.loadPage(msg["type"])

    def load(self):
        self.loadPage(None)

    def first(self):
        self.loadPage("first")

    def last(self):
        self.loadPage("last")

    def prev(self):
        self.loadPage("prev")

    def next(self):
        self.loadPage("next")

    def loadPage(self,page):
        o = {}
        o["request"] = "event-collection"
        o["id"] = self._id
        o["action"] = "load"
        if page != None:
            o["page"] = page
        self._conn.send(o)

    def events(self,xml):
        data = []

        nodes = xml.findall("event")

        ub = False

        for n in nodes:
            opcode = n.get("opcode")
            if opcode == None:
                opcode = "insert"

            if opcode == "updateblock":
                ub = True
            elif opcode == "delete":
                if ub:
                    ub = False
                    continue

            o = {}
            o["__opcode"] = opcode

            s = n.get("timestamp")

            if s != None:
                o["__timestamp"] = s

            values = n.findall("./*")

            for v in values:
                datatype = v.get("type")
                content = v.text

                if datatype != None:
                    o[v.tag] = "_data://" + datatype + ":" + content
                else:
                    o[v.tag] = content

            o["__key"] = self.getKey(o)
            data.append(o)

        if "page" in xml.attrib:
            self._page = xml.get("page")
            self._pages = xml.get("pages")
            self._events = {}

        self.process(data)

    def info(self,xml):
        if "page" in xml.attrib:
            self._page = xml.get("page")
            self._pages = xml.get("pages")
            self.deliverInfoChange()

    def process(self,events):
        for e in events:
            key = e["__key"]
            if key != None:
                opcode = e["__opcode"]
                if opcode == "delete":
                    if key in self._events:
                        del self._events[key]
                else:
                    o = {}

                    o["__key"] = key

                    for column in self._schema._columns:
                        if column in e:
                            o[column] = e[column]
                    self._events[key] = o

        a = []
        for e in six.itervalues(self._events):
            a.append(e)

        self.deliverDataChange()

    def getData(self):
        return(self._events)

    def getInfo(self):
        info = {}
        info["page"] = self._page
        info["pages"] = self._pages
        return(info)
 
    def getKeyValues(self):
        values = []

        for key,value in self._events.items():
            values.append(key)
        return(values)

    def getKeyTuple(self):
        values = self.getKeyValues()
        return(tuple(values))

    def getValues(self,name):
        values = {}

        fields = []

        f = self._schema.getField(name)

        #if f == None:
            #logging.info("RAISE EXC")
            #raise Exception("field " + name + " is not found")

        values = []

        for key,value in self._events.items():
            if name in value:
                if f["isNumber"]:
                    values.append(float(value[name]))
                else:
                    values.append(value[name])
            elif f["isNumber"]:
                values.append(0.0)
            else:
                values.append("")

        return(values)

    def getTuple(self,name):
        values = self.getValues(name)
        return(tuple(values))

    def getValuesForFields(self,names):
        values = {}

        fields = []

        for n in names:
            f = self._schema.getField(n)
            if f != None:
                fields.append(f)
                values[n] = []

        for key,value in self._events.items():
            for f in fields:
                n = f["name"]
                if n in value:
                    if f["isNumber"]:
                        values[n].append(float(value[n]))
                    else:
                        values[n].append(value[n])
                elif f["isNumber"]:
                    values[n].append(0.0)
                else:
                    values[n].append("")

        return(values)

    def getTuplesForFields(self,names):
        values = self.getValuesForFields(names)

        tuples = {}

        for key,value in values.items():
            v = values[key]
            tuples[key] = tuple(v)

        return(tuples)

    def getTableData(self,values):

        rows = [];
        columns = [];
        cells = [];

        a = [];

        if values != None and len(values) > 0:
            for v in values:
                f = self._schema.getField(v)
                if f != None:
                    a.append(f)
                    columns.append(f["name"])
        else:
            for f in self._schema.fields:
                if f["isKey"] == False:
                    a.append(f)
                    columns.append(f["name"])

        for key,value in self._events.items():
            rows.append(key)
            cell = []
            for f in a:
                cell.append(value[f["name"]])
            cells.append(cell);

        return({"rows":rows,"columns":columns,"cells":cells});

    def clear(self):
        self._events = {}
        #self.deliverDataChange()

class EventStream(Datasource):
    def __init__(self,conn,path,**kwargs):
        Datasource.__init__(self,conn,path,**kwargs)
        self._events = []
        self._counter = 1

    def open(self):
        o = {}
        o["request"] = "event-stream"
        o["id"] = self._id
        o["action"] = "open"
        o["window"] = self._path
        o["schema"] = True
        o["format"] = "xml"

        for n,v in self._options.options.items():
            o[n] = v

        if self._options.has("filter") == False:
            o["filter"]= ""

        self._conn.send(o)

    def set(self):
        o = {}
        o["request"] = "event-stream"
        o["id"] = self._id
        o["action"] = "set"
        o["format"] = "xml"

        for n,v in self._options.options.items():
            o[n] = v

        if self._options.has("filter") == False:
            o["filter"]= ""

        self._conn.send(o)

    def close(self):
        o = {}
        o["request"] = "event-stream"
        o["id"] = self._id
        o["action"] = "close"
        self._conn.send(o)

    def setSchema(self,xml):
        Datasource.setSchema(self,xml)
        for f in self._schema.fields:
            f["isKey"] = False

        f = {}
        f["name"] = "__opcode"
        f["espType"] = "utf8str"
        f["type"] = "string"
        f["isKey"] = False
        f["isNumber"] = False
        f["isDate"] = False
        f["isTime"] = False
        self._schema._fields.insert(0,f)
        self._schema._fieldMap["__opcode"] = f
        self._schema._columns.insert(0,f["name"])

        f = {}
        f["name"] = "__timestamp"
        f["espType"] = "timestamp"
        f["type"] = "date"
        f["isKey"] = False
        f["isNumber"] = True
        f["isDate"] = False
        f["isTime"] = True
        self._schema._fields.insert(0,f)
        self._schema._fieldMap["__timestamp"] = f
        self._schema._columns.insert(0,f["name"])

        f = {}
        f["name"] = "__counter"
        f["espType"] = "utf8str"
        f["type"] = "int"
        f["isKey"] = True
        f["isNumber"] = True
        f["isDate"] = False
        f["isTime"] = False
        self._schema._fields.insert(0,f)
        self._schema._fieldMap["__counter"] = f
        self._schema._columns.insert(0,f["name"])

        self._keyFields = [f]

        self._counter = 1

    def events(self,xml):
        data = []

        nodes = xml.findall("event")

        for n in nodes:
            opcode = n.get("opcode")
            if opcode == None:
                opcode = "insert"

            o = {}
            o["__opcode"] = opcode

            s = n.get("timestamp")

            if s != None:
                o["__timestamp"] = s

            values = n.findall("./*")

            for v in values:
                datatype = v.get("type")
                content = v.text

                if datatype != None:
                    o[v.tag] = "_data://" + datatype + ":" + content
                else:
                    o[v.tag] = content

            o["__key"] = self.getKey(o)
            data.append(o)

        self.process(data)

    def process(self,events):
        for e in events:
            o = {}
            o["__counter"] = self._counter
            self._counter += 1

            for column in self._schema._columns:
                if column in e:
                    o[column] = e[column]

            self._events.append(o)

        maxEvents = self._options.get("maxevents",50)

        diff = len(self._events) - maxEvents

        if diff > 0:
            for i in range(0,diff):
                del self._events[0]
 
        self.deliverDataChange()

    def getData(self):
        return(self._events)
 
    def getKeyValues(self):
        values = []

        for value in self._events:
            values.append(value["__counter"])
        return(values)

    def getKeyTuple(self):
        values = self.getKeyValues()
        return(tuple(values))
 
    def getValues(self,names):
        values = {}

        fields = []

        for n in names:
            f = self._schema.getField(n)
            if f != None:
                fields.append(f)
                values[n] = []

        for value in self._events:
            for f in fields:
                n = f["name"]
                if n in value:
                    if f["isNumber"]:
                        values[n].append(float(value[n]))
                    else:
                        values[n].append(value[n])
                elif f["isNumber"]:
                    values[n].append(0.0)
                else:
                    values[n].append("")

        return(values)

    def getTuples(self,names):
        tuples = self.getValues(names)

        for key,value in tuples.items():
            v = tuples[key]
            tuples[key] = tuple(v)

        return(tuples)

    def getTableData(self,values):

        rows = [];
        columns = [];
        cells = [];

        a = [];

        if values != None and len(values) > 0:
            for v in values:
                f = self._schema.getField(v)
                if f != None:
                    a.append(f)
                    columns.append(f["name"])
        else:
            for f in self._schema.fields:
                if f["isKey"] == False:
                    a.append(f)
                    columns.append(f["name"])

        for value in self._events:
            rows.append(value["__counter"])
            cell = []
            for f in a:
                cell.append(value[f["name"]])
            cells.append(cell);

        return({"rows":rows,"columns":columns,"cells":cells});

    def clear(self):
        self._events = []
        self.deliverDataChange()

class Stats(object):
    def __init__(self,connection):
        self._connection = connection
        self._delegates = []
        self._options = tools.Options()
        self._data = {}

    def process(self,xml):
        projects = xml.findall(".//project")

        self._data = []

        for p in projects:
            contqueries = p.findall(".//contquery")

            for cq in contqueries:
                windows = cq.findall(".//window")

                for w in windows:
                    o = {}
                    o["project"] = p.get("name")
                    o["contquery"] = cq.get("name")
                    o["window"] = w.get("name")
                    o["cpu"] = float(w.get("cpu"))
                    o["interval"] = float(w.get("interval"))
                    o["count"] = w.get("count") != None and float(w.get("count")) or 0
                    o["__key"] = o["project"] + "." + o["contquery"] + "." + o["window"]
                    self._data.append(o)

        for d in self._delegates:
            d.handleStats(self)

    def setOptions(self,**kwargs):
        self._options.setOptions(**kwargs)
        if len(self._delegates) > 0:
            self.set()

    def setOption(self,name,value):
        self._options.set(name,value)
        if len(self._delegates) > 0:
            self.set()

    def getOption(self,name,dv):
        return(self._options.get(name,dv))

    def set(self):
        o = {}
        o["request"] = "stats"
        o["action"] = "set"
        o["interval"] = self.getOption("interval",1)

        o["minCpu"] = self.getOption("cpu",5)
        o["counts"] = self.getOption("counts",False)
        o["config"] = self.getOption("config",False)
        o["memory"] = self.getOption("memory",False)
        self._connection.send(o)

    def stop(self):
        o = {}
        o["request"] = "stats"
        o["action"] = "stop"
        self._connection.send(o)

    def addDelegate(self,delegate):
        if tools.supports(delegate,"handleStats") == False:
            raise Exception("the delegate must implement the handleStats method")

        if tools.addTo(self._delegates,delegate):
            if len(self._delegates) == 1:
                self.set()

    def removeDelegate(self,delegate):
        if tools.removeFrom(self,_delegates,delegate):
            if len(self._delegates) == 0:
                self.stop()

    def getData(self):
        return(self._data)

class Log(object):
    def __init__(self,connection):
        self._connection = connection
        self._delegates = []

    def process(self,xml):
        nodes = xml.findall(".")
        if len(nodes) > 0:
            message = nodes[0].text
        for d in self._delegates:
            d.handleLog(self,message)

    def start(self):
        o = {}
        o["request"] = "logs"
        o["capture"] = True
        self._connection.send(o)

    def stop(self):
        o = {}
        o["request"] = "logs"
        o["capture"] = False
        self._connection.send(o)

    def addDelegate(self,delegate):
        if tools.supports(delegate,"handleLog") == False:
            raise Exception("the delegate must implement the handleLog method")

        if tools.addTo(self._delegates,delegate):
            if len(self._delegates) == 1:
                self.start()

    def removeDelegate(self,delegate):
        if tools.removeFrom(self._delegates,delegate) == True:
            if len(self._delegates.length) == 0:
                self.stop()

class Model(object):

    def __init__(self,xml):
        self._projects = []
        self._contqueries = []
        self._windows = []
        self._sourceWindows = []

        projects = xml.findall(".//project")

        for xml in projects:
            p = xml.get("name")

            project = {}
            project["xml"] = xml
            project["key"] = p
            project["name"] = p
            project["index"] = xml.get("index")

            self._projects.append(project)

            project["_contqueries"] = []

            contqueries = xml.findall(".//contquery")

            for cqXml in contqueries:

                cq = cqXml.get("name")
                contquery = {}
                contquery["name"] = cq
                contquery["key"] = p + "/" + cq
                contquery["index"] = cqXml.get("index")
                project["_contqueries"].append(contquery)
                self._contqueries.append(contquery)

                contquery["windows"] = []
                contquery["edges"] = []

                windows = cqXml.findall(".//windows/*")

                if len(windows) == 0:
                    continue

                for windowXml in windows:
                    win = self.addWindow(project,contquery,windowXml)
                    contquery["windows"].append(win)

                edges = cqXml.findall("./edges/edge")

                for edge in edges:
                    sources = edge.get("source").split(" ")
                    targets = edge.get("target").split(" ")

                    for source in sources:
                        source = source.strip()
                        a = p + "/" + cq + "/" + source
                        aw = self.getWindow(a)
                        if aw == None:
                            continue
                        for target in targets:
                            target = target.strip()

                            if len(target) > 0:
                                z = p + "/" + cq + "/" + target
                                zw = self.getWindow(z)

                                if zw != None:
                                    aw["outgoing"].append(zw)
                                    zw["incoming"].append(aw)
                                    contquery["edges"].append({"a":source,"z":target})

    #print(self._projects)

    def getWindow(self,key):
        return(self.get(key,self._windows))

    def get(self,key,a):
        for i in a:
            if i["key"] == key:
                return(i)
        return(None)

    def addWindow(self,project,contquery,xml):
        name = xml.get("name")
        type = xml.get("type")

        if type == None or len(type) == 0:
            type = xml.tag

        a = type.split("-")

        if len(a) > 1:
            type = ""

            for i in range(1,len(a)):
                if len(type) > 0:
                    type += "-"
                type += a[i]

        win = {}
        win["p"] = project["name"]
        win["cq"] = contquery["name"]
        win["name"] = name
        win["type"] = type
        win["index"] = xml.get("index")
        win["xml"] = xml

        if win["index"] == None:
            win["index"] = contquery["index"]

            if win["index"] == None:
                win["index"] = project["index"]

                if win["index"] == None:
                    win["index"] = "pi_HASH"

        win["key"] = project["name"] + "/" + contquery["name"] + "/" + name

        schema = Schema()
        schema.fromXml(xml)

        win["schema"] = schema

        win["incoming"] = []
        win["outgoing"] = []
        win["cpu"] = 0.0

        win["class"] = ServerConnection._windowClasses.get(win["type"],"unknown")

        if win["type"] == "window-source":
            self._sourceWindows[win["key"]] = True

        self._windows.append(win)

        return(win)

    @property
    def windows(self):
        return(self._windows)

class Schema(object):
    def __init__(self):
        self._fields = []
        self._fieldMap = {}
        self._keyFields = []
        self._columns = []

    def fromWindow(self,window):
        self._fields = []
        self._fieldMap = {}
        self._keyFields = []
        self._columns = []

        for name,value in window.schema.items():
            o = {}
            o["name"] = name
            o["espType"] = value.type
            o["isNumber"] = False
            o["isTime"] = False
            o["isDate"] = False

            if o["espType"] == "utf8str":
                o["type"] = "string"
            elif o["espType"] == "int32" or o["espType"] == "int64":
                o["type"] = "int"
                o["isNumber"] = True
            elif o["espType"] == "double" or o["espType"] == "money":
                o["type"] = "float"
                o["isNumber"] = True
            elif o["espType"] == "date":
                o["type"] = "date"
                o["isDate"] = True
            elif o["espType"] == "timestamp":
                o["type"] = "datetime"
                o["isTime"] = True
            else:
                o["type"] = o["espType"]

            o["isKey"] = value.key

            self._fields.append(o)
            self._columns.append(name)

            self._fieldMap[name] = o

            if o["isKey"]:
                self._keyFields.append(o)

    def fromXml(self,xml):
        self._fields = []
        self._fieldMap = {}
        self._keyFields = []
        self._columns = []

        if xml == None:
            raise Exception("no schema specified")

        fields = xml.findall(".//fields/field")

        for f in fields:
            o = {}

            name = f.get("name")
            o["name"] = name
            o["espType"] = f.get("type")
            o["isNumber"] = False
            o["isTime"] = False
            o["isDate"] = False

            if o["espType"] == "utf8str":
                o["type"] = "string"
            elif o["espType"] == "int32" or o["espType"] == "int64":
                o["type"] = "int"
                o["isNumber"] = True
            elif o["espType"] == "double" or o["espType"] == "money":
                o["type"] = "float"
                o["isNumber"] = True
            elif o["espType"] == "date":
                o["type"] = "date"
                o["isDate"] = True
            elif o["espType"] == "timestamp":
                o["type"] = "datetime"
                o["isTime"] = True
            else:
                o["type"] = o["espType"]

            o["isKey"] = (f.get("key") == "true")

            self._fields.append(o)
            self._columns.append(name)

            self._fieldMap[name] = o

            if o["isKey"]:
                self._keyFields.append(o)

    def getField(self,name):
        if name in self._fieldMap:
            return(self._fieldMap[name])
        return(None)

    def getFieldType(self,name):
        type = "string"
        if name in self._fieldMap:
            type = self._fieldMap[name]["type"]
        return(type)

    def isNumericField(self,field):
        code = False
        f = None
        if type(field) is str:
            if field in self._fieldMap:
                f = self._fieldMap[field]
        else:
            f = field

        if f != None:
            code = f["isNumber"]

        return(code)

    def isDateField(self,field):
        code = False
        f = None
        if type(field) is str:
            if field in self._fieldMap:
                f = self._fieldMap[field]
        else:
            f = field

        if f != None:
            code = f["isDate"]

        return(code)

    def isTimeField(self,field):
        code = False
        f = None
        if type(field) is str:
            if field in self._fieldMap:
                f = self._fieldMap[field]
        else:
            f = field

        if f != None:
            code = f["isTime"]

        return(code)

    def toXml(self):
        e = ElementTree.Element("schema")
        for field in self._fields:
            f = ElementTree.SubElement(e,"field")
            f.attrib["name"] = field["name"]
            f.attrib["espType"] = field["espType"]
            f.attrib["type"] = field["type"]
            if field["isKey"]:
                f.attrib["isKey"] = "true"
        return(e)

    def toJson(self):
        e = ElementTree.Element("schema")
        fields = []
        for field in self._fields:
            f = {}
            f["name"] = field["name"]
            f["espType"] = field["espType"]
            f["type"] = field["type"]
            if field["isKey"]:
                f["isKey"] = "true"
            fields.append(f)
        return(fields)

    def __str__(self):
        o = self.toJson()
        return(str(o))

    def toString(self):
        s = ""
        i = 0
        for field in self._fields:
            if field["isKey"] == False:
                continue
            if i > 0:
                s += ","
            s += field["name"]
            s += ":"
            s += field["espType"]
            s += "*"
            i += 1

        for field in self._fields:
            if field["isKey"] == True:
                continue
            if i > 0:
                s += ","
            s += field["name"]
            s += ":"
            s += field["espType"]
            i += 1

        return(s)

    def hasFields(self):
        return(len(self._fields) > 0)

    @property
    def fields(self):
        return(self._fields)

    @property
    def columns(self):
        return(self._columns)

class ModelDelegate(object):

    def __init__(self,connection,delegate):
        self._connection = connection
        self._delegate = delegate

    def deliver(self,xml):
        model = Model(xml)

        if tools.supports(self._delegate,"modelLoaded"):
            self._delegate.modelLoaded(model,self._connection)
