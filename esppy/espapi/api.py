from xml.etree.ElementTree import Element, SubElement
from xml.etree import ElementTree
import json
import pandas as pd
import six
import re
import esppy.websocket
import esppy

class Server(object):

    def __init__(self,esp,delegate = None):
        self._esp = esp
        self._delegate = delegate
        self._model = None
        s = re.findall("^\w+:",self._esp.base_url)

        if len(s) > 0:
            wsproto = (s[0] == "https:") and "wss:" or "ws:"
            self._wsBaseUrl = re.sub(r"^\w+:",wsproto,self._esp.base_url)

        self._windowClasses = {}

        self._windowClasses["source"] = "input";

        self._windowClasses["filter"] = "transformation";
        self._windowClasses["aggregate"] = "transformation";
        self._windowClasses["compute"] = "transformation";
        self._windowClasses["union"] = "transformation";
        self._windowClasses["join"] = "transformation";
        self._windowClasses["copy"] = "transformation";
        self._windowClasses["functional"] = "transformation";

        self._windowClasses["notification"] = "utility";
        self._windowClasses["pattern"] = "utility";
        self._windowClasses["counter"] = "utility";
        self._windowClasses["geofence"] = "utility";
        self._windowClasses["procedural"] = "utility";

        self._windowClasses["model-supervisor"] = "analytics";
        self._windowClasses["model-reader"] = "analytics";
        self._windowClasses["train"] = "analytics";
        self._windowClasses["calculate"] = "analytics";
        self._windowClasses["score"] = "analytics";

        self._windowClasses["text-context"] = "textanalytics";
        self._windowClasses["text-category"] = "textanalytics";
        self._windowClasses["text-sentiment"] = "textanalytics";
        self._windowClasses["text-topic"] = "textanalytics";

        self.build()

    def build(self):
        self._projects = []
        self._contqueries = []
        self._windows = []
        self._sourceWindows = []

        for s in self._esp.get_projects():
            xml = ElementTree.fromstring(self._esp.get_project(s).to_xml())

            projects = xml.findall(".//project")

            for projectXml in projects:
                p = projectXml.get("name")

                project = {}
                project["xml"] = projectXml
                project["key"] = p
                project["name"] = p
                project["index"] = projectXml.get("index")

                self._projects.append(project)

                project["_contqueries"] = []

                contqueries = projectXml.findall(".//contquery")

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

        win["class"] = self._windowClasses.get(win["type"],"unknown")

        if win["type"] == "window-source":
            self._sourceWindows[win["key"]] = True

        self._windows.append(win)

        return(win)

    def getStreamingDatasource(self,window,**kwargs):
        return(StreamingDatasource(self,window,**kwargs))

    def getUpdatingDatasource(self,window,**kwargs):
        return(UpdatingDatasource(self,window,**kwargs))

    def getStats(self,cpu = 0,interval = 5,counts = True,delegate = None):
        return(ProjectStats(self,cpu,interval,counts,delegate))

    def getLogs(self,delegate = None):
        return(Logs(self,delegate))

    @property
    def esp(self):
        return(self._esp)

    @property
    def windows(self):
        return(self._windows)

    @property
    def wsBaseUrl(self):
        return(self._wsBaseUrl)

class Datasource(object):
    def __init__(self,server):
        self._server = server
        self._type = None
        self._schema = Schema()
        self._changeDelegates = []

    def getData(self):
        return({})

    def getInfo(self):
        return({})

    def addChangeDelegate(self,delegate):
        exists = False
        for d in self._changeDelegates:
            if d == delegate:
                exists = True
                break
        if exists == False:
            self._changeDelegates.append(delegate)

    def removeChangeDelegate(self,delegate):
        self._changeDelegates.remove(delegate)

    def deliverDataChange(self):
        for delegate in self._changeDelegates:
            if Delegate.supports(delegate,"dataChanged"):
                delegate.dataChanged(self)

    def deliverInfoChange(self):
        for delegate in self._changeDelegates:
            if Delegate.supports(delegate,"infoChanged"):
                delegate.infoChanged(self)

    @property
    def server(self):
        return(self._server)

    @property
    def schema(self):
        return(self._schema)

    @property
    def type(self):
        return(self._type)

    @type.setter
    def type(self,value):
        self._type = value

class Connection(Datasource):
    def __init__(self,server,delegate = None):
        Datasource.__init__(self,server)
        self._delegate = delegate
        self._websocket = None
        self._connected = False
        self._handshakeComplete = False
        self._headers = None
        self._options = {}

    def start(self):
        if (self.isConnected):
            return False

        self.clear()

        url = self.getUrl()

        if (url == None):
            return(False)

        self._connected = False

        self.getWebSocket(url)

        return(True)

    def getUrl(self):
        return(None)

    def getWebSocket(self,url):
        if (self._websocket == None):
            self._websocket = esppy.websocket.WebSocketClient(url,on_message=self.on_message,on_error=self.on_error,on_open=self.on_open,on_close=self.on_close)
            self._websocket.connect()

        return(self._websocket)

    def message(self,message):
        if (self._handshakeComplete):
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
                self.ready()

                if (self.supports("connected")):
                    self._delegate.connected(self)
            elif (value == 401):
                if (self._server._authorization != None):
                    self._websocket.send(self._server._authorization)
                elif (Delegate.supports(self._server._delegate,"authenticate")):
                    scheme = self.getHeader("www-authenticate")
                    self._server._delegate.authenticate(self._server,scheme)

    def on_open(self,ws):
        self._connected = True
        if (self.supports("open")):
            self._delegate.open(self)

    def on_close(self,ws,code,reason):
        self.clear()
        if (self.supports("close")):
            self._delegate.close(self)

    def on_error(self,ws,error):
        self.clear()
        if (self.supports("error")):
            self._delegate.error(self)

    def on_message(self,ws,message):
        self.message(message)

    def ready(self):
        pass

    def setOption(self,name,value):
        s = name.lower()

        if value != None:
            self._options[s] = value
        else:
            del self._options[s]

    def getOption(self,name,dv = None):
        s = name.lower()

        value = None

        if hasattr(self._options,s):
            value = self._options[s]
        elif dv != None:
            value = dv

        return(value)

    def clear(self):
        if (self._websocket != None):
            self._websocket.close()
            self._websocket = None

        self._connected = False
        self._handshakeComplete = False
        self._headers = None

    def getHeader(self,name):
        value = None
        if (self._headers != None):
            value = self._headers[name]
        return(value)

    def supports(self,name):
        code = False
        if (self._delegate != None):
            code = Delegate.supports(self._delegate,name)
        return(code)

    @property
    def isConnected(self):
        return(self._websocket != None)

    @property
    def handshakeComplete(self):
        return(self._handshakeComplete)

class PubSub(Connection):

    def __init__(self,server,window,delegate = None):
        Connection.__init__(self,server,delegate)

        self._name = ""

        a = window.split("/")

        if len(a) != 3:
            raise Exception("must supply project/contquery/window")

        p = self._server._esp.get_project(a[0])
        self._window = p.get_window(a[1] + "/" + a[2])
        self._schema.fromWindow(self._window)

    @property
    def name(self):
        return(self._name)

    @name.setter
    def name(self,value):
        self._name = value

    def getKey(self,o):
        key = ""

        for j in range(0,len(self.schema._keyFields)):
            f = self.schema._keyFields[j]
            try:
                value = o[f["name"]]
                if len(key) > 0:
                    key += "-"
                key += value
            except KeyError:
                key = None
                break

        return(key)

    def start(self):
        Connection.start(self)

    def getUrl(self):
        url = self._window.subscriber_url
        url += "?"
        url += "format=xml"
        url += "&info=5"
        return(url)

    def setSchema(self,xml):
        self._schema.fromXml(xml)

    def clear(self):
        Connection.clear(self)

class Subscriber(PubSub):

    def __init__(self,server,window,mode,delegate = None,**kwargs):
        PubSub.__init__(self,server,window,delegate)
        self.type = mode
        self._page = 0
        self._pages = 0
        self._pageSize = 50
        self._filter = None
        self._sort = None
        self._args = kwargs != None and kwargs.copy() or None

    @property
    def isUpdating(self):
        return(self.type == "updating")

    @property
    def isStreaming(self):
        return(self.type == "streaming")

    def getUrl(self):
        url = PubSub.getUrl(self)
        url += "&mode=" + self.type
        if self._args != None:
            for key,value in self._args.items():
                url += "&" + key + "=" + str(value)
        return(url)

    def message(self,message):
        if (self.handshakeComplete == False):
            PubSub.message(self,message)
            return
            
        xml = ElementTree.fromstring(str(message))

        if xml.tag == "schema":
            self.setSchema(xml)
            if self.supports("schema"):
                self._delegate.schema(self)

        elif xml.tag == "events":
            isPage = False

            s = xml.get("page")

            if s != None:
                self._page = int(s)
                isPage = True

            s = xml.get("pages")

            if s != None:
                self._pages = int(s)

            a = []

            nodes = xml.findall("event")

            ub = False

            updating = self.isUpdating

            for i in range(0,len(nodes)):
                e = nodes[i]
                opcode = e.get("opcode")
                if opcode == None:
                    opcode = "insert"

                if updating:
                    if opcode == "updateblock":
                        ub = True
                    elif opcode == "delete":
                        if ub:
                            ub = False
                            continue

                o = {}
                o["__opcode"] = opcode

                s = e.get("timestamp")

                if s != None:
                    o["__timestamp"] = s

                values = e.findall("./*")

                for j in range(0,len(values)):
                    datatype = values[j].get("type")
                    content = values[j].text

                    if datatype != None:
                        o[values[j].tag] = "_data://" + datatype + ":" + content
                    else:
                        o[values[j].tag] = content

                o["__key"] = self.getKey(o)
                a.append(o)

            if isPage:
                if self.supports("page"):
                    self._delegate.page(self,a,self._page,self._pages)
            elif self.supports("events"):
                self._delegate.events(self,a)

        elif xml.tag == "info":

            s = xml.get("pages")

            if s != None:
                self._pages = int(s)

            if self.supports("pages"):
                self._delegate.pages(self,self._page,self._pages)

        elif xml.tag == "error":

            if self.supports("error"):
                node = xml.findall("text")

                if node != None:
                    self._delegate.error(self,node.text)

    def connected(self,connection):
        self.load()

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
        if (self.ready):
            s = ""
            s += "<load"

            if (page != None):
                s += " page='" + str(page) + "'"

            s += " pagesize='" + str(self._pageSize) + "'"

            if (self._sort != None):
                if "field" in self._sort:
                    s += " sort='"
                    s += self._sort["field"]
                    direction = "descending"
                    if "direction" in self._sort:
                        s += self._sort.direction
                        direction = self._sort["direction"]
                    s += (":" + direction)
                    s += "'"
            s += ">"

            filter = ""

            if (self._filter != None):
                filter = self._filter

            s += "<filter>"
            s += filter
            s += "</filter>"

            s += "</load>"

            self._websocket.send(s)

    @property
    def pageSize(self):
        return(self._pageSize)

    @pageSize.setter
    def pageSize(self,value):
        if self._pageSize != value:
            self._pageSize = value

            if self.ready:
                self.load()
            else:
                self._sendProperties = true

    @property
    def filter(self):
        return(self._filter)

    @filter.setter
    def filter(self,value):
        #if self._filter != value:
        if True:
            self._filter = value

            if self.ready:
                self.load()
            else:
                self._sendProperties = true

    @property
    def sort(self):
        return(self._sort)

    @sort.setter
    def sort(self,value):
        self._sort = value;

class EventCollection(Subscriber):
    def __init__(self,server,window,type,delegate = None,**kwargs):
        Subscriber.__init__(self,server,window,type,self,**kwargs)
        if self.isUpdating:
            self._events = {}
        else:
            self._events = []
        self._df = pd.DataFrame(columns=self._schema.columns)

    def frameData(self,initial,max_data,terminate):
        data = self._df
        return(data)

    def getData(self):
        return(self._events)

    def getInfo(self):
        return({})

    def handleMessage(self,msg):
        type = msg["type"]
        if type == "next":
            self.next()
        elif type == "prev":
            self.prev()
        elif type == "first":
            self.first()
        elif type == "last":
            self.last()

    @property
    def data(self):
        return(self._df)

class UpdatingDatasource(EventCollection):

    def __init__(self,server,window,**kwargs):
        EventCollection.__init__(self,server,window,"updating",self,**kwargs)
        self.start()

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

                    for column in self.schema._columns:
                        if column in e:
                            o[column] = e[column]
                    self._events[key] = o

        a = []
        for e in six.itervalues(self._events):
            a.append(e)
        self._df = pd.DataFrame(data=a,columns=self.schema._columns)

        if Delegate.supports(self._delegate,"collectionChanged"):
            self._delegate.collectionChanged(self)

        self.deliverDataChange()
 
    def events(self,subscriber,events):
        self.process(events)

    def page(self,subscriber,events,page,pages):
        self._events = {}
        self.process(events)

    def pages(self,subscriber,page,pages):
        self.deliverInfoChange()

    def getInfo(self):
        info = {}
        info["page"] = self._page
        info["pages"] = self._pages
        return(info)

class StreamingDatasource(EventCollection):

    def __init__(self,server,window,**kwargs):
        EventCollection.__init__(self,server,window,"streaming",self,**kwargs)

        for f in self._schema._fields:
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

        self.start()

    def process(self,events):
        for e in events:
            o = {}
            o["__counter"] = self._counter
            self._counter += 1

            for column in self._schema._columns:
                if column in e:
                    o[column] = e[column]

            self._events.append(o)

        diff = len(self._events) - self.pageSize

        if diff > 0:
            for i in range(0,diff):
                del self._events[0]
 
        self.deliverDataChange()

    def events(self,subscriber,events):
        self.process(events)

    def page(self,subscriber,events,page,pages):
        self._events = {}
        self.process(events)

    def getInfo(self):
        info = {}
        info["page"] = self._page
        info["pages"] = self._pages
        return(info)

class ProjectStats(Connection):

    def __init__(self,server,cpu = 0,interval = 10,counts = True,delegate = None):
        Connection.__init__(self,server,delegate)
        self._cpu = float(cpu)
        self._interval = int(interval)
        self._counts = counts

        self._fields = ["__key", "project", "contquery", "window", "cpu", "interval", "count"]
        self._df = pd.DataFrame(columns = self._fields)

        self._data = {}

    def getUrl(self):
        url = self._server.wsBaseUrl + "projectStats"
        url += "?interval="
        url += str(self._interval)
        url += "&minCpu="
        url += str(self._cpu)
        if self._counts:
            url += "&counts=true"
        return(url)

    def frameData(self,initial,max_data,terminate):
        data = self._df
        return(data)

    def getData(self):
        return(self._data)

    def message(self,message):
        if self._handshakeComplete == False:
            Connection.message(self,message)
            return

        xml = ElementTree.fromstring(str(message))

        projects = xml.findall(".//project")

        self._data = []

        for p in projects:
            contqueries = p.findall(".//contquery")

            for cq in contqueries:
                windows = cq.findall(".//window")

                for w in windows:
                    cpu = float(w.get("cpu"))

                    if cpu < self._cpu:
                        continue

                    o = {}
                    o["project"] = p.get("name")
                    o["contquery"] = cq.get("name")
                    o["window"] = w.get("name")
                    o["cpu"] = float(w.get("cpu"))
                    o["interval"] = float(w.get("interval"))
                    o["count"] = w.get("count") != None and float(w.get("count")) or 0
                    o["__key"] = o["project"] + "." + o["contquery"] + "." + o["window"]
                    self._data.append(o)

        self._df = pd.DataFrame(data = self._data,columns = self._fields)
        self.deliverDataChange()

class Logs(Connection):

    def __init__(self,server,delegate = None):
        Connection.__init__(self,server,delegate)

    def getUrl(self):
        return(self._server.wsBaseUrl + "logs")

    def message(self,message):
        if self._handshakeComplete == False:
            Connection.message(self,message)
            return

        data = str(message)

        if Delegate.supports(self._delegate,"handleLog"):
            self._delegate.handleLog(data)

class Schema(object):
    def __init__(self):
        self._fields = []
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
        return(e);

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

    @property
    def fields(self):
        return(self._fields)

    @property
    def columns(self):
        return(self._columns)

class Delegate(object):

    @staticmethod
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

class Options(object):
    def __init__(self,**kwargs):
        self._options = {}

        if kwargs != None:
            for name,value in kwargs.items():
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

    @property
    def options(self):
        return(self._options)

    @options.setter
    def options(self,options):
        for name,value in six.iteritems(options):
            self.set(name,value)
