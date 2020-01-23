from xml.etree import ElementTree
import pandas as pd
import esppy.espapi.tools as tools
import threading
import logging
import esppy
import time
import six
import re
import os

if os.getenv("ESPPY_LOG") != None:
    logging.basicConfig(filename=os.getenv("ESPPY_LOG"),level=logging.INFO)

class Connection(tools.Options):
    def __init__(self,host,port,secure,**kwargs):
        tools.Options.__init__(self,**kwargs)
        self._host = host
        self._port = port
        self._secure = secure
        self._websocket = None
        self._handshakeComplete = False
        self._headers = None
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
            #logging.info("SEND: " + str(data))
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

    def closed(self):
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
            if name in self._headers:
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

    def getHttpProtocol(self):
        if self._secure:
            return("https")
        else:
            return("http")

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
        self._datasources = {}
        self._publishers = {}
        self._stats = Stats(self)
        self._log = Log(self)
        self._modelDelegates = {}
        self._urlPublishers = {}
        self._autoReconnect = True

    def message(self,message):
        if self.isHandshakeComplete == False:
            Connection.message(self,message)
            return

        #logging.info("MSG: " + message)

        xml = ElementTree.fromstring(str(message))

        if xml.tag == "events" or xml.tag == "schema" or xml.tag == "info":

            datasource = None

            if "id" in xml.attrib:
                id = xml.get("id")
                if id in self._datasources:
                    datasource = self._datasources[id]
            if datasource != None:
                if xml.tag == "events":
                    datasource.events(xml)
                elif xml.tag == "schema":
                    datasource.setSchema(xml)
                elif xml.tag == "info":
                    datasource.info(xml)
        elif xml.tag == "stats":
            self._stats.process(xml)
        elif xml.tag == "log":
            self._log.process(xml)
        elif (xml.tag == "model"):
            if "id" in xml.attrib:
                id = xml.get("id")

                if id in self._modelDelegates:
                    delegate = self._modelDelegates[id]
                    delegate.deliver(xml)
                    del self._modelDelegates[id]
        elif (xml.tag == "url-publisher"):
            if "id" in xml.attrib:
                id = xml.get("id")
                if id in self._urlPublishers:
                    publisher = self._urlPublishers[id]
                    if "complete" in xml.attrib:
                        complete = xml.get("complete")
                        publisher["complete"] = (complete == "true")
                        del self._urlPublishers[id]

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
        self._datasources[ec._id] = ec
        if self.isHandshakeComplete:
            ec.open()
        return(ec)

    def getEventStream(self,path,**kwargs):
        es = EventStream(self,path,**kwargs)
        self._datasources[es._id] = es
        if self.isHandshakeComplete:
            es.open()
        return(es)

    def getPublisher(self,path,**kwargs):
        publisher = Publisher(self,path,**kwargs);
        self._publishers[publisher._id] = publisher;
        if self.isHandshakeComplete:
            publisher.open();
        return(publisher);

    def publishUrl(self,path,url,**kwargs):
        opts = tools.Options(**kwargs)
        blocksize = opts.getOpt("blocksize",1)
        wait = opts.getOpt("wait",False)

        id = tools.guid();

        o = {}
        o["request"] = "url-publisher"
        o["id"] = id
        o["window"] = path;
        o["url"] = url;
        o["blocksize"] = blocksize

        publisher = {"complete":False}
        self._urlPublishers[id] = publisher

        self.send(o)

        if wait:
            while publisher["complete"] == False:
                time.sleep(1)

    def publishDataFrame(self,path,df,**kwargs):
        opts = tools.Options(**kwargs)

        size = opts.getOpt("size",100)
        blocksize = opts.getOpt("blocksize",1)

        id = tools.guid()

        request = {}
        request["request"] = "publisher"
        request["id"] = id
        request["action"] = "set"
        request["window"] = path
        self.send(request)

        request["action"] = "publish"

        data = []

        for index, row in df.iterrows():
            o = {}
            for col in df.columns:
                o[col] = row[col]
            data.append(o)
            if len(data) == size:
                request["data"] = data
                self.send(request)
                data = []

        if len(data) > 0:
            request["data"] = data
            self.send(request)

        request["data"] = None
        request["action"] = "delete"
        self.send(request)

    def publishList(self,path,l,**kwargs):
        opts = tools.Options(**kwargs)

        size = opts.getOpt("size",100)
        blocksize = opts.getOpt("blocksize",1)

        id = tools.guid()

        request = {}
        request["request"] = "publisher"
        request["id"] = id
        request["action"] = "set"
        request["window"] = path
        self.send(request)

        request["action"] = "publish"

        data = []

        for i in l:
            o = {}
            for key,value in i.items():
                o[key] = i[key]
            data.append(o)
            if len(data) == size:
                request["data"] = data
                self.send(request)
                data = []

        if len(data) > 0:
            request["data"] = data
            self.send(request)

        request["data"] = None
        request["action"] = "delete"
        self.send(request)

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

        for c in self._datasources.values():
            c.open()

        for p in self._publishers.values():
            p.open()

        if len(self._log._delegates) > 0:
            self._log.start()

        if len(self._stats._delegates) > 0:
            self._stats.set()

        if tools.supports(self._delegate,"connected"):
            self._delegate.connected(self)

    def closed(self):
        for c in self._datasources.values():
            c.clear()

        if tools.supports(self._delegate,"closed"):
            self._delegate.closed(self)

        if self._autoReconnect:
            thread = threading.Thread(target = self.reconnect)
            thread.daemon = True
            thread.start()

    def reconnect(self):
        logging.info("RECONNECT")
        while self.isConnected == False:
            #time.sleep(5)
            #time.sleep(1)
            time.sleep(300)
            try:
                self.start()
            except:
                pass

class Datasource(tools.Options):
    def __init__(self,connection,**kwargs):
        tools.Options.__init__(self,**kwargs)
        self._connection = connection
        self._id = tools.guid()
        self._fields = None
        self._keyFields = None
        self._schema = Schema()
        self._delegates = []
        self._paused = False
        self._data = None

    def setSchema(self,xml):
        self._schema.fromXml(xml)
        for d in self._delegates:
            if tools.supports(d,"schemaSet"):
                d.schemaSet(self)

    def setFilter(self,value):
        self.setOpt("filter",value)
        self.set()

    def getFilter(self):
        return(self.getOpt("filter",""))

    def togglePlay(self):

        code = False

        if self._paused:
            self.play()
            code = True
        else:
            self.pause()

        return(code)

    def getFields(self):
        fields = None
        if self._schema != None:
            fields = self._schema.getFields()
        return(fields)

    def getKeyFields(self):
        fields = None
        if self._schema != None:
            fields = self._schema.getKeyFields()
        return(fields)

    def getKeyFieldNames(self):
        names = []
        fields = self.getKeyFields()
        if fields != None:
            for f in fields:
                names.append(f["name"])
        return(names)

    def getColumnFields(self):
        fields = None
        if self._schema != None:
            fields = self._schema.getColumnFields()
        return(fields)

    def getField(self,name):
        field = None
        if self._schema != None:
            field = self._schema.getField(name)
        return(field)

    def getKey(self,o):
        key = ""

        for f in self._schema._keyFields:
            name = f["name"]
            if (name in o) == False:
                break
            if len(key) > 0:
                key += "-"
            key += str(o[name])

        return(key)

    def getData(self):
        return(self._data)

    def getList(self):
        if isinstance(self._data,dict):
            l = []
            for k,v in self._data.items():
                l.append(v)
            return(l)
        elif isinstance(self._data,list):
            return(self._data)

    def getValues(self,name):

        f = self._schema.getField(name)

        if f == None:
            return(None)

        values = []

        if isinstance(self._data,dict):
            for key,value in self._data.items():
                if name in value:
                    if f["isNumber"]:
                        values.append(float(value[name]))
                    else:
                        values.append(value[name])
                elif f["isNumber"]:
                    values.append(0.0)
                else:
                    values.append("")
        elif isinstance(self._data,list):
            for value in self._data:
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

    def getValuesBy(self,keys,names,delimiter = "."):
        keyFields = []

        for s in keys:
            f = self._schema.getField(s)
            if f == None:
                raise Exception("field " + s + " not found")
            keyFields.append(f)

        timeKeys = False

        if len(keyFields) == 1:
            if keyFields[0]["isDate"]:
                timeKeys = True
            elif keyFields[0]["isTime"]:
                timeKeys = True

        valueFields = []

        for s in names:
            f = self._schema.getField(s)
            if f == None:
                raise Exception("field " + s + " not found")
            valueFields.append(f)

        items = None

        if isinstance(self._data,dict):
            items = self._data.values()
        elif isinstance(self._data,list):
            items = self._data

        if items == None:
            raise Exception("invalid data")

        data = {}

        for o in items:
            key = ""
            for f in keyFields:
                name = f["name"]
                if name in o:
                    if len(key) > 0:
                        key += delimiter
                    key += str(o[name])

            if key in data:
                entry = data[key]
            else:
                entry = {}
                for f in valueFields:
                    name = f["name"]
                    entry[name] = 0.0
                data[key] = entry

            for f in valueFields:
                if f["isNumber"]:
                    name = f["name"]
                    entry[name] += float(o[name])

        keyValues = []
        values = {}

        for f in valueFields:
            name = f["name"]
            values[name] = []

        for k,v in data.items():
            if timeKeys:
                dt = pd.to_datetime(k,unit="us")
                keyValues.append(dt)
            else:
                keyValues.append(k)
            for f in valueFields:
                name = f["name"]
                values[name].append(v[name])

        v = {"keys":keyValues,"values":values}

        return(v)

    def getDataFrame(self,values = None):
        if self._data == None:
            return(None)

        data = {}

        fields = []

        if values != None:
            for v in values:
                f = self._schema.getField(v)
                if f != None:
                    fields.append(f)
        else:
            fields = self._schema._fields

        if isinstance(self._data,dict):
            #data["_key_"] = []

            for f in fields:
                data[f["name"]] = []

            for key,o in self._data.items():
                #data["_key_"].append(key)
                for f in fields:
                    name = f["name"]
                    if name in o:
                        if f["isNumber"]:
                            data[name].append(float(o[name]))
                        else:
                            data[name].append(o[name])
                    elif f["isNumber"]:
                        data[name].append(0.0)
                    else:
                        data[name].append("")
        elif isinstance(self._data,list):

            for f in fields:
                data[f["name"]] = []

            for o in self._data:
                for f in fields:
                    name = f["name"]
                    if name in o:
                        if f["isNumber"]:
                            data[name].append(float(o[name]))
                        else:
                            data[name].append(o[name])
                    elif f["isNumber"]:
                        data[name].append(0.0)
                    else:
                        data[name].append("")

        df = pd.DataFrame(data)

        return(df)

    def getInfo(self):
        return({})

    def addDelegate(self,delegate):
        if tools.supports(delegate,"dataChanged") == False:
            raise Exception("the delegate must implement the dataChanged method")

        tools.addTo(self._delegates,delegate)

    def removeDelegate(self,delegate):
        tools.removeFrom(self._delegates,delegate)

    def clear(self):
        pass

    def deliverDataChange(self,data,clear):
        for d in self._delegates:
            d.dataChanged(self,data,clear)

    def deliverInfoChange(self):
        for d in self._delegates:
            if tools.supports(d,"infoChanged"):
                d.infoChanged(self)

    def isList(self):
        return(self._data != None and isinstance(self._data,list))

    def isDict(self):
        return(self._data != None and isinstance(self._data,dict))

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
        Datasource.__init__(self,conn,**kwargs)
        self._path = path
        self._page = 0
        self._pages = 0
        self._data = {}

    def open(self):
        o = {}
        o["request"]= "event-collection"
        o["id"]= self._id
        o["action"]= "set"
        o["window"]= self._path
        o["schema"]= True
        o["info"]= 5
        o["format"]= "xml"

        interval = None

        if self.hasOpt("interval"):
            interval = self.getOpt("interval")
        elif self._connection.hasOpt("interval"):
            interval = self._connection.getOpt("interval")

        if interval == None:
            interval = 1000

        self.setOpt("interval",interval)

        for key,value in self.options.items():
            o[key] = value

        if self.hasOpt("filter") == False:
            o["filter"]= ""

        self._connection.send(o)

    def set(self):
        if self._connection.isHandshakeComplete == False:
            return

        o = {}
        o["request"]= "event-collection"
        o["id"]= self._id
        o["action"]= "set"

        for key,value in self.options.items():
            o[key] = value

        if self.hasOpt("filter") == False:
            o["filter"] = ""

        self._connection.send(o)

    def close(self):
        o = {}
        o["request"] = "event-collection"
        o["id"] = self._id
        o["action"] = "close"
        self._connection.send(o)

    def play(self):
        if self._paused:
            self._paused = False
            o["request"]= "event-collection"
            o["id"]= self._id
            o["action"]= "play"
            self.deliverInfoChange()

    def pause(self):
        if self._paused == False:
            self._paused = True
            o["request"]= "event-collection"
            o["id"]= self._id
            o["action"]= "pause"
            self.deliverInfoChange()

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
        self._connection.send(o)

    def events(self,xml):
        data = []

        nodes = xml.findall("entries/event")

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
            o["_opcode"] = opcode

            s = n.get("timestamp")

            if s != None:
                o["_timestamp"] = s

            values = n.findall("./*")

            for v in values:
                datatype = v.get("type")
                content = v.text

                if datatype != None:
                    o[v.tag] = "_data://" + datatype + ":" + content
                else:
                    o[v.tag] = content

            o["_key_"] = self.getKey(o)
            data.append(o)

        clear = "page" in xml.attrib

        self.process(data,clear)

        if clear:
            self.info(xml)

    def info(self,xml):
        if "page" in xml.attrib:
            self._page = int(xml.get("page"))
            self._pages = int(xml.get("pages"))
            self.deliverInfoChange()

    def process(self,events,clear):
        if clear:
            self._data = {}

        for e in events:
            key = e["_key_"]
            if key != None:
                opcode = e["_opcode"]
                if opcode == "delete":
                    if key in self._data:
                        del self._data[key]
                else:
                    o = {}

                    o["_key_"] = key

                    for column in self._schema._columns:
                        if column in e:
                            o[column] = e[column]
                    self._data[key] = o

        self.deliverDataChange(events,clear)

    def getInfo(self):
        info = {}
        info["page"] = self._page
        info["pages"] = self._pages
        return(info)
 
    def getKeyValues(self):
        values = []

        for key,value in self._data.items():
            values.append(key)
        return(values)

    def getTableData(self,values):

        rows = []
        columns = []
        cells = []

        a = []

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

        for key,value in self._data.items():
            rows.append(key)
            cell = []
            for f in a:
                cell.append(value[f["name"]])
            cells.append(cell)

        return({"rows":rows,"columns":columns,"cells":cells})

    def clear(self):
        self._data = {}
        self.deliverDataChange(None,True)

class EventStream(Datasource):
    def __init__(self,conn,path,**kwargs):
        Datasource.__init__(self,conn,**kwargs)
        self._path = path
        self._data = []
        self._counter = 1

    def open(self):
        o = {}
        o["request"] = "event-stream"
        o["id"] = self._id
        o["action"] = "set"
        o["window"] = self._path
        o["schema"] = True
        o["format"] = "xml"

        interval = None

        if self.hasOpt("interval"):
            interval = self.getOpt("interval")
        elif self._connection.hasOpt("interval"):
            interval = self._connection.getOpt("interval")

        if interval == None:
            interval = 1000

        self.setOpt("interval",interval)

        for n,v in self.options.items():
            o[n] = v

        if self.hasOpt("filter") == False:
            o["filter"]= ""

        self._connection.send(o)

    def set(self):
        o = {}
        o["request"] = "event-stream"
        o["id"] = self._id
        o["action"] = "set"
        o["format"] = "xml"

        for n,v in self.options.items():
            o[n] = v

        if self.hasOpt("filter") == False:
            o["filter"]= ""

        self._connection.send(o)

    def close(self):
        o = {}
        o["request"] = "event-stream"
        o["id"] = self._id
        o["action"] = "close"
        self._connection.send(o)

    def play(self):
        if self._paused:
            self._paused = False
            o["request"]= "event-stream"
            o["id"]= self._id
            o["action"]= "play"
            self.deliverInfoChange()

    def pause(self):
        if self._paused == False:
            self._paused = True
            o["request"]= "event-stream"
            o["id"]= self._id
            o["action"]= "pause"
            self.deliverInfoChange()

    def setSchema(self,xml):
        Datasource.setSchema(self,xml)
        for f in self._schema.fields:
            f["isKey"] = False

        self._keyFields = []

        f = {"name":"_opcode","espType":"utf8str","type":"string","isKey":False,"isNumber":False,"isDate":False,"isTime":False}
        self._schema._fields.insert(0,f)
        self._schema._fieldMap["_opcode"] = f
        self._schema._columns.insert(0,f["name"])

        f = {"name":"_timestamp","espType":"timestamp","type":"date","isKey":False,"isNumber":True,"isDate":False,"isTime":True}
        self._schema._fields.insert(0,f)
        self._schema._fieldMap["_timestamp"] = f
        self._schema._columns.insert(0,f["name"])

        f = {"name":"_counter","espType":"int32","type":"int","isKey":True,"isNumber":True,"isDate":False,"isTime":False}
        self._schema._fields.insert(0,f)
        self._schema._fieldMap["_counter"] = f
        self._schema._columns.insert(0,f["name"])

        self._schema._keyFields = [f]

        self._counter = 1

    def events(self,xml):
        data = []

        nodes = xml.findall("entries/event")

        ignoreDeletes = self.getOpt("ignore_deletes",False)

        for n in nodes:
            opcode = n.get("opcode")
            if opcode == None:
                opcode = "insert"

            if opcode == "delete" and ignoreDeletes:
                continue

            o = {}
            o["_opcode"] = opcode

            s = n.get("timestamp")

            if s != None:
                o["_timestamp"] = s

            values = n.findall("./*")

            for v in values:
                datatype = v.get("type")
                content = v.text

                if datatype != None:
                    o[v.tag] = "_data://" + datatype + ":" + content
                else:
                    o[v.tag] = content

            data.append(o)

        self.process(data)

    def process(self,events):
        for e in events:
            o = {}

            for column in self._schema._columns:
                if column in e:
                    o[column] = e[column]

            o["_counter"] = self._counter
            o["_key_"] = self.getKey(o)
            e["_counter"] = o["_counter"]
            e["_key_"] = o["_key_"]

            self._counter += 1

            self._data.append(o)

        maxEvents = self.getOpt("maxevents",50)

        diff = len(self._data) - maxEvents

        if diff > 0:
            for i in range(0,diff):
                del self._data[0]
 
        self.deliverDataChange(events,False)

    def getData(self):
        return(self._data)

    def getKeyValues(self):
        values = []

        for value in self._data:
            values.append(value["_counter"])
        return(values)

    def getTableData(self,values):

        rows = []
        columns = []
        cells = []

        a = []

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

        for value in self._data:
            rows.append(value["_counter"])
            cell = []
            for f in a:
                cell.append(value[f["name"]])
            cells.append(cell)

        return({"rows":rows,"columns":columns,"cells":cells})

    def clear(self):
        self._data = []
        self.deliverDataChange(None,True)

class Publisher(tools.Options):
    def __init__(self,connection,path,**kwargs):
        tools.Options.__init__(self,**kwargs)
        self._connection = connection
        self._path = path
        self._id = tools.guid()
        self._data = []

    def open(self):
        o = {}
        o["request"] = "publisher"
        o["id"] = self._id
        o["action"] = "set"
        o["window"] = self._path
        o["schema"] = True
        self._connection.send(o)

    def close(self):
        o = {}
        o["request"] = "publisher"
        o["id"] = self._id
        o["action"] = "delete"
        self._connection.send(o)

    def begin(self):
        self._o = {}

    def set(self,name,value):
        self._o[name] = value

    def end(self):
        if self._o != None:
            self._data.append(self._o)
            self._o = {}

    def add(self,o):
        self._data.append(o)

    def publish(self):
        if len(self._data) > 0:
            o = {}
            o["request"] = "publisher"
            o["id"] = self._id
            o["action"] = "publish"
            o["data"] = self._data
            self._connection.send(o)
            self._data = []

    def publishUrl(self,url,**kwargs):
        self._connection.publishUrl(self._path,url,**kwargs)

class Stats(Datasource):
    def __init__(self,connection,**kwargs):
        Datasource.__init__(self,connection,**kwargs)
        self._delegates = []
        self._data = []
        self._memory = {}

        self._schema.addField({"name":"_key_","espType":"utf8str","type":"string","isKey":True,"isNumber":False,"isDate":False,"isTime":False})
        self._schema.addField({"name":"project","espType":"utf8str","type":"string","isKey":False,"isNumber":False,"isDate":False,"isTime":False})
        self._schema.addField({"name":"contquery","espType":"utf8str","type":"string","isKey":False,"isNumber":False,"isDate":False,"isTime":False})
        self._schema.addField({"name":"window","espType":"utf8str","type":"string","isKey":False,"isNumber":False,"isDate":False,"isTime":False})

        #self._schema.addField({"name":"project","espType":"utf8str","type":"string","isKey":True,"isNumber":False,"isDate":False,"isTime":False})
        #self._schema.addField({"name":"contquery","espType":"utf8str","type":"string","isKey":True,"isNumber":False,"isDate":False,"isTime":False})
        #self._schema.addField({"name":"window","espType":"utf8str","type":"string","isKey":True,"isNumber":False,"isDate":False,"isTime":False})

        self._schema.addField({"name":"cpu","espType":"double","type":"double","isKey":False,"isNumber":True,"isDate":False,"isTime":False})
        self._schema.addField({"name":"interval","espType":"int64","type":"int","isKey":False,"isNumber":True,"isDate":False,"isTime":False})
        self._schema.addField({"name":"count","espType":"int64","type":"int","isKey":False,"isNumber":True,"isDate":False,"isTime":False})

    def sortValue(self,o):
        return(o["cpu"])

    def process(self,xml):
        projects = xml.findall(".//project")

        stats = []

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
                    o["_key_"] = o["project"] + "." + o["contquery"] + "." + o["window"]
                    stats.append(o)

        stats.sort(key = self.sortValue,reverse = True)

        nodes = xml.findall(".//server-memory")

        self._memory = {}

        if len(nodes) == 1:
            node = nodes[0].find("system")
            if node != None:
                self._memory["system"] = int(node.text)
            node = nodes[0].find("virtual")
            if node != None:
                self._memory["virtual"] = int(node.text)
            node = nodes[0].find("resident")
            if node != None:
                self._memory["resident"] = int(node.text)

        self._data = stats

        for d in self._delegates:
            d.handleStats(self)

    def setOpts(self,**kwargs):
        tools.Options.setOpts(self,**kwargs)
        if len(self._delegates) > 0:
            self.set()

    def setOpt(self,name,value):
        tools.Options.setOpt(self,name,value)
        if len(self._delegates) > 0:
            self.set()

    def set(self):
        o = {}
        o["request"] = "stats"
        o["action"] = "set"
        o["interval"] = self.getOpt("interval",1)

        o["minCpu"] = self.getOpt("mincpu",5)
        o["counts"] = self.getOpt("counts",False)
        o["config"] = self.getOpt("config",False)
        o["memory"] = self.getOpt("memory",True)
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
        if tools.removeFrom(self._delegates,delegate):
            if len(self._delegates) == 0:
                self.stop()

    def getData(self):
        return(self._data)

    def getMemoryData(self):
        return(self._memory)
 
    def getKeyValues(self):
        values = []
        for o in self._data:
            values.append(o["_key_"])
        return(values)

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

    def addField(self,field):
        name = field["name"]
        if (name in self._fieldMap) == False:
            self._fields.append(field)
            self._columns.append(name)

            self._fieldMap[name] = field

            if field["isKey"]:
                self._keyFields.append(field)

    def getField(self,name):
        if name in self._fieldMap:
            return(self._fieldMap[name])
        return(None)

    def getFields(self):
        return(self._fields)

    def getKeyFields(self):
        return(self._keyFields)

    def getColumnFields(self):
        fields = []
        for f in self._fields:
            if field["isKey"] == False:
                keys.append(f)

        return(keys)

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
            f["isNumber"] = field["isNumber"]
            f["isDate"] = field["isDate"]
            f["isTime"] = field["isTime"]
            if field["isKey"]:
                f["isKey"] = "true"
            else:
                f["isKey"] = "false"
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
