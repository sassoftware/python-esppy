from IPython.display import display, Javascript
import esppy.espapi.connections as connections
from ipykernel.comm import Comm
from xml.etree import ElementTree
import logging
import copy
import datetime
import esppy.espapi.api as api
import esppy.espapi.tools as tools
import json
import re

class Charts(object):
    def __init__(self,connection,options = None):
        self._connection = connection
        self._charts = []
        self._options = tools.Options(options)
        self._url = self._options.get("url","")
        self._theme = self._options.get("theme","sas_corporate")
        self._dataSkin = self._options.get("dataSkin","crisp")
        self._kpiSkin = self._options.get("kpiSkin","charcoal")
        self._id = 1

    def createChart(self,type,datasource,values,options = None):

        datasource.addDelegate(self)

        chart = Chart(self,type,datasource,values,options)
        self._charts.append(chart)
        return(chart)

    def createModelViewer(self,options):
        mv = ModelViewer(self,options)
        return(mv)

    def schemaSet(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.initialize()

    def dataChanged(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.draw()

    def infoChanged(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.sendInfo()

    def deliverSelection(self,chart,message):
        for c in self._charts:
            if c != chart and c._datasource == chart._datasource:
            #if c._datasource == chart._datasource:
                o = {}
                o["type"] = "selection"
                o["indices"] = message["indices"]
                c.send(o)

    def getId(self):
        id = self._id
        self._id += 1
        return(str(id))

class Chart(object):

    def __init__(self,charts,type,datasource,values,options):
        self._charts = charts
        self._id = tools.guid()
        self._id = self._charts.getId()
        self._type = type
        self._datasource = datasource
        self.values = values
        self._info = self._datasource.getInfo()
        self._data = self.getData()
        self._comm = None
        self.options = options
        self._initialized = False

    def draw(self):
        self.data = self.getData()

    def sendInfo(self):
        if self._comm != None:
            o = self._datasource.getInfo()
            o["type"] = "info"
            self._comm.send(o)

    def send(self,o):
        if self._comm != None:
            self._comm.send(o)

    def target(self,comm,msg):
        self._comm = comm
        if self._datasource.schema.hasFields():
            self.initialize()
        @comm.on_msg
        def _recv(msg):
            message = msg["content"]["data"]
            if message["type"] == "selection":
                self._charts.deliverSelection(self,message)
            else:
                self._datasource.handleMessage(message)

    def initialize(self):
        if self._initialized == False:
            o = {}
            o["type"] = "schema"
            o["schema"] = self._datasource.schema.toJson()
            self._comm.send(o)
            self.data = self.getData()
            self._initialized = True

    def _repr_html_(self):
        self.data = self.getData()
        html = ""
        html += self.getHtml()
        return(html)

    def getHtml(self):
        get_ipython().kernel.comm_manager.register_target(self._id,self.target)

        values = ""

        i = 0

        if len(self._values) > 0:
            for i in range(0,len(self._values)):
                if i > 0:
                    values += ","
                values += (self._values[i])

        html = ""

        url = self._charts._url

        if len(url) > 0:
            url  += "/esp/js/libs"

        height = self.getHeight()

        html += '''

        <style type="text/css">
        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS.ttf);
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-Bold.ttf);
            font-weight:bold;
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-BoldItalic.ttf);
            font-weight:bold;
            font-style:italic;
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-Italic.ttf);
            font-style:italic;
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-Light.ttf);
            opacity:.2;
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-LightItalic.ttf);
            font-style:italic;
            opacity:.2;
        }

        @font-face
        {
            font-family:sas-icons;
            src:url(/static/fonts/sas-icons.ttf);
        }

        .visualContainer
        {
            position:relative;
            overflow:auto;
        }

        div.chart
        {
            border:1px solid #d8d8d8;
        }

        div.chartContainer
        {
            border:6px solid #d8d8d8;
            border:0;
        }

        .icon
        {
            font-family:sas-icons;
            font-weight:regular;
            font-size:1.5rem;
            color:#4e4e4e;
            padding:0;
        }

        a.icon
        {
            color:#4e4e4e;
            text-decoration:none;
            text-decoration:underline;
            font-size:14pt;
        }

        a.icon:hover
        {
            color:#0093e5;
        }

        </style>

        <script type="text/javascript">

        var _chart%(id)s = null;
        var _schema%(id)s = null;
        var _options%(id)s = %(options)s;
        var _container%(id)s = null;

        </script>

        %(chartHtml)s

        <script type="text/javascript">

        function
        selected_%(id)s()
        {
            console.log("data selected: " + this._collection.getSelectedIndices());
            o = new Object();
            o.type = "selection";
            o.indices = this._collection.getSelectedIndices();
            console.log(o);
            _comm%(id)s.send(o);
        }

        function
        send_%(id)s(type,data)
        {
            var o = new Object();
            o["type"] = type;
            if (data != null)
            {
                o["data"] = data;
            }
                console.log(JSON.stringify(o));
            _comm%(id)s.send(o);
        }

        var _data%(id)s = null;

        function
        draw%(id)s()
        {
            if ("%(type)s" == "timeseries")
            {
            }
        }

        if (Jupyter.notebook.kernel != null)
        {
            var _comm%(id)s = Jupyter.notebook.kernel.comm_manager.new_comm("%(id)s");

            _comm%(id)s.on_msg(function(msg)
            {
                //console.log("data: " + JSON.stringify(msg.content));

                var type = msg.content.data.type;

                if (type == "schema")
                {
                    _schema%(id)s = msg.content.data.schema;
                    if (_chart%(id)s != null)
                    {
                        _chart%(id)s._collection.setFields(_schema%(id)s);
                        _chart%(id)s.create();
                    }
                }
                else if (type == "data")
                {
                    _data%(id)s = msg.content.data.chartdata;
                    if (_chart%(id)s != null)
                    {
                        _chart%(id)s._collection.clear();
                        _chart%(id)s._collection.clearDataset();
                        _chart%(id)s._collection.setItems(_data%(id)s);
                        _chart%(id)s._collection.setFieldValues();
                        _chart%(id)s.update();

                        var page = new Number(msg.content.data.info.page);
                        var pages = msg.content.data.info.pages;

                        pages_%(id)s(page,pages);
                    }
                    draw%(id)s();
                }
                else if (type == "options")
                {
                    _options%(id)s = msg.content.data.options;
                    draw%(id)s();
                }
                else if (type == "info")
                {
                    var page = new Number(msg.content.data.page);
                    var pages = msg.content.data.pages;
                    pages_%(id)s(page,pages);
                }
                else if (type == "selection")
                {
                    _chart%(id)s._collection.clearDataset();
                    _chart%(id)s._collection.setSelectedIndices(msg.content.data.indices);
                    _chart%(id)s.setSelections();
                    _chart%(id)s.update();
                }
                else
                {
                    console.log("MSG: " + JSON.stringify(msg.content));
                }
            });

            send_%(id)s("init");
        }

        if (window.hasOwnProperty("_sascharts") == false)
        {
            var baseUrl = "%(url)s";

            if (baseUrl.length > 0)
            {
                requirejs.config({baseUrl:baseUrl});
            }

            require(["common/sascharts"],
            function(sascharts)
            {
                window._sascharts = sascharts;
                sascharts.initPy({"ready":ready_%(id)s},"%(theme)s");
            });
        }
        else
        {
            create_%(id)s(window._sascharts);
        }

        function
        ready_%(id)s(sascharts)
        {
            create_%(id)s(sascharts);
        }

        function
        create_%(id)s(sascharts)
        {
            var charts = sascharts.create(null);

            charts.setDataSkin("%(dataSkin)s");
            charts.setKpiSkin("%(kpiSkin)s");

            var values = "%(values)s".split(",");
            _chart%(id)s = charts.createChart("%(type)s",null,values,document.getElementById("%(id)s_div"),%(options)s);
            if (_schema%(id)s != null)
            {
                _chart%(id)s._collection.setFields(_schema%(id)s);
            }

            _chart%(id)s.dataSelected = selected_%(id)s;

            if (_data%(id)s != null)
            {
                _chart%(id)s._collection.setItems(_data%(id)s);
                _chart%(id)s._collection.setFieldValues();
                _chart%(id)s.update();
            }

            _chart%(id)s.create();

            var div = document.getElementById("%(id)s_div");

            while (div != null)
            {
                if (div.className != null)
                {
                    if (div.className.indexOf("output_html") != -1 || div.className == "dashboardContainer")
                    {
                        _container%(id)s = div;
                        break;
                    }
                }
                div = div.parentNode;
            }

            size_%(id)s();
        }

        function
        size_%(id)s()
        {
            if (_container%(id)s != null)
            {
                var c = _container%(id)s;

                //c.style.overflow = "hidden";

                var inset = 0;

                if (c.className.indexOf("output_html") != -1)
                {
                    inset = 10;
                }

                var container = document.getElementById("%(id)s_container");
                var div = document.getElementById("%(id)s_div");
                var buttons = document.getElementById("%(id)s_buttons");
                if (container == null || div == null || buttons == null)
                {
                    return;
                }
                container.style.width = (c.clientWidth - inset) + "px";
                //container.style.height = (c.clientHeight - inset) + "px";
                container.style.height = "%(height)spx";
                div.style.width = (container.clientWidth - inset) + "px";
                var height = container.clientHeight;

                if (buttons != null)
                {
                    buttons.style.width = div.offsetWidth + "px";
                    height -= buttons.offsetHeight;
                }

                div.style.height = height + "px";
            }

            _chart%(id)s.size();
        }

        function
        pages_%(id)s(page,pages)
        {
            var buttons = document.getElementById("%(id)s_buttons");

            if (buttons == null)
            {
                return;
            }

            if (isNaN(page))
            {
                buttons.style.display = "none";
            }
            else
            {
                var pageText = document.getElementById("%(id)s_page");
                var pagesText = document.getElementById("%(id)s_pages");

                if (pageText != null)
                {
                    pageText.innerText = (page + 1);
                    pagesText.innerText = pages;
                }

                buttons.style.display = (pages <= 1) ? "none" : "block";
            }

            size_%(id)s();
        }

        </script>

        ''' % dict(id=self._id,type=self._type,values=values,options=json.dumps(self._options),chartHtml=self.getChartHtml(),url=url,theme=self._charts._theme,dataSkin=self._charts._dataSkin,kpiSkin=self._charts._kpiSkin,height=height)

        return(html)

    def getChartHtml(self):
        html = ""

        width = self.getOption("width","400")
        height = self.getOption("height","400")

        html += '''

        <div id='%(id)s_container' class='chartContainer' style='width:%(width)spx;height:%(height)spx'>
        <!--
        <div class='chart' id='%(id)s_div' style='width:%(width)spx;height:%(height)spx;position:relative'>
        <div class="chart" id="%(id)s_div" style="width:100%%;height:100%%;position:relative">
        -->
        <div class="chart" id="%(id)s_div" style="position:relative">
        </div>
        ''' % dict(id=self._id,width=width,height=height)

        #if self._datasource.type == "updating":
        if True:
            html += '''
            <div id='%(id)s_buttons' class='espButtons' style='display:none'>
                <table style='width:100%%'>
                    <tr>
                        <td>
                            <table class='espButtons'>
                                <tr>
                                    <td class='icon'><button onclick='javascript:send_%(id)s("prev")'>&#xf043;</button></td>
                                    <td class='icon'><button onclick='javascript:send_%(id)s("next")'>&#xf045;</button></td>
                                    <td class='icon'><button onclick='javascript:send_%(id)s("first")'>&#xf044;</button></td>
                                    <td class='icon'><button onclick='javascript:send_%(id)s("last")'>&#xf046;</button></td>
                                </tr>
                            </table>
                        </td>
                        <td>
                            <span>Page</span>
                            <span id='%(id)s_page'></span>
                            <span>of</span>
                            <span id='%(id)s_pages'></span>
                        </td>
                    </tr>
                </table>
            </div>

            ''' % dict(id=self._id)

        html += "</div>"
        html += "\n"

        return(html)

    def getData(self):
        data = []

        if self._values == None:
            return;

        events = self._datasource.getData()
        fields = self._datasource.schema.fields;

        if isinstance(self._datasource,connections.EventCollection):
            for key,e in events.items():
                o = {}

                for field in fields:
                    name = field["name"]
                    if name in e:
                        if field["isNumber"] == True:
                            o[name] = float(e[name])
                        else:
                            o[name] = e[name]
                    else:
                        o[name] = 0

                data.append(o)
        else:
            for e in events:
                o = {}

                for field in fields:
                    name = field["name"]
                    if name in e:
                        if name == "__timestamp":
                            ms = int(e[name])
                            ms = int(ms / 1000000);
                            timestamp = datetime.datetime.fromtimestamp(ms)
                            o[name] = ms
                        elif field["isNumber"] == True:
                            o[name] = float(e[name])
                        else:
                            o[name] = e[name]
                    else:
                        o[name] = 0

                data.append(o)

        return(data)

    def getHeight(self):
        return(self.getOption("height","400"))

    def setHeight(self,value):
        self.setOption("height",value)

    @property
    def values(self):
        return(self._values)

    @values.setter
    def values(self,value):

        self._values = []

        if value != None:
            if type(value) is list:
                for v in value:
                    self._values.append(v)
            else:
                self._values.append(value)

        #self.data = self.getData()

    @property
    def data(self):
        return(self._data)

    @data.setter
    def data(self,value):
        self._data = value
        if self._comm != None:
            o = {}
            o["type"] = "data"
            o["info"] = self._datasource.getInfo()
            o["chartdata"] = self._data
            self._comm.send(o)

    @property
    def options(self):
        return(self._options)

    @options.setter
    def options(self,options):
        if options == None:
            self._options = {}
        else:
            self._options = options

        if self._comm != None:
            o = {}
            o["type"] = "options"
            o["options"] = self._options
            self._comm.send(o)

    def getOption(self,name,dv = None):
        if dv != None:
            value = dv
        else:
            value = None
        if name in self._options:
            value = self._options[name]
        return(value)

    def setOption(self,name,value):
        if value != None:
            self._options[name] = value
        else:
            self._options.pop(value,None)
        return(value)

    @property
    def type(self):
        return(self._type)

class ModelViewer(object):

    def __init__(self,charts,options):
        self._charts = charts
        self._id = tools.guid()
        self._comm = None
        self._project = None
        self._options = tools.Options(options)
        self._stats = None
        self._selectionCb = None

    def target(self,comm,msg):
        self._comm = comm
        self.sendModel()
        @comm.on_msg
        def _recv(msg):
            message = msg["content"]["data"]
            if message["type"] == "selection":
                if self._selectionCb != None:
                    self._selectionCb(message["data"])
            #logging.debug(message)

    def sendModel(self):
        if self._comm != None:
            o = {}
            o["type"] = "model"
            o["chartdata"] = self.build()
            self._comm.send(o)

    def sendStats(self,data):
        if self._comm != None:
            o = {}
            o["type"] = "stats"
            o["statsdata"] = data
            self._comm.send(o)

    def _repr_html_(self):
        html = ""
        html += self.getHtml()

        return(html)

    def getHtml(self):
        get_ipython().kernel.comm_manager.register_target(self._id,self.target)

        width = self._options.get("width",800)
        height = self._options.get("height",800)

        html = ""

        url = self._charts._url;

        if len(url) > 0:
            url  += "/esp/js/libs";

        html += '''

        <style type="text/css">

        .visualContainer
        {
            position:relative;
            overflow:auto;
        }

        div.chart
        {
            border:1px solid #d8d8d8;
        }

        div.chartContainer
        {
            border:6px solid #d8d8d8;
            border:0;
        }

        div.rendered_html td
        {
            background:white;
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS.ttf);
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-Bold.ttf);
            font-weight:bold;
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-BoldItalic.ttf);
            font-weight:bold;
            font-style:italic;
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-Italic.ttf);
            font-style:italic;
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-Light.ttf);
            opacity:.2;
        }

        @font-face
        {
            font-family:AvenirNextforSAS;
            src:url(/static/fonts/AvenirNextforSAS-LightItalic.ttf);
            font-style:italic;
            opacity:.2;
        }

        @font-face
        {
            font-family:sas-icons;
            src:url(/static/fonts/sas-icons.ttf);
        }

        .icon
        {
            font-family:sas-icons;
            font-weight:regular;
            color:#4e4e4e;
            font-size:10pt;
            padding:0;
        }

        </style>

        <div id='%(id)s_container' class='chartContainer' style='width:%(width)spx;height:%(height)spx'>
        <div class="chart" id="%(id)s_div" style="position:relative;overflow:hidden"></div>
        </div>

        <!--
        <table>
        <tr><td><div class="chart" id="%(id)s_div" style="width:%(width)spx;height:%(height)spx;position:relative;overflow:hidden"></div></td></tr>
        </table>
        -->
        <script language="javascript">

        var mv_%(id)s = null;
        var model_%(id)s = null;
        var container_%(id)s = null;

        if (Jupyter.notebook.kernel != null)
        {
            var _comm%(id)s = Jupyter.notebook.kernel.comm_manager.new_comm("%(id)s");

            _comm%(id)s.on_msg(function(msg)
            {
                var type = msg.content.data.type;
                //console.log(msg.content.data);
                if (type == "model")
                {
                    if (mv_%(id)s != null)
                    {
                        mv_%(id)s.loadFromModel(msg.content.data.chartdata);
                    }
                    else
                    {
                        model_%(id)s = msg.content.data.chartdata;
                    }
                }
                else if (type == "stats")
                {
                    if (mv_%(id)s != null)
                    {
                        mv_%(id)s.setProjectStats(msg.content.data.statsdata);
                    }
                }
                else
                {
                    console.log("MSG: " + JSON.stringify(msg.content));
                }
            });

            function
            send_%(id)s(type,data)
            {
                var o = new Object();
                o["type"] = type;
                if (data != null)
                {
                    o["data"] = data;
                }
                _comm%(id)s.send(o);
            }

            send_%(id)s("init");
        }

        if (window.hasOwnProperty("_sascharts") == false)
        {
            var baseUrl = "%(url)s";

            if (baseUrl.length > 0)
            {
                window._tmpBaseUrl = require.toUrl("");
                requirejs.config({baseUrl:baseUrl});
            }

            require(["common/sascharts"],
            function(sascharts)
            {
                window._sascharts = sascharts;
                sascharts.initPy({"ready":ready_%(id)s},"%(theme)s");
            });
        }
        else
        {
            create_%(id)s(window._sascharts);
        }

        function
        ready_%(id)s(sascharts)
        {
            create_%(id)s(sascharts);
        }

        function
        create_%(id)s(sascharts)
        {
            var charts = sascharts.create();
            charts.loadResources();
            var options = %(options)s;
            mv_%(id)s = charts.createModelViewer("%(id)s_div",options);
            mv_%(id)s.nodeSelected = nodeSelected_%(id)s;

            if (model_%(id)s != null)
            {
                model_%(id)s = msg.content.data.chartdata;
                mv_%(id)s.loadFromModel(model_%(id)s);
            }

            var div = document.getElementById("%(id)s_div");

            while (div != null)
            {
                if (div.className != null)
                {
                    if (div.className.indexOf("output_html") != -1 || div.className == "dashboardContainer")
                    {
                        _container%(id)s = div;
                        break;
                    }
                }
                div = div.parentNode;
            }

            size_%(id)s();
        }

        function
        size_%(id)s()
        {
            if (_container%(id)s != null)
            {
                var c = _container%(id)s;

                c.style.overflow = "hidden";

                var inset = 0;

                if (c.className.indexOf("output_html") != -1)
                {
                    inset = 0;
                }

                var container = document.getElementById("%(id)s_container");
                var div = document.getElementById("%(id)s_div");
                container.style.width = (c.clientWidth - inset) + "px";
                container.style.height = (c.clientHeight - inset) + "px";
                div.style.width = (container.clientWidth - inset) + "px";
                div.style.height = (container.clientHeight - inset) + "px";
            }

            mv_%(id)s.size();
        }

        function
        nodeSelected_%(id)s(items,indices)
        {
            var a = new Array();

            for (var i = 0; i < items.length; i++)
            {
                a.push(items[i].get("id"));
            }

            send_%(id)s("selection",a);
        }

        </script>

        ''' % dict(id=self._id,width=width,height=height,options=json.dumps(self._options.options),url=url,theme=self._charts._theme)

        return(html)

    def build(self):
        if self._project == None:
            return

        model = {}
        windows = []
        edges = []
        model["windows"] = windows
        model["edges"] = edges

        for a in self._charts._connection._server.windows:
            if self._project == "*" or a["p"] == self._project:
                if a["type"] == "source" or a["type"] == "window-source" or len(a["incoming"]) > 0 or len(a["outgoing"]) > 0:
                    window = {}
                    window["key"] = a["key"]
                    window["name"] = a["name"]
                    window["type"] = a["type"]
                    window["index"] = a["index"]
                    window["xml"] = ElementTree.tostring(a["xml"]).decode()
                    window["schema"] = a["schema"].toString()
                    if window["type"] in api.Server._windowClasses:
                        window["class"] = api.Server._windowClasses[window["type"]]
                    windows.append(window)

                    for z in a["outgoing"]:
                        edge = {}
                        edge["a"] = a["key"]
                        edge["z"] = z["key"]
                        edges.append(edge)

        return(model)

    def setShowStats(self,value):
        if value:
            if self._stats == None:
                self._stats = ModelViewerStats(self._charts._connection,self)
        elif self._stats != None:
            self._stats.stop()
            self._stats = None

    def getHeight(self):
        return(self._options.get("height","400"))

    def setHeight(self,value):
        self._options.set("height",value)

    def getOption(self,name,dv = None):
        return(self._options.get(name,dv))

    def setOption(self,name,value):
        self._options.set(name,value)

    @property
    def project(self):
        return(self._project)

    @project.setter
    def project(self,value):
        self._project = value
        self.sendModel()

    @property
    def selectionCb(self):
        return(self._selectionCb)

    @selectionCb.setter
    def selectionCb(self,value):
        self._selectionCb = value

class ModelViewerStats(object):

    def __init__(self,connection,modelViewer):
        self._connection = connection
        self._modelViewer = modelViewer
        #self._stats = self._connection._server.getStats(0,2)
        self._stats = self._connection.getStats();
        self._stats.addDelegate(self)

    def __del__(self):
        self._stats.stop()

    def stop(self):
        self._stats.removeDelegate(self)
        del self

    def handleStats(self,stats):
        self._modelViewer.sendStats(stats.getData())
