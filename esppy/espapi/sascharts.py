from IPython.display import display, Javascript
from ipykernel.comm import Comm
from xml.etree import ElementTree
import datetime
import esppy.espapi.api
import uuid
import json
import re

class SasCharts(object):
    def __init__(self,server):
        self._server = server
        self._charts = []

    def createChart(self,type,datasource,values,options = None):

        datasource.addChangeDelegate(self)

        chart = Chart(type,datasource,values,options)
        self._charts.append(chart)
        return(chart)

    def dataChanged(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.render()

    def infoChanged(self,datasource):
        pass

class Chart(object):

    def __init__(self,type,datasource,values,options,**kwargs):
        self._id = "1"
        self._id = str(uuid.uuid4()).replace('-', '_')
        self._type = type
        self._datasource = datasource
        self.values = values
        self._info = self._datasource.getInfo()
        self._data = self.getData()
        self._comm = None
        self.options = options

    def render(self):
        self.data = self.getData()

    def target(self,comm,msg):
        self._comm = comm
        o = {}
        o["type"] = "schema"
        #o["schema"] = ElementTree.tostring(self._datasource.schema.toXml()).decode("utf-8")
        o["schema"] = self._datasource.schema.toJson()
        self._comm.send(o)
        self.data = self.getData()
        @comm.on_msg
        def _recv(msg):
            self._datasource.handleMessage(msg["content"]["data"])

    def _repr_html_(self):
        self.data = self.getData()
        html = ""
        html += self.getHtml()
        return(html)

    def getHtml(self):
        get_ipython().kernel.comm_manager.register_target(self._id,self.target)

        values = ""

        i = 0

        for i in range(0,len(self._values)):
            if i > 0:
                values += ","
            values += (self._values[i])

        html = ""

        html += '''

        <style type="text/css">
        .visualContainer
        {
            position:relative;
            overflow:auto;
        }
        </style>

        <script type="text/javascript">

        var _sascharts = null;
        var _datasources = new Object();
        var _chart%(id)s = null;
        var _schema%(id)s = null;
        var _options%(id)s = %(options)s;

        </script>

        %(chartHtml)s

        <script type="text/javascript">

        function
        createChart()
        {
            var values = "%(values)s".split(",");
            _chart%(id)s = _sascharts.createChart("%(type)s",values,document.getElementById("%(id)s_div"));
            if (_schema%(id)s != null)
            {
                _chart%(id)s._collection.setFields(_schema%(id)s);
            }

            if (_data%(id)s != null)
            {
                _chart%(id)s._collection.setItems(_data%(id)s);
                _chart%(id)s._collection.setFieldValues();
                _chart%(id)s.update();
            }

            _chart%(id)s.create();
        }

        </script>

        <script language="javascript">
        function
        send_%(id)s(type)
        {
            _comm%(id)s.send({"type":type});
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
                        _chart%(id)s._collection.setItems(_data%(id)s);
                        _chart%(id)s._collection.setFieldValues();
                        _chart%(id)s.update();
                    }
                    draw%(id)s();
                }
                else if (type == "options")
                {
                    _options%(id)s = msg.content.data.options;
                    draw%(id)s();
                }
                else
                {
                    //console.log("MSG: " + JSON.stringify(msg.content));
                }
            });

            send_%(id)s("init");
        }

        if (require.defined("sascharts") == false)
        {
            require(["sascharts"],
            function(sascharts)
            {
                sascharts.init({"ready":ready});
            });
        }
        else
        {
            ready(require("sascharts"));
        }

        function
        ready(sascharts)
        {
            console.log("READY ID IS: %(id)s");
            _sascharts = sascharts;
            createChart();
        }

        </script>

        ''' % dict(id=self._id,type=self._type,values=values,options=json.dumps(self._options),chartHtml=self.getChartHtml())

        return(html)

    def getChartHtml(self):
        html = ""

        width = self.getOption("width","400")
        height = self.getOption("height","400")

        html += '''

        <div class='espapiContainer'>
        <div class='chart' id='%(id)s_div' style='width:%(width)spx;height:%(height)spx;position:relative'>
        </div>
        ''' % dict(id=self._id,width=width,height="600")

        if self._datasource.type == "updating":
            html += '''
            <div class='espButtons'>
                <table style='width:100%%'>
                <td>
                <table class='espButtons'>
                <tr>
                <td class='espButton'><button onclick='javascript:send_%(id)s(\'prev\')'>Prev</button></td>
                <td class='espButton'><button onclick='javascript:send_%(id)s(\'next\')'>Next</button></td>
                <td class='espButton'><button onclick='javascript:send_%(id)s(\'first\''">First</button></td>
                <td class='espButton'><button onclick='javascript:send_%(id)s(\'last\')'>Last</button></td>
                </tr>
                </table>
                </td>
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

        updating = (self._datasource.type == "updating");

        if updating == True:
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

    @property
    def values(self):
        return(self._values)

    @values.setter
    def values(self,value):

        self._values = []

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

    @property
    def type(self):
        return(self._type)

class Dashboard(object):

    def __init__(self):
        self._rows = []
        self._charts = []

        self._rows.append([])

    def addRow(self):
        self._rows.append([])

    def addChart(self,chart):
        self._rows[len(self._rows) - 1].append(chart)

    def _repr_html_(self):
        html = ""

        maxcols = 0

        for row in self._rows:
            if len(row) > maxcols:
                maxcols = len(row)

        html += '''
        <style type="text/css">
        .dashboardTable
        {
            width:100%;
            border:2px solid red;
        }

        td.dashboardCell
        {
            background:white;
            padding:0;
        }

        .dashboardContainer
        {
            padding:10px;
        }

        </style>
        '''

        html += "<table class='dashboardTable'>"

        for row in self._rows:
            html += "<tr>"
            for i in range(0,len(row)):
                chart = row[i]
                html += "<td class='dashboardCell'"
                if i == 0 and len(row) < maxcols:
                    html += " colspan='" + str(maxcols - len(row) + 1) + "'"
                html += ">"
                html += "<div class='dashboardContainer'>";
                html += chart.getHtml()
                html += "</div>";
                html += "</td>"
            html += "</tr>"

        html += "</table>"

        return(html)
