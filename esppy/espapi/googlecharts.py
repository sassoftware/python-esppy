from IPython.display import display, Javascript
from ipykernel.comm import Comm
import datetime
import esppy.espapi.api
import esppy.espapi.dashboards
import esppy.espapi.tools as tools
import json
import re

class Charts(object):
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

    _chartsLoaded = False

    def __init__(self,type,datasource,values,options):
        self._id = tools.guid()
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

        html = ""

        if Chart._chartsLoaded == False:
            html += "<script type='text/javascript' src='https://www.gstatic.com/charts/loader.js'></script>"
            Chart._chartsLoaded = True

        html += '''

        <style type="text/css">

        div.chart
        {
            border:1px solid #d8d8d8;
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
            font-size:1.5rem;
            color:#4e4e4e;
            padding:0;
        }

        </style>
        '''

        html += self.getChartHtml()

        html += '''
        <script type="text/javascript">

            var _chart%(id)s = null;
            var _options%(id)s = %(options)s;

            if (window.hasOwnProperty("google") == false)
            {
                require(["https://www.gstatic.com/charts/loader.js"], function() {
                    google.charts.load("current",{"packages":["corechart","table","gauge","treemap","orgchart","annotationchart","geochart","treemap"]});
                    google.charts.setOnLoadCallback(createChart);
                });
            }
            else
            {
                createChart();
            }

            function
            createChart()
            {
                if ("%(type)s" == "vbar")
                {
                    _chart%(id)s = new google.visualization.ColumnChart(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "hbar")
                {
                    _chart%(id)s = new google.visualization.BarChart(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "line")
                {
                    _chart%(id)s = new google.visualization.LineChart(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "pie")
                {
                    _chart%(id)s = new google.visualization.PieChart(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "table")
                {
                    _chart%(id)s = new google.visualization.Table(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "gauge")
                {
                    _chart%(id)s = new google.visualization.Gauge(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "area")
                {
                    _chart%(id)s = new google.visualization.AreaChart(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "bubble")
                {
                    _chart%(id)s = new google.visualization.BubbleChart(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "tree")
                {
                    _chart%(id)s = new google.visualization.TreeMap(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "map")
                {
                    _chart%(id)s = new google.visualization.Map(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "timeseries")
                {
                    _chart%(id)s = new google.visualization.AnnotationChart(document.getElementById("%(id)s_div"));
                }
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
                _chart%(id)s = new google.visualization.AnnotationChart(document.getElementById("%(id)s_div"));
            }

            var dt = new google.visualization.DataTable();

            if (_data%(id)s != null)
            {
                var column;

                for (var i = 0; i < _data%(id)s.columns.length; i++)
                {
                    column = _data%(id)s.columns[i];
                    dt.addColumn(column.type,column.name);

                    if (column.type == "date")
                    {
                        for (var j = 0; j < _data%(id)s.rows.length; j++)
                        {
                            row = _data%(id)s.rows[j];
                            row[i] = new Date(row[i]);
                        }
                    }
                }

                var row;

                for (var i = 0; i < _data%(id)s.rows.length; i++)
                {
                    row = _data%(id)s.rows[i];
                    dt.addRow(row);
                }
            }

            if (_chart%(id)s != null)
            {
                var options = _options%(id)s;
                /*
                options["allowHtml"] = true;
                */
                //console.log(JSON.stringify(options));
                _chart%(id)s.draw(dt,options);
            }
        }

        if (Jupyter.notebook.kernel != null)
        {
            var _comm%(id)s = Jupyter.notebook.kernel.comm_manager.new_comm("%(id)s");

            _comm%(id)s.on_msg(function(msg)
            {
                var type = msg.content.data.type;

                if (type == "data")
                {
                    //console.log(msg.content.data.chartdata);
                    _data%(id)s = msg.content.data.chartdata;
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

        </script>

        ''' % dict(id=self._id,type=self._type,options=json.dumps(self._options))

        return(html)

    def getChartHtml(self):
        html = ""

        width = self.getOption("width","400")
        height = self.getOption("height","400")

        html += '''
        <div class='espapiContainer'>
        <div class='chart' id='%(id)s_div' style='width:%(width)spx;height:%(height)spx;position:relative'>
        </div>
        ''' % dict(id=self._id,width=width,height=height)

        if self._datasource.type == "updating":
            html += '''
            <div id='%(id)s_buttons' class='espButtons' style='display:none'>
                <table style='width:100%%'>
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
                </table>
            </div>

            ''' % dict(id=self._id)

        html += "</div>"
        html += "\n"

        return(html)

    def getData(self):
        data = {}

        if self._values == None:
            return;

        columns = []
        data["columns"] = columns

        if self._datasource.type == "updating":
            columns.append({"type":"string","name":"__key"})
        elif self._type == "timeseries":
            schema = self._datasource.schema;
            for field in self._datasource.schema.columns:
                if schema.isDateField(field):
                    columns.append({"type":"date","name":field})
                    break
                elif schema.isTimeField(field):
                    columns.append({"type":"date","name":field})
                    break
        else:     
            columns.append({"type":"number","name":"__counter"})

        for i in range(0,len(self._values)):
            value = self._values[i]
            column = {}
            type = self._datasource.schema.getFieldType(value)
            if self._datasource.schema.isNumericField(value):
                column["type"] = "number"
            else:
                column["type"] = type
            column["name"] = value;
            columns.append(column)

        rows = []
        data["rows"] = rows

        events = self._datasource.getData()

        updating = (self._datasource.type == "updating");

        if updating:
            for key,e in events.items():
                row = []

                for value in columns:
                    if value["name"] in e:
                        if value["type"] == "number":
                            row.append(float(e[value["name"]]))
                        else:
                            row.append(e[value["name"]])
                    else:
                        row.append(0)

                rows.append(row)
        else:
            for e in events:
                row = []

                for value in columns:
                    if value["name"] in e:
                        if value["name"] == "__timestamp":
                            ms = int(e[value["name"]])
                            ms = int(ms / 1000000);
                            timestamp = datetime.datetime.fromtimestamp(ms)
                            row.append(ms)
                        elif value["type"] == "number":
                            row.append(float(e[value["name"]]))
                        else:
                            row.append(e[value["name"]])
                    else:
                        row.append(0)
                rows.append(row)

        return(data)

    def getHeight(self):
        return(self.getOption("width","400"))

    def setHeight(self,value):
        self.setOption("height",value)

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
