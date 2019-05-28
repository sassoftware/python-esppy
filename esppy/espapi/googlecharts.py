from IPython.display import display, Javascript
from ipykernel.comm import Comm
import datetime
import esppy.espapi.api
import uuid
import json
import re

class Chart(object):
    _chartsLoaded = False

    def __init__(self,datasource,type,**kwargs):
        self._id = str(uuid.uuid4()).replace('-', '_')
        self._type = type
        self._values = None
        self._datasource = datasource
        self._info = self._datasource.getInfo()
        self._data = self.getData()
        self._options = {}
        self._datasource.addChangeDelegate(self)
        self._comm = None

    def dataChanged(self,datasource):
        print("changed chart")
        self.data = self.getData()

    def infoChanged(self,datasource):
        pass

    def target(self,comm,msg):
        self._comm = comm
        self.data = self.getData()
        @comm.on_msg
        def _recv(msg):
            self._datasource.handleMessage(msg["content"]["data"])

    def _repr_html_(self):
        get_ipython().kernel.comm_manager.register_target(self._id,self.target)
        self.data = self.getData()

        html = ""

        if Chart._chartsLoaded == False:
            html += "<script type='text/javascript' src='https://www.gstatic.com/charts/loader.js'></script>"
            Chart._chartsLoaded = True

        html += '''

        <style type="text/css">

        .espapiContainer
        {
            width:100%%;
        }

        .espapiContainer td.espButton
        {
            background:white;
        }

        div#%(id)s_div
        {
            border:5px solid red;
        }

        </style>
        <table class="espapiContainer">
        <tr><td class="espChart"><div id="%(id)s_div" 
        ''' % dict(id=self._id)

        html += '''
        style="width:100%;background:white"></div></td></tr>
        '''

        if self._datasource.type == "updating":
            html += '''
            <tr>
                <td>
                <table>
                <tr>
                <td class="espButton"><button onclick="javascript:send_%(id)s('prev')">Prev</button></td>
                <td class="espButton"><button onclick="javascript:send_%(id)s('next')">Next</button></td>
                <td class="espButton"><button onclick="javascript:send_%(id)s('first')">First</button></td>
                <td class="espButton"><button onclick="javascript:send_%(id)s('last')">Last</button></td>
                </tr>
                </table>
                </td>
            </tr>
            ''' % dict(id=self._id)

        html += '''
        </table>

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
                if ("%(type)s" == "column")
                {
                    _chart%(id)s = new google.visualization.ColumnChart(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "bar")
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
                else if ("%(type)s" == "orgchart")
                {
                    _chart%(id)s = new google.visualization.OrgChart(document.getElementById("%(id)s_div"));
                }
                else if ("%(type)s" == "map")
                {
                    _chart%(id)s = new google.visualization.Map(document.getElementById("%(id)s_div"));
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
                options["allowHtml"] = true;
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

    def getData(self):
        data = {}

        if self._values == None:
            return;

        columns = []
        data["columns"] = columns

        if self._datasource.type == "updating":
            columns.append({"type":"string","name":"__key"})
        else:     
            #columns.append({"type":"date","name":"__timestamp"})
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

        self.data = self.getData()

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

    @property
    def type(self):
        return(self._type)

class Bar(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"bar",**kwargs)

class Column(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"column",**kwargs)

class Line(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"line",**kwargs)

class Pie(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"pie",**kwargs)

class Table(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"table",**kwargs)

class Area(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"area",**kwargs)

class Gauge(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"gauge",**kwargs)

class Bubble(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"bubble",**kwargs)

class Tree(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"tree",**kwargs)

class OrgChart(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"orgchart",**kwargs)

class StatsChart(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"orgchart",**kwargs)

    def getData(self):
        data = {}
        rows = []
        columns = []

        columns.append({"type":"string","name":"a"})
        columns.append({"type":"string","name":"b"})
        columns.append({"type":"string","name":"c"})

        rows.append([{"v":'Mike', "f":'Mike<div style="color:red; font-style:italic">President</div>'}, '', 'The President'])
        rows.append([{"v":'Jim', "f":'Jim<div style="color:red; font-style:italic">Vice President</div>'}, 'Mike', 'VP'])
        rows.append(['Alice', 'Mike', ''])
        rows.append(['Bob', 'Jim', 'Bob Sponge'])
        rows.append(['Carol', 'Bob', ''])
        '''
        '''

        data["rows"] = rows
        data["columns"] = columns

        return(data)

class Map(Chart):
    def __init__(self,datasource,**kwargs):
        Chart.__init__(self,datasource,"map",**kwargs)
        self._latitude = None
        self._longitude = None

    @property
    def latitude(self):
        return(self._latitude)

    @latitude.setter
    def latitude(self,value):
        self._latitude = value

    @property
    def longitude(self):
        return(self._longitude)

    @longitude.setter
    def longitude(self,value):
        self._longitude = value

    def getData(self):
        data = {}

        if self._latitude == None or self._longitude == None:
            return(data)

        columns = []
        data["columns"] = columns
        columns.append({"type":"number","name":"Lat"})
        columns.append({"type":"number","name":"Lon"})

