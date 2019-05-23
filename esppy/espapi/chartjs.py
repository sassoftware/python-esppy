from IPython.display import display, Javascript
from ipykernel.comm import Comm
import esppy.espapi.api
import uuid
import json
import re

class Chart(object):
    def __init__(self,datasource,**kwargs):
        self._id = str(uuid.uuid4()).replace('-', '_')
        self._type = "bar" 
        self._values = None
        self._datasource = datasource
        self._info = self._datasource.getInfo()
        self._opts = esppy.espapi.api.Options(**kwargs)
        self._data = self.getData()
        self._options = self.getOptions()
        print(self._options);
        self._datasource.addChangeDelegate(self)
        self._comm = None

    def dataChanged(self,collection):
        self.data = self.getData()

    def infoChanged(self,collection):
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

        width = self._opts.get("width",500)
        height = self._opts.get("height",200)
        html = '''

        <table>
        <tr><canvas id="%(id)s_canvas" width="' + %(width)s + '" height="' + %(height)s + '"></canvas>'</tr>
        <tr>
            <td><button onclick="javascript:send_%(id)s('prev')">Prev</button></td>
            <td><button onclick="javascript:send_%(id)s('next')">Next</button></td>
            <td><button onclick="javascript:send_%(id)s('first')">First</button></td>
            <td><button onclick="javascript:send_%(id)s('last')">Last</button></td>
        </tr>
        </table>
        <script language="javascript">
        function
        send_%(id)s(type)
        {
            _comm%(id)s.send({"type":type});
        }

        var _comm%(id)s = Jupyter.notebook.kernel.comm_manager.new_comm("%(id)s");

        _comm%(id)s.on_msg(function(msg)
        {
            var type = msg.content.data.type;

            if (type == "data")
            {
                if (_chart%(id)s != null)
                {
                    _chart%(id)s.data = msg.content.data.chartdata;
                    _chart%(id)s.update(0);
                }
            }
            else
            {
                console.log("MSG: " + JSON.stringify(msg.content));
            }
        });

        var _chart%(id)s = null;

        requirejs.config({
            paths: {
                Chart: ['//cdnjs.cloudflare.com/ajax/libs/Chart.js/2.8.0/Chart.min']
            }
        });

        require(['Chart'], function(Chart) {
        var context = document.getElementById("%(id)s_canvas").getContext("2d");
        _chart%(id)s = new Chart(context,{type:"%(type)s",options:%(options)s});
        })
        </script>

        ''' % dict(id=self._id,type=self._type,width=width,height=height,options=json.dumps(self._options))

        return(html)

    def getData(self):
        data = {}
        data["labels"] = []

        if self._values == None:
            return;

        datasets = []
        data["datasets"] = datasets

        backgrounds = [
            "rgba(0,0,255,0.2)",
            "rgba(255,0,0,0.2)",
            "rgba(255,0,255,0.2)",
            "rgba(255,255,0,0.2)"
        ]
        for i in range(0,len(self._values)):
            value = self._values[i]
            ds = {}
            ds["label"] = value
            ds["data"] = []
            ds["backgroundColor"] = backgrounds[i]
            datasets.append(ds)

        events = self._datasource.getData()

        updating = (self._datasource.type == "updating");

        if updating:
            for key,e in events.items():
                data["labels"].append(e["__key"])

                for i in range(0,len(self._values)):
                    ds = datasets[i]

                    value = self._values[i]

                    if value in e:
                        ds["data"].append(e[value])
                    else:
                        ds["data"].append(0)
        else:
            for e in events:
                data["labels"].append(e["__timestamp"])

                for i in range(0,len(self._values)):
                    ds = datasets[i]

                    value = self._values[i]

                    if value in e:
                        ds["data"].append(e[value])
                    else:
                        ds["data"].append(0)

        return(data)

    def getOptions(self):
        options = {}
        '''
        if self.isStreaming:
            scales = {}
            scales["xAxes"] = [{"type":"time"}]
            options["scales"] = scales
        '''
        return(options)

    @property
    def values(self):
        return(self._values)

    @values.setter
    def values(self,value):

        self._values = []

        if type(value) is list:
            for v in values:
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
    def type(self):
        return(self._type)

    @type.setter
    def type(self,value):
        self._type = value;
