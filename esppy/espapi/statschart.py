try:
    import graphviz as gv
except ImportError:
    raise ImportError("The graphviz module is required for exporting to graphs.")

from ..utils.notebook import scale_svg
from IPython.display import display, Javascript
from ipykernel.comm import Comm
import uuid

class StatsChart(object):
    def __init__(self,server):
        self._id = str(uuid.uuid4()).replace("-", "_")
        self._server = server
        self._stats = self._server.getStats(interval=4)
        self._stats.addChangeDelegate(self)
        self._data = {}
        self._project = "*"
        self._comm = None

        self._colors = {}
        self._colors["input"] = "#7BCAE1"
        self._colors["transformation"] = "#8BFEA8"
        self._colors["utility"] = "#FFC895"
        self._colors["analytics"] = "#DDB9B9"
        self._colors["textanalytics"] = "#C0E0DA"
        self._stats.start()

    def target(self,comm,msg):
        self._comm = comm
        self.sendData()
        @comm.on_msg
        def _recv(msg):
            pass

    def dataChanged(self,datasource):
        self._data = {}
        data = datasource.getData();

        for o in data:
            key = o["project"]
            key += "/"
            key += o["contquery"]
            key += "/"
            key += o["window"]
            self._data[key] = o;

        #self.sendData()

    def sendData(self):
        if self._comm != None:
            o = {}
            o["type"] = "data"
            o["chartdata"] = self.build()._repr_svg_()
            self._comm.send(o)

    def _repr_html_(self):
        get_ipython().kernel.comm_manager.register_target(self._id,self.target)
        html = ""

        html += '''
        <table>
        <tr><td><div id="%(id)s_div"></div></td></tr>
        <tr><td><button onclick="javascript:zoom_%(id)s()">Zoom</button></td></tr>
        </table>
        <script language="javascript">

        var _comm%(id)s = Jupyter.notebook.kernel.comm_manager.new_comm("%(id)s");

        function
        send_%(id)s(type)
        {
            _comm%(id)s.send({"type":type});
        }

        function
        zoom_%(id)s()
        {
            if (_svg != null)
            {
                console.log("SVG: " + _svg.viewBox.baseVal.width);
                /*
                _svg.width = 2000;
                _svg.width.baseVal.width = "2000pt";
                _svg.viewBox.baseVal.width = 500;
                _svg.viewBox.baseVal.height = 500;
                */
                _svg.width.baseVal.width = "9000pt";
                _svg.viewBox.baseVal.width = 100;
                _svg.viewBox.baseVal.height = 100;
                console.log("HERE: ",_svg.width);
            }
        }

        var _svg = null;

        _comm%(id)s.on_msg(function(msg)
        {
            var type = msg.content.data.type;

            if (type == "data")
            {
                //console.log(msg.content.data.chartdata);
                document.getElementById("%(id)s_div").innerHTML = msg.content.data.chartdata;
                _svg = document.getElementById("%(id)s_div").firstChild;

                var nodes = document.getElementById("%(id)s_div").childNodes;
                var node;

                for (var i = 0; i < nodes.length; i++)
                {
                    node = nodes[i];
                    if (node.nodeName == "svg")
                    {
                        _svg = node;
                        console.log(_svg.outerHTML);
                        console.log("width: " + _svg.offsetWidth);
                        console.log("height: " + _svg.getAttribute("height"));
                        console.log("VIEWBOX: " + _svg.getAttribute("viewBox"));
                        _svg.style.overflow = "auto";
                        break;
                    }
                }
            }
            else
            {
                console.log("MSG: " + JSON.stringify(msg.content));
            }
        });

        //send_%(id)s("init");

        </script>

        ''' % dict(id=self._id)

        return(html)

    def build(self):
        self._graph = gv.Digraph(format="svg")
        self._graph.attr("node",shape="rect",fontname="tahoma")
        self._graph.attr("graph",rankdir="LR",center="false")
        self._graph.attr("edge",fontname="times-italic")
        self._graph.attr(label=self._project,labeljust="l",style="filled,bold,rounded",color="#c0c0c0",fillcolor="#dadada",fontcolor="black")
        for a in self._server.windows:
            if self._project == "*" or a["p"] == self._project:
                if a["type"] == "source" or a["type"] == "window-source" or len(a["incoming"]) > 0 or len(a["outgoing"]) > 0:
                    text = ""
                    text += a["name"]
                    text += " ("
                    text += a["type"]
                    text += ")"
                    o = self._data.get(a["key"])
                    if o != None:
                        text += "\n"
                        text += "CPU: " + str(o["cpu"])
                    color = self._colors.get(a["class"])
                    self._graph.node(a["key"],text,style="filled",color=color)
                    #self._graph.node(a["key"],label=text,labeljust="l",style="filled,bold,rounded",color="#c0c0c0",fillcolor="#dadada",fontcolor="black")
                    for z in a["outgoing"]:
                        self._graph.edge(a["key"],z["key"])
        return(self._graph)

    @property
    def project(self):
        return(self._project)

    @project.setter
    def project(self,value):
        self._project = value;
        self.sendData()
