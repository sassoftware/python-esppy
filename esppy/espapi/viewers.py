try:
    import graphviz as gv
except ImportError:
    raise ImportError("The graphviz module is required for exporting to graphs.")

from ..utils.notebook import scale_svg
import ipywidgets as widgets

import logging
import re

import esppy.espapi.tools as tools

from esppy.espapi.tools import Options

class ViewerBase(widgets.VBox,Options):
    def __init__(self,visuals,connection,**kwargs):
        widgets.VBox.__init__(self,layout=widgets.Layout(border=visuals.getOpt("border","0"),padding=visuals.getOpt("padding","10px"),margin=visuals.getOpt("margin","10px")))
        Options.__init__(self,**kwargs)
        self._visuals = visuals
        self._connection = connection

    def setWidth(self,value):
        self.setOpt("width",value)

    def setHeight(self,value):
        self.setOpt("height",value)

class ModelViewer(ViewerBase):
    def __init__(self,visuals,connection,**kwargs):
        ViewerBase.__init__(self,visuals,connection,**kwargs)
        self._stats = self._connection.getStats()
        self._data = None
        self._project = "*"
        self._model = None
        self._projects = None
        self._memory = None

        self._gradient = tools.Gradient("#ffffff",levels=100,min=0,max=100)

        self._windowColors = None

        colors = self._visuals._colors.getSpread(5)
        self._windowColors = {}
        for i,type in enumerate(["input","utility","analytics","textanalytics","transformation"]):
            self._windowColors[type] = colors[i]

        width = self.getOpt("width","98%",True)
        height = self.getOpt("height","400px",True)

        self._projects = widgets.Dropdown(description="Projects")

        self._projects.observe(self.setProject,names="value")

        w = []

        l1 = layout=widgets.Layout(margin="0px 0px 0px 10px")
        l2 = layout=widgets.Layout(margin="0px 10px 0px 5px")

        self._systemMem = widgets.Label(value="0",layout=l2)
        self._virtualMem = widgets.Label(value="0",layout=l2)
        self._residentMem = widgets.Label(value="0",layout=l2)

        w.append(widgets.Label(value="System Memory:",layout=l1))
        w.append(self._systemMem)
        w.append(widgets.Label(value="Virtual Memory:",layout=l1))
        w.append(self._virtualMem)
        w.append(widgets.Label(value="Resident Memory:",layout=l1))
        w.append(self._residentMem)

        cbLayout = widgets.Layout(max_width="100px")

        self._memory = widgets.HBox(w)

        showcpu = widgets.Checkbox(description="CPU",indent=False,layout=widgets.Layout(max_width="100px",margin="0px 0px 0px 50px"),value=self.getOpt("cpu"))
        showcounts = widgets.Checkbox(description="Counts",indent=False,layout=cbLayout,value=self.getOpt("counts"))
        showtypes = widgets.Checkbox(description="Types",indent=False,layout=cbLayout,value=self.getOpt("type"))
        showindices = widgets.Checkbox(description="Indices",indent=False,layout=cbLayout,value=self.getOpt("index"))
        showschema = widgets.Checkbox(description="Schema",indent=False,layout=cbLayout,value=self.getOpt("schema"))

        show = widgets.HBox([showcpu,showcounts,showtypes,showindices,showschema])

        box = widgets.HBox([self._projects,show])
        #top = widgets.VBox([box,self._memory],layout=widgets.Layout(border="1px solid #c0c0c0"))
        top = widgets.VBox([box,self._memory],layout=widgets.Layout(border="1px solid #c0c0c0",padding="10px",margin="5px 5px 0px 5px"))

        #self._graph = widgets.HTML(layout=widgets.Layout(border="1px solid #c0c0c0",overflow="auto",padding="10px"))
        self._graph = widgets.HTML(layout=widgets.Layout(border="1px solid #c0c0c0",overflow="auto",margin="5px"))
        #self._html = widgets.VBox([top,self._graph],layout=widgets.Layout(border="1px solid #c0c0c0"))
        #self._html = widgets.VBox([top,self._graph],layout=widgets.Layout(border="1px solid #c0c0c0"))

        showcpu.observe(self.showCpu,names="value")
        showcounts.observe(self.showCounts,names="value")
        showtypes.observe(self.showTypes,names="value")
        showindices.observe(self.showIndices,names="value")
        showschema.observe(self.showSchema,names="value")

        self.setStats()

        self._connection.loadModel(self)

        self.children = [top,self._graph]

    def setStats(self):
        cpu = self.getOpt("cpu",False)
        counts = self.getOpt("counts",False)
        cpuColor = self.getOpt("cpu_color")

        if cpu or counts or (cpuColor != None):
            if self._data == None:
                self._data = {}
                self._stats.addDelegate(self)
            self._stats.setOpt("counts",counts)
        else:
            self._data = None
            self._stats.removeDelegate(self)

    def showCpu(self,b):
        self.setOpt("cpu",b["new"])
        self.setStats()
        self.setContent()

    def showCounts(self,b):
        self.setOpt("counts",b["new"])
        self.setStats()
        self.setContent()

    def showTypes(self,b):
        self.setOpt("type",b["new"])
        self.setContent()

    def showIndices(self,b):
        self.setOpt("index",b["new"])
        self.setContent()

    def showSchema(self,b):
        self.setOpt("schema",b["new"])
        self.setContent()

    def setContent(self):
        content = self.build()
        if content != None:
            self._graph.value = content

    def modelLoaded(self,model,conn):
        self._model = model
        self.setContent()

    def handleStats(self,stats):
        self._data = {}
        data = stats.getData();
        memory = stats.getMemoryData();

        for o in data:
            key = o["project"]
            key += "/"
            key += o["contquery"]
            key += "/"
            key += o["window"]
            self._data[key] = o;

        if memory != None:
            self._systemMem.value = str(memory["system"])
            self._virtualMem.value = str(memory["virtual"])
            self._residentMem.value = str(memory["resident"])

        self.setContent()

    def build(self):
        if self._model == None:
            return(None)

        a = [("ALL","*")]

        if self._projects != None:
            for p in self._model._projects:
                a.append((p["name"],p["name"]))

            self._projects.options = a

            if self._project != "*":
                self._projects.value = self._project

        rankdir = "LR"

        opt = self.getOpt("orientation","horizontal")

        if opt == "vertical":
            rankdir = "TB"

        graphAttr = {}
        graphAttr["style"] = "filled,rounded"
        graphAttr["color"] = "#58a0d3"
        graphAttr["fillcolor"] = "#f8f8f8"
        graphAttr["rankdir"] = rankdir
        graphAttr["center"] = "true"

        nodeAttr = {}
        nodeAttr["shape"] = "rect"
        nodeAttr["fontname"] = "helvetica"
        nodeAttr["fontsize"] = "12"
        nodeAttr["style"] = "filled,bold"
        nodeAttr["color"] = "#58a0d3"
        nodeAttr["fillcolor"] = "#c8f0ff"

        edgeAttr = {}
        edgeAttr["splines"] = "spline"

        graphs = {}

        container = None

        if self._project == "*":

            container = gv.Digraph(graph_attr=graphAttr,node_attr=nodeAttr,edge_attr=edgeAttr,format="svg")

            for i,p in enumerate(self._model._projects):
                graph = gv.Digraph(name="cluster_" + str(i),graph_attr=graphAttr,node_attr=nodeAttr,edge_attr=edgeAttr,format="svg")
                graph.attr(label=p["name"],labeljust='l')
                graphs[p["name"]] = graph
        else:
            graph = gv.Digraph(name="cluster_1",graph_attr=graphAttr,node_attr=nodeAttr,edge_attr=edgeAttr,format="svg")
            #graph.attr(label=self._project,labeljust='l')
            graphs[self._project] = graph

        cpuColor = self.getOpt("cpu_color")

        if cpuColor != None:
            self._gradient.color = tools.Colors.getColorFromName(cpuColor)

        showCpu = self.getOpt("cpu",False)
        showCounts = self.getOpt("counts",False)
        showType = self.getOpt("type",False)
        showIndex = self.getOpt("index",False)
        showSchema = self.getOpt("schema",False)

        if self._memory != None:
            if showCpu:
                self._memory.layout.display = "flex"
            else:
                self._memory.layout.display = "none"

        showProperties = showCpu or showCounts or showType or showIndex

        for a in self._model._windows:
            if self._project == "*" or a["p"] == self._project:
                if a["type"] == "source" or a["type"] == "window-source" or len(a["incoming"]) > 0 or len(a["outgoing"]) > 0:
                    label = []
                    label.append("<<table border='0' cellspacing='0' cellpadding='0'>")
                    label.append("<tr><td>" + a["name"] + "</td></tr>")
                    if showProperties:
                        label.append("<tr><td><table border='0' cellspacing='0' cellpadding='1'>")
                    if showType:
                        label.append("<tr><td align='right'>type:</td><td> </td><td align='left'>" + a["type"] + "</td></tr>")
                    color = None
                    cpu = None
                    count = None
                    if self._data != None:
                        o = self._data.get(a["key"])
                        cpu = 0
                        count = 0
                        if o != None:
                            cpu = int(o["cpu"])
                            count = int(o["count"])

                        if showCpu:
                            label.append("<tr><td align='right'>cpu:</td><td> </td><td align='left'>" + "{0:5}".format(cpu) + "</td></tr>")
                        if cpuColor:
                            color = self._gradient.darken(cpu)
                        if showCounts:
                            label.append("<tr><td align='right'>count:</td><td> </td><td align='left'>" + "{0:5}".format(count) + "</td></tr>")
                    if showIndex:
                        label.append("<tr><td align='right'>index:</td><td> </td><td align='left'>" + a["index"] + "</td></tr>")
                    if showProperties:
                        label.append("</table></td></tr>")
                    if showSchema:
                        label.append("<tr><td>")
                        label.append("<table border='0' cellspacing='0' cellpadding='1'>")
                        label.append("<tr><td colspan='2'> </td></tr>")
                        label.append("<tr><td colspan='2'>Schema</td></tr>")
                        for f in a["schema"]._fields:
                            if f["isKey"]:
                                label.append("<tr><td align='left'>" + f["name"] + "*</td><td align='left'>" + f["type"] + "</td></tr>")
                        for f in a["schema"]._fields:
                            if f["isKey"] == False:
                                label.append("<tr><td align='left'>" + f["name"] + "</td><td align='left'>" + f["type"] + "</td></tr>")
                        label.append("</table>")
                        label.append("</td></tr>")
                    label.append("</table>>")
                    label = "".join(label)
                    if color == None:
                        if self._windowColors != None:
                            color = self._windowColors.get(a["class"])

                    graph = graphs[a["p"]]

                    if color != None:
                        graph.node(a["key"],label,fillcolor=color)
                    else:
                        graph.node(a["key"],label)
                        
                    for z in a["outgoing"]:
                        graph.edge(a["key"],z["key"])

        if container != None:
            for key,value in graphs.items():
                container.subgraph(value)
            return(container._repr_svg_())

        return(graph._repr_svg_())

    def setProject(self,change):
        self.project = self._projects.value

    def dataChanged(self,datasource,data,clear):
        pass

    @property
    def project(self):
        return(self._project)

    @project.setter
    def project(self,value):
        self._project = value;
        self.setContent()

class LogViewer(ViewerBase):
    def __init__(self,visuals,connection,**kwargs):
        ViewerBase.__init__(self,visuals,connection,**kwargs)

        width = self.getOpt("width","98%")
        height = self.getOpt("height","200px")

        self._max = self.getOpt("max",50);

        self._bg = self.getOpt("bg","#f8f8f8")
        self._border = self.getOpt("border","1px solid #d8d8d8")

        components = []
        self._log = widgets.HTML(value="",layout=widgets.Layout(width=width,height=height,border=self._border,overflow="auto"))
        components.append(self._log)

        self._filter = self.getOpt("filter")
        self._regex = None

        if self._filter != None:
            self._filterText = widgets.Text(description="Filter",value=self._filter,layout=widgets.Layout(width="70%"))
            if len(self._filter) > 0:
                self._regex = re.compile(self._filter,re.I)
            setButton = widgets.Button(description="Set")
            clearButton = widgets.Button(description="Clear")
            setButton.on_click(self.filter)
            clearButton.on_click(self.clearFilter)
            components.append(widgets.HBox([self._filterText,setButton,clearButton]))

        self._box = widgets.VBox(components,layout=widgets.Layout(width="100%"))

        s = ""
        s += "<div style='width:100%;height:100%;background:" + self._bg + "'>"
        s += "</div>"
        self._log.value = s

        self._messages = []

        self._connection.getLog().addDelegate(self)
        
        self.children = [self._box]

    def handleLog(self,connection,message):
        self._messages.append(message)

        if self._max != None and len(self._messages) > self._max:
            diff = len(self._messages) - self._max

            for i in range(0,diff):
                self._messages.pop(self._max + i)

        self.load()

    def load(self):
        s = ""

        s += "<div style='width:100%;height:100%;background:" + self._bg + "'>"
        s += "<pre style='width:100%;height:100%;background:" + self._bg + "'>"

        #for message in self._messages:
        for message in reversed(self._messages):
            if self._regex != None:
                if self._regex.search(message) == None:
                    continue
                
            s += message;
            s += "\n"
            s += "\n"

        s += "</pre>"
        s += "</div>"

        self._log.value = s

    def filter(self,b):
        self._filter = self._filterText.value.strip()
        if len(self._filter) == 0:
            self._filter = None
        else:
            self._regex = re.compile(self._filter,re.I)
        self.load()

    def clearFilter(self,b):
        self._filterText.value = ""
        self._filter = None
        self._regex = None
        self.load()

class StatsViewer(ViewerBase):
    def __init__(self,visuals,connection,**kwargs):
        ViewerBase.__init__(self,visuals,connection,**kwargs)
        self._stats = self._connection.getStats();
        self._stats.addDelegate(self)

        width = self.getOpt("width","90%",True)
        height = self.getOpt("height","400px",True)

        self._stats.setOpts(**self.options)

        self._log = widgets.HTML()

        self.children = [self._log]

    def handleStats(self,stats):
        data = stats.getData()
        s = ""
        s += "<table border='1' cellspacing='0' cellpadding='4'>"
        s += "<tr>"
        s += "<td>Project</td>"
        s += "<td>Contquery</td>"
        s += "<td>Window</td>"
        s += "<td>CPU</td>"
        s += "</tr>"

        for o in data:
            if o["cpu"] >= 1:
                s += "<tr>"
                s += "<td>" + o["project"] + "</td>"
                s += "<td>" + o["contquery"] + "</td>"
                s += "<td>" + o["window"] + "</td>"
                s += "<td>" + str(int(o["cpu"])) + "</td>"
                s += "</tr>"

        s += "</table>"

        self._log.value = s
