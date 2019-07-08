try:
    import graphviz as gv
except ImportError:
    raise ImportError("The graphviz module is required for exporting to graphs.")

from ..utils.notebook import scale_svg
import ipywidgets as widgets

import matplotlib

import seaborn as sns

import logging

import esppy.espapi.tools as tools

class ModelViewer(object):
    def __init__(self,connection,**kwargs):
        self._connection = connection
        self._options = tools.Options(**kwargs)
        self._stats = None
        self._connection.loadModel(self)
        self._data = None
        self._project = "*"
        self._model = None

        if self._options.has("style"):
            matplotlib.style.use(self._options.get("style"))

        if self._options.has("palette"):
            sns.set_palette(self._options.get("palette"))

        self._colors = None

        palette = sns.color_palette()
        if palette != None:
            colors = palette.as_hex()
            self._colors = {}
            self._colors["input"] = colors[0]
            self._colors["transformation"] = colors[1]
            self._colors["utility"] = colors[2]
            self._colors["analytics"] = colors[3]
            self._colors["textanalytics"] = colors[4]

        width = self._options.get("width","98%",True)
        height = self._options.get("height","400px",True)

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

        self._memory = widgets.HBox(w)

        banner = widgets.HBox([self._projects,self._memory])

        self._graph = widgets.HTML(background="red",layout=widgets.Layout(border="1px solid #c0c0c0",overflow="auto",padding="20px"))
        self._html = widgets.VBox([banner,self._graph])

        if self._options.get("showstats",False):
            self.showStats()

    def setOptions(self,**kwargs):
        self._options.setOptions(**kwargs)

    def setContent(self):
        content = self.build()
        if content != None:
            self._graph.value = content

    def showStats(self):
        if self._stats == None:
            self._data = {}
            self._stats = self._connection.getStats()
            self._stats.addDelegate(self)

    def hideStats(self):
        if self._stats != None:
            self._data = None
            self._stats.removeDelegate(self)
            self._stats = None

    def modelLoaded(self,model,conn):
        self._model = model
        self.setContent()

    def handleStats(self,stats):
        self._data = {}
        data = stats.getData();

        for o in data["stats"]:
            key = o["project"]
            key += "/"
            key += o["contquery"]
            key += "/"
            key += o["window"]
            self._data[key] = o;

        if "memory" in data:
            memory = data["memory"]
            if memory != None:
                self._systemMem.value = str(memory["system"])
                self._virtualMem.value = str(memory["virtual"])
                self._residentMem.value = str(memory["resident"])

        self.setContent()

    def build(self):
        if self._model == None:
            return(None)

        a = [("ALL","*")]

        for p in self._model._projects:
            a.append((p["name"],p["name"]))

        self._projects.options = a

        rankdir = "LR"

        opt = self._options.get("orientation","horizontal")

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

        cpuColor = self._options.get("cpucolor")

        showStats = self._options.get("showstats",False)
        showType = self._options.get("showtype",False)
        showIndex = self._options.get("showindex",False)

        if showStats:
            self._memory.layout.display = "flex"
        else:
            self._memory.layout.display = "none"

        showProperties = showStats or showType or showIndex

        for a in self._model._windows:
            if self._project == "*" or a["p"] == self._project:
                if a["type"] == "source" or a["type"] == "window-source" or len(a["incoming"]) > 0 or len(a["outgoing"]) > 0:
                    label = []
                    label.append("<<table border='0' cellspacing='0' cellpadding='0'>")
                    label.append("<tr><td>" + a["name"] + "</td></tr>")
                    if showProperties:
                        label.append("<tr><td><table border='0' cellspacing='0' cellpadding='1'>")
                    if showType:
                        label.append("<tr><td align='right'>type:</td><td>&nbsp;</td><td align='left'>" + a["type"] + "</td></tr>")
                    color = None
                    cpu = None
                    if self._data != None:
                        o = self._data.get(a["key"])
                        cpu = 0
                        if o != None:
                            cpu = int(o["cpu"])

                        label.append("<tr><td align='right'>cpu:</td><td>&nbsp;</td><td align='left'>" + str(cpu) + "</td></tr>")
                        if cpuColor != None:
                            color = tools.darken(cpuColor,cpu)
                    if showIndex:
                        label.append("<tr><td align='right'>index:</td><td>&nbsp;</td><td align='left'>" + a["index"] + "</td></tr>")
                    if showProperties:
                        label.append("</table></td></tr>")
                    label.append("</table>>")
                    label = "".join(label)
                    if color == None:
                        if self._colors != None:
                            color = self._colors.get(a["class"])

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

    @property
    def project(self):
        return(self._project)

    @project.setter
    def project(self,value):
        self._project = value;
        self.setContent()

    def display(self):
        return(self._html)
