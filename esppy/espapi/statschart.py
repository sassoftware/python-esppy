try:
    import graphviz as gv
except ImportError:
    raise ImportError("The graphviz module is required for exporting to graphs.")

from ..utils.notebook import scale_svg
from IPython.display import display, Javascript
from ipykernel.comm import Comm
import ipywidgets as widgets

import logging

import esppy.espapi.tools as tools

class StatsChart(object):
    def __init__(self,connection,**kwargs):
        self._connection = connection
        self._options = tools.Options(**kwargs)
        self._stats = self._connection.getStats()
        self._stats.addDelegate(self)
        self._connection.loadModel(self)
        self._data = {}
        self._project = "*"
        self._comm = None
        self._model = None

        self._colors = {}
        self._colors["input"] = "#7BCAE1"
        self._colors["transformation"] = "#8BFEA8"
        self._colors["utility"] = "#FFC895"
        self._colors["analytics"] = "#DDB9B9"
        self._colors["textanalytics"] = "#C0E0DA"

        width = self._options.get("width","90%",True)
        height = self._options.get("height","400px",True)

        self._graph = widgets.HTML(layout=widgets.Layout(width=width,height=height))

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

        for o in data:
            key = o["project"]
            key += "/"
            key += o["contquery"]
            key += "/"
            key += o["window"]
            self._data[key] = o;

        self.setContent()

    def build(self):
        if self._model == None:
            return(None)

        graph = gv.Digraph(format="svg")
        graph.attr("node",shape="rect",fontname="helvetica")
        graph.attr("graph",rankdir="LR",center="false")
        graph.attr("edge",fontname="times-italic")
        graph.attr(label=self._project,labeljust="l",style="filled,bold,rounded",color="#c0c0c0",fillcolor="#dadada",fontcolor="black")
        for a in self._model._windows:
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
                        text += "CPU: " + str(int(o["cpu"]))
                    #color = self._colors.get(a["class"])
                    #graph.node(a["key"],text,style="filled",color=color)
                    graph.node(a["key"],label=text,labeljust="l",style="filled,bold,rounded",color="#c0c0c0",fillcolor="#dadada",fontcolor="black")
                    for z in a["outgoing"]:
                        graph.edge(a["key"],z["key"])
        return(graph._repr_svg_())

    @property
    def project(self):
        return(self._project)

    @project.setter
    def project(self,value):
        self._project = value;
        self.setContent()

    def display(self):
        return(self._graph)
