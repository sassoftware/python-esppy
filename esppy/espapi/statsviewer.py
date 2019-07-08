import ipywidgets as widgets
import logging

import esppy.espapi.tools as tools

class StatsViewer(object):
    def __init__(self,connection,**kwargs):
        self._connection = connection
        self._options = tools.Options(**kwargs)
        self._stats = self._connection.getStats();
        self._stats.addDelegate(self)

        width = self._options.get("width","90%",True)
        height = self._options.get("height","400px",True)

        self._stats.setOptions(**self._options.options)

        self._log = widgets.HTML()

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
            s += "<tr>"
            s += "<td>" + o["project"] + "</td>"
            s += "<td>" + o["contquery"] + "</td>"
            s += "<td>" + o["window"] + "</td>"
            s += "<td>" + str(o["cpu"]) + "</td>"
            s += "</tr>"

        s += "</table>"

        self._log.value = s

    def display(self):
        return(self._log)
