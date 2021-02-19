from plotly import tools as ptools
import plotly.io as pio

import plotly.graph_objs as go

import esppy.espapi.connections as connections
import esppy.espapi.tools as tools
import esppy.espapi.viewers as viewers

from esppy.espapi.tools import Options

import esppy.espapi.colors as colors

from base64 import b64encode

import sys

import threading

import datetime

import base64

import ipywidgets as widgets
import ipyleaflet as maps

import numpy as np

import logging
import re
import math
import random

pio.templates.default = "none"

class Visuals(Options):

    _dataHeader = "_data://"

    def __init__(self,**kwargs):
        Options.__init__(self,**kwargs)
        self._visuals = []
        self._border = None

        if self.hasOpt("colormap"):
            self._colors = colors.Colors(colormap=self.getOpt("colormap"))
        elif self.hasOpt("colors"):
            self._colors = colors.Colors(colors=self.getOpt("colors"))
        else:
            self._colors = colors.Colors(colormap="")

        self._chartStyle = tools.Options()
        self._css = self.getOpt("css","visuals")

        self._headerStyle = tools.Options(font_family="Arial, Helvetica, sans-serif",font_size="12pt")

        self._dashboardLayout = tools.Options()

        #self.setDashboardLayout(border="1px solid #c0c0c0",padding="5px")

        self._axisWidth = 1

    def setDashboardLayout(self,**kwargs):
        opts = tools.Options(**kwargs)
        for key,value in kwargs.items():
            self._dashboardLayout.setOpt(key,value)

    def setLayoutValues(self,layout,opts):
        for key,value in opts.items():
            if key == "width":
                layout.width = value
            elif key == "height":
                layout.height = value
            elif key == "border":
                layout.border = value
            elif key == "margin":
                layout.margin = value
            elif key == "padding":
                layout.padding = value

    def optionSet(self,name,value):
        if name == "colormap":
            colormap = value
            self._colors = colors.Colors(colormap=colormap)

    def setTitleStyle(self,**kwargs):
        self._headerStyle.setOpts(**kwargs)

    def setChartStyle(self,**kwargs):
        self._chartStyle.setOpts(**kwargs)

    def createBarChart(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = BarChart(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createLineChart(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = LineChart(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createScatterPlot(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = ScatterPlot(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createTimeSeries(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = TimeSeries(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createBubbleChart(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = BubbleChart(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createPieChart(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = PieChart(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createMap(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = Map(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createGauge(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = Gauge(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createCompass(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = Compass(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createTable(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = Table(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createImageViewer(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = ImageViewer(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createImages(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        chart = Images(self,datasource,layout,**kwargs)
        chart.create()
        self._visuals.append(chart)
        return(chart)

    def createControls(self,datasource,layout = None,**kwargs):
        datasource.addDelegate(self)
        controls = Controls(self,datasource,layout,**kwargs)
        controls.create()
        self._visuals.append(controls)
        return(controls)

    def createDashboard(self,**kwargs):
        box = Dashboard(self,layout=widgets.Layout(width="100%"),**kwargs)
        return(box)

    def createWrapper(self,widget,layout = None,**kwargs):
        wrapper = Wrapper(self,widget,layout,**kwargs)
        self._visuals.append(wrapper)
        wrapper.create()
        return(wrapper)

    def createModelViewer(self,connection,layout = None,**kwargs):
        return(viewers.ModelViewer(self,connection,layout,**kwargs))

    def createLogViewer(self,connection,layout = None,**kwargs):
        return(viewers.LogViewer(self,connection,layout,**kwargs))

    def createStatsViewer(self,connection,layout = None,**kwargs):
        return(viewers.StatsViewer(self,connection,layout,**kwargs))

    def dataChanged(self,datasource,data,clear):
        for v in self._visuals:
            if v._datasource == datasource:
                v.draw(data,clear)

    def infoChanged(self,datasource):
        for v in self._visuals:
            if v._datasource == datasource:
                v.info(datasource.getInfo())

    def handleStats(self,datasource):
        for v in self._visuals:
            if v._datasource == datasource:
                v.draw()

    def clear(self):
        self._visuals = []

    def getHeaderStyle(self):
        content = ""
        i = 0
        for k,v in self._headerStyle.options.items():
            s = k.replace("_","-")
            if i > 0:
                content += ";"
            content += s + ":" + v
            i += 1 
        return(content)
 
    def formatTitle(self,text):
        content = ""
        content += "<div style='"
        content += self.getHeaderStyle()
        content += ";text-align:center"
        content += "'>"
        if text == None:
            content += "&nbsp;"
        else:
            content += text
        content += "</div>"

        return(content)

    @property
    def css(self):
        return(self._css)

class Chart(Options,widgets.Box):
    def __init__(self,visuals,datasource = None,layout = None,**kwargs):
        Options.__init__(self,**kwargs)

        if layout != None:
            widgets.Box.__init__(self,layout=layout)
        else:
            widgets.Box.__init__(self)

        if self.hasOpt("width"):
            self.layout.width = self.getOpt("width")

        if self.hasOpt("height"):
            self.layout.height = self.getOpt("height")

        self._visuals = visuals
        self._datasource = datasource

        self.add_class(self._visuals.css + "_chart")

        self._class = self.getOpt("css")

        if self._class != None:
            self.add_class(self._class)

        self._container = widgets.Box(layout=widgets.Layout(width="100%",display="inline_flex",flex_flow="column",justify_content="center"))

        self._container.add_class(self._visuals.css + "_container")
        if self._class != None:
            self._container.add_class(self._class + "_container")

        self._header = widgets.HTML(layout=widgets.Layout(overflow="hidden",width="100%",margin="0"))
        self._header.add_class(self._visuals.css + "_header")
        if self._class != None:
            self._header.add_class(self._class + "_header")

        self._content = widgets.Box(layout=widgets.Layout(width="100%",display="inline_flex",flex_flow="row wrap",justify_content="center",align_items="center"))
        #self._content = widgets.Box(layout=widgets.Layout(width="100%",display="inline_flex",flex_flow="row wrap",justify_content="center",align_items="flex-start"))
        self._content.add_class(self._visuals.css + "_content")
        if self._class != None:
            self._content.add_class(self._class + "_content")

        self._footer = widgets.Box(layout=widgets.Layout(width="100%"))
        self._footer.add_class(self._visuals.css + "_footer")
        if self._class != None:
            self._footer.add_class(self._class + "_footer")
        if True:
            self._footer.children = [widgets.Text(value="footer")]

        self.children = [self._container]
        self._figure = None
        self._data = None
        self._layout = None
        self._controls = None

        self._chartStyle = tools.Options()

        self._delegates = []

        self.setDisplay()

    def addDelegate(self,delegate):
        tools.addTo(self._delegates,delegate)
        if self._figure is not None:
            self.draw()

    def removeDelegate(self,delegate):
        tools.removeFrom(self._delegates,delegate)
        if self._figure is not None:
            self.draw()

    def setDisplay(self):
        children = []
        height = 100
        if self.getOpt("show_header",True):
            children.append(self._header)
            height -= 10
        children.append(self._content)
        if self.getOpt("show_controls",False):
            if self._controls == None:
                self._controls = ControlPanel(self._datasource) 
            self._footer.children = [self._controls]
            children.append(self._footer)
            height -= 10
        self._content.layout.height = str(height) + "%"
        self._container.children = children

    def setOpts(self,**kwargs):
        Options.setOpts(self,**kwargs)
        self.create()

    def setChartStyle(self,**kwargs):
        self._chartStyle.setOpts(**kwargs)

    def setWidth(self,value):
        self.setOpt("width",value)

    def setHeight(self,value):
        self.setOpt("height",value)

    def createContent(self):
        pass

    def create(self):
        self.createContent()

        if self._data != None:
            self._layout = go.Layout()

            margin = 20
            self._layout["margin"] = dict(l=margin,r=margin,b=margin,t=margin)

            xRange = self.getOpt("xrange")
            if xRange != None:
                self._layout["xaxis"]["range"] = xRange
            yRange = self.getOpt("yrange")
            if yRange != None:
                self._layout["yaxis"]["range"] = yRange

            self._layout["xaxis"]["showticklabels"] = self.getOpt("showticks",True)
            self._layout["xaxis"]["showline"] = False

            self.addChartStyle()

            self._figure = go.FigureWidget(data=self._data,layout=self._layout)
            self._figure.add_class(self._visuals.css + "_figure")

            self._content.children = [self._figure]

        self.draw(None,True)

    def addChartStyle(self):
        for name,value in self._visuals._chartStyle.items():
            self._layout[name] = value
        for name,value in self._chartStyle.items():
            self._layout[name] = value

    def setTitle(self,title = None):

        if title == None:
            title = self.getOpt("title")

        if self._datasource != None:
            if title == None:
                title = self._datasource._path

            if isinstance(self._datasource,connections.EventCollection):
                if self._datasource._pages > 1:
                        title += " (Page " + str(self._datasource._page + 1) + " of " + str(self._datasource._pages) + ")"

            filter = self._datasource.getOpt("filter")

            if filter != None:
                title += "<br>"
                title += filter

        self._header.value = self._visuals.formatTitle(title)

    def createMarkers(self):
        o = {}
        marker = {}

        keys = self._datasource.getKeyValues()

        text = []

        for i,key in enumerate(keys):
            text.append(key)

        size = None

        value = self.getOpt("size")

        if value != None: 
            try:
                num = int(value)
                marker["size"] = num
            except:
                size = value

        color = self.getOpt("color")

        if size != None or color != None:

            if size != None:
                s = self._datasource.getValues(size)
                if s != None and len(s) > 0:
                    maxsize = 60.
                    minsize = 5
                    marker["size"] = s
                    marker["sizemode"] = "area"
                    marker["sizeref"] = 2. * max(s) / (maxsize ** 2)
                    marker["sizemin"] = minsize

                    for i,v in enumerate(s):
                        text[i] += "<br>"
                        text[i] += size + "=" + str(v)

            color = self.getOpt("color")

            if color != None:
                s = self._datasource.getValues(color)
                if s != None and len(s) > 0:
                    marker["color"] = s
                    marker["showscale"] = True
                    marker["colorscale"] = self._visuals._colors.colorscale

                    if size == None or color != size:
                        for i,v in enumerate(s):
                            if size != None and v in size == False:
                                text[i] += "<br>"
                            text[i] += color + "=" + str(v)

        #marker["line"] = {"width":2,"color":"#ff0000"}

        o["marker"] = marker
        o["text"] = text

        return(o)

    def info(self,data):
        if self._controls != None:
            self._controls.processInfo()
        self.setTitle()

    def getValues(self,name):
        values = []

        value = self.getOpt(name)

        if value != None:
            if type(value) is list:
                for v in value:
                    values.append(v)
            else:
                values.append(value)

        return(values)

class BarChart(Chart):
    def __init__(self,visuals,datasource,layout,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)

    def createContent(self):
        values = self.getValues("y")
        colors = self._visuals._colors.getFirst(len(values))
        opacity = self.getOpt("opacity")

        self._data = []

        orientation = self.getOpt("orientation","vertical")

        if orientation == "horizontal":
            for i,v in enumerate(values):
                self._data.append(go.Bar(x=[0],y=[""],name=v,orientation="h",marker_color=colors[i]))

        else:
            for i,v in enumerate(values):
                self._data.append(go.Bar(x=[""],y=[0],name=v,opacity=opacity,marker_color=colors[i]))

    def draw(self,data = None,clear = False):
        if self._figure == None:
            return

        self._figure.update_xaxes(showline=True,linewidth=self._visuals._axisWidth)
        self._figure.update_yaxes(showline=True,linewidth=self._visuals._axisWidth)

        x = self.getValues("x")
        values = self.getValues("y")

        orientation = self.getOpt("orientation","vertical")
        marker = {}

        if orientation == "horizontal":

            if len(x) > 0:
                try:
                    data = self._datasource.getValuesBy(x,values)
                except:
                    return

                for i,v in enumerate(values):
                    self._figure.data[i].x = data["values"][v]
                    self._figure.data[i].y = data["keys"]

            else:
                keys = self._datasource.getKeyValues()

                for i,v in enumerate(values):
                    y = self._datasource.getValues(v)
                    self._figure.data[i].x = y
                    self._figure.data[i].y = keys

        else:

            if len(x) > 0:
                try:
                    data = self._datasource.getValuesBy(x,values)
                except:
                    return

                for i,v in enumerate(values):
                    self._figure.data[i].x = data["keys"]
                    self._figure.data[i].y = data["values"][v]
            else:
                keys = self._datasource.getKeyValues()

                if len(keys) == 0:
                    for i,v in enumerate(values):
                        self._figure.data[i].x = [""]
                        self._figure.data[i].y = [0]
                else:
                    for i,v in enumerate(values):
                        y = self._datasource.getValues(v)
                        self._figure.data[i].x = keys
                        self._figure.data[i].y = y

        markers = None

        for d in self._delegates:
            if tools.supports(d,"get_bar_color"):
                markers = []
                colors = []
                if orientation == "horizontal":
                    for i,v in enumerate(values):
                        marker = {}
                        for j in range(0,len(self._figure.data[i].y)):
                            color = d.get_bar_color(self._figure.data[i].y[j],self._figure.data[i].x[j])
                            colors.append(color)
                        marker["color"] = colors
                        markers.append(marker)
                else:
                    for i,v in enumerate(values):
                        marker = {}
                        for j in range(0,len(self._figure.data[i].x)):
                            color = d.get_bar_color(self._figure.data[i].x[j],self._figure.data[i].y[j])
                            colors.append(color)
                        marker["color"] = colors
                        markers.append(marker)

        if markers is not None:
            for i,m in enumerate(markers):
                self._figure.data[i].marker = markers[i]

        self._figure.update_xaxes(automargin=True)
        self._figure.update_yaxes(automargin=True)

        self.setTitle()

class LineChart(Chart):
    def __init__(self,visuals,datasource,layout,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)

    def createContent(self):
        values = self.getValues("y")

        self._data = []

        width = self.getOpt("line_width",2)
        shape = "linear"
        if self.getOpt("curved",False):
            shape = "spline"
        line = {"width":width,"shape":shape}

        fill = self.getOpt("fill",False)

        colors = self._visuals._colors.getFirst(len(values))

        mode = "lines"

        if fill:
            mode = "none"
        elif self.hasOpt("mode"):
            mode = self.getOpt("mode")

        for i,v in enumerate(values):
            if fill:
                if i == 0:
                    self._data.append(go.Scatter(x=[""],y=[0],name=v,mode=mode,fill="tozeroy",fillcolor=colors[i]))
                else:
                    self._data.append(go.Scatter(x=[""],y=[0],name=v,mode=mode,fill="tonexty",fillcolor=colors[i]))
            else:
                line["color"] = colors[i]
                self._data.append(go.Scatter(x=[""],y=[0],name=v,mode=mode,line=line))

    def draw(self,data = None,clear = False):
        if self._figure == None:
            return

        self._figure.update_xaxes(showline=True,linewidth=self._visuals._axisWidth)
        self._figure.update_yaxes(showline=True,linewidth=self._visuals._axisWidth)

        values = self.getValues("y")
        x = self.getValues("x")

        if len(x) > 0:
            try:
                data = self._datasource.getValuesBy(x,values)
            except Exception as e:
                logging.info(str(e))
                return

            for i,v in enumerate(values):
                self._figure.data[i].x = data["keys"]
                self._figure.data[i].y = data["values"][v]
        else:
            keys = self._datasource.getKeyValues()

            for i,v in enumerate(values):
                y = self._datasource.getValues(v)
                self._figure.data[i].x = keys
                self._figure.data[i].y = y

        self._figure.update_xaxes(automargin=True)
        self._figure.update_yaxes(automargin=True)

        self.setTitle()

class ScatterPlot(LineChart):
    def __init__(self,visuals,datasource,layout,**kwargs):
        LineChart.__init__(self,visuals,datasource,layout,**kwargs)
        self.setOpt("mode","markers")

class TimeSeries(LineChart):
    def __init__(self,visuals,datasource,layout,**kwargs):
        LineChart.__init__(self,visuals,datasource,layout,**kwargs)

        if self.hasOpt("time") == False:
            raise Exception("must specify time field for a TimeSeries")
 
        self.setOpt("x",self.getOpt("time"))

class PieChart(Chart):
    def __init__(self,visuals,datasource,layout,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)

    def createContent(self):
        value = self.getValues("value")

        self._data = []

        if len(value) == 1:
            self._data.append(go.Pie(labels=[""],values=[0],name=value[0]))

    def draw(self,data = None,clear = False):
        if self._figure == None:
            return

        labels = self.getValues("labels")
        value = self.getValues("value")

        if len(value) == 1:
            if len(labels) > 0:
                data = self._datasource.getValuesBy(labels,value)

                self._figure.data[0].labels = data["keys"]
                self._figure.data[0].values = data["values"][value[0]]

            else:
                keys = self._datasource.getKeyValues()

                v = self._datasource.getValues(value[0])
                self._figure.data[0].labels = keys
                self._figure.data[0].values = v

        self.setTitle()

class BubbleChart(Chart):
    def __init__(self,visuals,datasource,layout,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)

    def createContent(self):
        values = self.getValues("y")

        self._data = []

        for v in values:
            self._data.append(go.Scatter(x=[""],y=[0],name=v,mode="markers"))

    def draw(self,data = None,clear = False):
        if self._figure == None:
            return

        self._figure.update_xaxes(showline=True,linewidth=self._visuals._axisWidth)
        self._figure.update_yaxes(showline=True,linewidth=self._visuals._axisWidth)

        data = None

        x = self.getValues("x")
        if len(x) == 0:
            x = self._datasource.getKeyFieldNames()
        values = self.getValues("y")
        size = self.getOpt("size")
        color = self.getOpt("color")

        a = []
        a.extend(values)

        if size != None:
            if (size in a) == False:
                a.append(size)

        if color != None:
            if (color in a) == False:
                a.append(color)

        try:
            data = self._datasource.getValuesBy(x,a)
        except:
            return

        keys = data["keys"]

        if len(keys) == 0:
            for i,v in enumerate(values):
                self._figure.data[i].x = [""]
                self._figure.data[i].y = [0]
                return

        marker = {}

        text = None

        if size != None or color != None:

            text = []

            for i in range(0,len(keys)):
                text.append("")

            if size != None:
                if size in data["values"]:
                    s = data["values"][size]
                    if s != None and len(s) > 0:
                        maxsize = 60.
                        minsize = 5
                        marker["size"] = s
                        marker["sizemode"] = "area"
                        marker["sizeref"] = 2. * max(s) / (maxsize ** 2)
                        marker["sizemin"] = minsize

                        for i,v in enumerate(s):
                            text[i] += size + "=" + str(v)

            color = self.getOpt("color")

            if color != None:
                if color in data["values"]:
                    s = data["values"][color]
                    if s != None:
                        marker["color"] = s
                        marker["showscale"] = True
                        marker["colorscale"] = self._visuals._colors.colorscale

                        for i,v in enumerate(s):
                            if size != None:
                                text[i] += "<br>"
                            text[i] += color + "=" + str(v)

        for i,v in enumerate(values):
            self._figure.data[i].x = keys
            self._figure.data[i].y = data["values"][v]
            self._figure.data[i].marker = marker
            self._figure.data[i].text = text

        self._figure.update_xaxes(automargin=True)
        self._figure.update_yaxes(automargin=True)

        self.setTitle()

class Table(Chart):
    def __init__(self,visuals,datasource,layout,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)
        self._html = widgets.HTML(layout=widgets.Layout(overflow="auto",width="100%",height="100%",margin="0",padding="0"))
        self._html.add_class(self._visuals.css + "_figure")
        self._html.add_class(self._visuals.css + "_table")
        self._content.children = [self._html]

    def draw(self,data = None,clear = False):
        allfields = self._datasource.getFields()
        columns = self.getValues("values")

        fields = []

        if len(columns) > 0:
            for column in columns:
                f = self._datasource.getField(column)
                if f != None:
                    fields.append(f)
        else:
            for f in allfields:
                fields.append(f)

        if len(fields) == 0:
            return

        border = "1px solid #d8d8d8"
        padding = "4px"

        content = ""

        style = ""
        style += "position:relative"
        style += ";overflow:auto"
        style += ";border:" + border
        style += ";border:0"
        style += ";width:100%"

        content += "<table cellspacing='0' cellpadding='0' style='" + style + "'>"
        content += "<tr>"

        style = ""
        style += "position:sticky"
        style += ";padding:" + padding
        style += ";top:0px"

        for i,f in enumerate(fields):
            thstyle = style
            #thstyle += ";border:" + border
            thstyle += ";border-left-width:0"
            thstyle += ";border-top-width:0"
            if i == len(fields) - 1:
                thstyle += ";border-right-width:0"
            if f["isNumber"]:
                thstyle += ";text-align:right"
            else:
                thstyle += ";text-align:left"
            content += "<th style='" + thstyle + "'>"
            content += f["name"] + "</th>"
            content += "\n"

        content += "</tr>"

        items = self._datasource.getList()

        content += "<tr>"

        gradient = None

        color = self.getOpt("color")

        if color != None:
            if len(items) > 0:
                a = []

                for o in items:
                    if color in o:
                        a.append(float(o[color]))
                    else:
                        a.append(0)

                baseColor = self.getOpt("start_color")
                if baseColor == None:
                    baseColor = self._visuals._colors.lightest
                gradient = colors.Gradient(baseColor,levels=100,min=min(a),max=max(a))

        if self.getOpt("reversed",False):
            l = reversed(items)
        else:
            l = items

        style = ""
        #style += ";background:white"
        style += ";border:" + border
        style += ";padding:" + padding

        for i,o in enumerate(l):
            content += "<tr"
            if color != None and gradient != None:
                if color in o:
                    content += " style='"
                    value = o[color]
                    c = gradient.darken(float(value))
                    content += "background:" + c
                    luma = colors.Colors.getLuma(c)
                    if luma < 170:
                        content += ";color:white"
                    else:
                        content += ";color:black"
            for d in self._delegates:
                if tools.supports(d,"get_row_style"):
                    style = d.get_row_style(o)
                    if style is not None:
                        content += ";" + style
                    content += "'"
            content += ">"
            for j,f in enumerate(fields):
                name = f["name"]
                value = o[name]
                imagedata = o[name]
                if f["type"] == "blob":
                    format = "png" 
                    if isinstance(imagedata,dict):
                        format = imagedata["@type"]
                        imagedata = imagedata["*value"]
                    value = "<div style='width:100%;position:relative;margin:auto'>"
                    value += "<img style='width:100%;height:100%' src='data:image/" + format + ";base64," + imagedata + "'/>"
                    if "_nObjects_" in o:
                        numObjects = int(float(o["_nObjects_"]))
                        for j in range(0,numObjects):
                            s = "_Object" + str(j) + "_"
                            text = o[s].strip()
                            s = "_Object" + str(j) + "_x"
                            x = int(float(o[s]) * 100)
                            s = "_Object" + str(j) + "_y"
                            y = int(float(o[s]) * 100)
                            div = "<div style='position:absolute;left:" + str(x) + "%;top:" + str(y) + "%;"
                            div += "color:" + self.getOpt("image_text_color","black") + ";"
                            div += "'>"
                            div += text
                            div += "</div>"
                            value += div

                    value += "</div>"
                elif f["isDate"]:
                    num = int((int)(value))
                    date = datetime.datetime.fromtimestamp(num)
                    value = str(date)
                elif f["isTime"]:
                    num = int((int)(value) / 1000000)
                    date = datetime.datetime.fromtimestamp(num)
                    value = str(date)
                style = ""
                style += "border-left-width:0"
                style += ";border-top-width:0"
                if f["isNumber"]:
                    style += ";text-align:right"
                if j == len(fields) - 1:
                    style += ";border-right-width:0"
                for d in self._delegates:
                    if tools.supports(d,"get_cell_style"):
                        s = d.get_cell_style(o,name)
                        if s is not None:
                            style += ";" + s
                content += "<td style='" + style + "'>"
                content += str(value)
                content += "</td>"
            content += "</tr>"
            content += "\n"

        content += "</table>"

        self._html.value = content

        self.setTitle()

class ImageViewer(Chart):
    def __init__(self,visuals,datasource,layout,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)
        self._data = None
        self._detection = None

    def draw(self,data = None,clear = False):
        if data != None and len(data) > 0:

            if self._detection == None:
                if self._datasource.schema.hasFields():
                    self._detection = (self._datasource.schema.getField("_nObjects_") != None)

            self._data = data[len(data) - 1]
            field = self.getOpt("image")

            imageWidth = self.getOpt("image_width",400)
            imageHeight = self.getOpt("image_height",400)

            if self.hasOpt("scale"):
                imageWidth = imageWidth * self.getOpt("scale")
                imageHeight = imageHeight * self.getOpt("scale")

            html = None

            if field in self._data:
                imagedata = self._data[field]
                html = ""
                html += "<div style='width:" + str(imageWidth) + "px;height:" + str(imageHeight) + "px;position:relative;margin:auto"
                if self.hasOpt("image_border"):
                    html += ";border:" + self.getOpt("image_border")
                html += "'>"
                html += "<img style='width:100%;height:100%' src='data:image/jpeg;base64," + imagedata + "'/>"

                if self._detection:
                    if "_nObjects_" in self._data:
                        value = str(self._data["_nObjects_"])
                        numObjects = int(float(value))
                        for i in range(0,numObjects):
                            s = "_Object" + str(i) + "_"
                            text = self._data[s].strip()
                            s = "_Object" + str(i) + "_x"
                            x = int(float(self._data[s]) * 100)
                            s = "_Object" + str(i) + "_y"
                            y = int(float(self._data[s]) * 100)
                            html += "<div style='position:absolute;zindex:1000;font-weight:normal;color:" + self.getOpt("label_color","white") + ";left:" + str(x) + "%;top:" + str(y) + "%;'>" + text + "</div>"

                html += "</div>"

            if html != None:
                #content = widgets.HTML(value=html,layout=widgets.Layout(width="100%",height="100%",overflow="auto"))
                content = widgets.HTML(value=html,layout=widgets.Layout(overflow="auto",border="2px solid black"))
                self._content.children = [content]
                self.setDisplay()

        self.setTitle()

class Images(Chart):

    def __init__(self,visuals,datasource,layout,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)
        self.layout.overflow = "auto"
        if self.hasOpt("image") == False:
            raise Exception("you must specify the image property")
        self._entries = {}
        self._lock = threading.Lock()
        self._imageWidth = self.getOpt("image_width",300)
        self._imageHeight = self.getOpt("image_height",300)
        orientation = self.getOpt("orientation","vertical")

        if orientation == "horizontal":
            self.layout.width = self.getOpt("width","800px")
            self.layout.height = str(self._imageHeight + 100) + "px"
        else:
            self.layout.width = str(self._imageWidth + 80) + "px"
            self.layout.height = self.getOpt("height","800px")

        self._detection = None

    def createContent(self):
        #self._content.children = [self._header]
        pass

    def draw(self,data = None,clear = False):
        if self._detection == None:
            if self._datasource.schema.hasFields():
                self._detection = (self._datasource.schema.getField("_nObjects_") != None)

        field = self.getOpt("image")

        data = self._datasource.getList()

        layout = False

        if clear:
            self._entries = []
            layout = True

        keys = []

        for o in data:
            if ("@key" in o) == False:
                continue

            key = o["@key"]

            if "@opcode" in o and o["@opcode"] == "delete":
                if self.removeEntry(key) != None:
                    layout = True
            else:
                keys.append(key)

                entry = self.getEntry(key)

                if entry == None:
                    entry = ImageEntry(self)
                    entry.data = o
                    entry.setOpt("title",key)
                    self._entries.append(entry)
                    layout = True

        remove = []

        for entry in self._entries:
            if (entry.key in keys) == False:
                remove.append(entry.key)
                layout = True

        for key in remove:
            self.removeEntry(key)

        if layout:
            orientation = self.getOpt("orientation","vertical")
            html = ""
            html += "<div style='overflow:visible'>"
            html += "<table cellspacing='10' cellpadding='10'>"
            if orientation == "horizontal":
                html += "<tr>"

            for entry in reversed(self._entries):
                if orientation == "vertical":
                    html += "<tr>"
                html += "<td>"
                html += entry.html
                html += "</td>"
                if orientation == "vertical":
                    html += "</tr>"
            if orientation == "horizontal":
                html += "</tr>"
            html += "</table>"
            html += "</div>"

            images = widgets.HTML(value=html,layout=widgets.Layout(border=self._visuals.getOpt("border","1px solid #d8d8d8"),width="100%",height="100%",overflow="auto",))

            self.children = [self._header,images]

        self.layout.overflow = "auto"
        self.setTitle()

    def getEntry(self,key):
        for entry in self._entries:
            if entry.key == key:
                return(entry)

        return(None)

    def removeEntry(self,key):
        for i,entry in enumerate(self._entries):
            if entry.key == key:
                del self._entries[i]
                return(entry)

        return(None)

class ImageEntry(Options):
    def __init__(self,images,**kwargs):
        Options.__init__(self,**kwargs)
        self._images = images
        self._data = None
        self._key = ""
        self._header = widgets.HTML()
        self._html = None

    @property
    def html(self):
        if self._html == None:
            html = ""

            if self._data == None:
                return(html)

            field = self._images.getOpt("image")

            if field in self._data:

                data = self._data[field]

                format = "png" 

                if isinstance(data,dict):
                    format = data["@type"]
                    data = data["*value"]

                key = self._data["@key"]

                html += "<div style='width:" + str(self._images.getOpt("image_width",400)) + "px;height:" + str(self._images.getOpt("image_height",400)) + "px;position:relative;margin:auto;border:" + self._images.getOpt("image_border","1px solid #000000") + "'>"
                html += "<img style='width:100%;height:100%' src='data:image/jpeg;base64," + data + "'/>"

                if self._images._detection:
                    if "_nObjects_" in self._data:
                        value = str(self._data["_nObjects_"])
                        numObjects = int(float(value))
                        for i in range(0,numObjects):
                            s = "_Object" + str(i) + "_"
                            text = self._data[s].strip()
                            s = "_Object" + str(i) + "_x"
                            x = int(float(self._data[s]) * 100)
                            s = "_Object" + str(i) + "_y"
                            y = int(float(self._data[s]) * 100)
                            html += "<div style='position:absolute;zindex:1000;font-weight:normal;color:" + self._images.getOpt("label_color","white") + ";left:" + str(x) + "%;top:" + str(y) + "%;'>" + text + "</div>"

                html += "</div>"

            self._html = html

        return(self._html)

    @property
    def key(self):
        return(self._key)

    @property
    def data(self):
        return(self._data)
    
    @data.setter
    def data(self,value):
        self._data = value
        if "@key" in self._data:
            self._key = self._data["@key"]

class Map(Chart):
    def __init__(self,visuals,datasource,layout = None,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)

        self._map = maps.Map()
        self._map.add_class(self._visuals.css + "_figure")
        self._lat = None
        self._lon = None
        self._markers = {}
        self._colorbar = None

        self._colors = None
        self._colorRange = None

        self._circles = []
        self._polygons = []

        self._lock = threading.Lock()

    def createContent(self):
        if self.hasOpt("center"):
            self._map.center = self.getOpt("center")
        if self.hasOpt("zoom"):
            self._map.zoom = self.getOpt("zoom")

        self._lat = self.getOpt("lat")
        if self._lat == None:
            raise Exception("you must specify the lat property")

        self._lon = self.getOpt("lon")
        if self._lon == None:
            raise Exception("you must specify the lon property")

        #if len(self._circles) > 0:
            #for o in self._circles:
                #self._map.add_layer(o["layers"])

        #if len(self._polygons) > 0:
            #for o in self._polygons:
                #self._map.add_layer(o["layers"])

        self._colors = None

        if self.hasOpt("colormap"):
            self._colors = colors.Colors(colormap=self.getOpt("colormap"))
        elif self.hasOpt("colors"):
            self._colors = colors.Colors(colors=self.getOpt("colors"))
        else:
            self._colors = self._visuals._colors

        if self._colors != None:
            if self.hasOpt("color_range"):
                range = self.getOpt("color_range")
                self._colorRange = colors.ColorRange(self._colors,range[0],range[1])

        components = []

        #components.append(self._header)

        colorbar = None

        if self.hasOpt("color"):
            #data = go.Scatter(x=[None],y=[None],marker=dict(colorscale=self._visuals._colors.colorscale,showscale=True,cmin=-5,cmax=5,colorbar=dict(xpad=0,ypad=0,ticks="")))
            data = go.Scatter(x=[None],y=[None],marker=dict(colorscale=self._visuals._colors.colorscale,showscale=True,colorbar=dict(thickness=30)))
            layout = dict(xaxis=dict(visible=False),yaxis=dict(visible=False),showlegend=False)
            #self._colorbar = go.FigureWidget(data=data,layout=layout)
            #colorbar = widgets.Box([self._colorbar],layout=widgets.Layout(width="150px",margin="0px",padding="0px"))

        if colorbar != None:
            components.append(widgets.HBox([self._map,colorbar]))
        else:
            components.append(self._map)

        self._content.children = components

    def draw(self,data = None,clear = False):
        if self._map == None:
            return

        popup = self.getValues("popup")

        value = self.getOpt("size")

        tracking = self.getOpt("tracking",False)
        center = None

        sizePixels = None
        sizeField = None

        if value != None: 
            try:
                sizePixels = int(value)
            except:
                sizeField = value

        color = None
        colorField = None

        value = self.getOpt("color")

        if value != None: 
            color = colors.Colors.getColorFromName(value)

            if color == None:
                colorField = value

        minSize = sys.maxsize
        maxSize = 0

        minColor = sys.maxsize
        maxColor = 0

        sizes = self.getOpt("sizes",(1,20))

        sizeRange = []
        colorRange = None

        gradient = None

        data = self._datasource.getList()

        if sizeField != None or colorField != None:
            for value in data:
                if sizeField != None:
                    if sizeField in value:
                        num = float(value[sizeField])
                        if num < minSize:
                            minSize = num
                        if num > maxSize:
                            maxSize = num
                if colorField != None:
                    if colorField in value:
                        num = float(value[colorField])
                        if num < minColor:
                            minColor = num
                        if num > maxColor:
                            maxColor = num
        if sizeField != None:
            if maxSize > minSize:
                sizeRange = np.arange(minSize,maxSize,(maxSize - minSize) / int(sizes[1] - sizes[0]))

        if colorField != None:
            if self._colorRange != None:
                colorRange = self._colorRange
            elif self._colors != None:
                colorRange = colors.ColorRange(self._colors,minColor,maxColor)
            else:
                colorRange = colors.ColorRange(self._visuals._colors,minColor,maxColor)
            if self._colorbar != None:
                self._colorbar.data[0].marker.cmin = minColor
                self._colorbar.data[0].marker.cmax = maxColor
            #baseColor = self.getOpt("base_color",self._visuals._colors.lightest)
            #gradient = colors.Gradient(baseColor,levels=200,min=minColor,max=maxColor)

        opacity = self.getOpt("marker_opacity",1)
        border = self.getOpt("marker_border",True)

        keyValues = self.getValues("keys")

        if len(keyValues) == 0:
            keyValues = ["@key"]

        keys = []

        createMarker = self.getOpt("create_marker")
        updateMarker = self.getOpt("update_marker")

        iconOpts = None

        if self.hasOpt("icon"):
            iconOpts = tools.Options()
            iconOpts.setOpts(**self.getOpt("icon"))

        for value in data:
            key = ""
            for i,k in enumerate(keyValues):
                if i > 0:
                    key += "."
                if k in value:
                    key += value[k]
                
            keys.append(key)

            if key in self._markers:
                marker = self._markers[key]
            else:
                if createMarker != None:
                    marker = createMarker(value)
                elif iconOpts != None:
                    iconUrl = iconOpts.getOpt("url","")
                    iconSize = None
                    if iconOpts.hasOpts(["width","height"]):
                        iconSize = [iconOpts.getOpt("width"),iconOpts.getOpt("height")]

                    if iconOpts.hasOpt("name"):
                        icon = maps.AwesomeIcon(name=iconOpts.getOpt("name"),icon_size=iconSize)
                    else:
                        icon = maps.Icon(icon_url=iconUrl,icon_size=iconSize)

                    marker = maps.Marker(icon=icon)
                else:
                    marker = maps.CircleMarker()
                    marker.stroke = border
                    marker.fill_opacity = opacity

                self._map.add_layer(marker)
                self._markers[key] = marker
                if len(popup) > 0:
                    marker.popup = widgets.HTML()

            if self._lat in value and self._lon in value:
                lat = value[self._lat]
                lon = value[self._lon]
                marker.location = (lat,lon)

            if tracking and center == None:
                center = marker.location

            if sizePixels != None:
                marker.radius = sizePixels
            elif len(sizeRange) > 0:
                size = 0
                if sizeField in value:
                    size = float(value[sizeField])
                index = np.where(size >= sizeRange)[0]
                marker.radius = len(index) - 1

            if color != None:
                marker.fill_color = color
            elif colorRange != None:
                if colorField in value:
                    num = float(value[colorField])
                    marker.fill_color = colorRange.getColor(num)

            if gradient != None:
                if colorField in value:
                    num = float(value[colorField])
                    marker.fill_color = gradient.darken(num - minColor)

            if marker.popup != None:
                text = ""
                for i,s in enumerate(popup):
                    if i > 0:
                        text += "<br/>"
                    text += s + "=" + str(value[s])
                marker.popup.value = text

            if updateMarker != None:
                updateMarker(marker,value)

        remove = {}

        for key,marker in self._markers.items():
            if (key in keys) == False:
                remove[key] = marker

        for key,marker in remove.items():
            self._markers.pop(key)
            self._map.remove_layer(marker)

        if center != None:
            self._map.center = center

        self.setTitle()

    def addCircles(self,datasource,**kwargs):
        options = tools.Options(**kwargs)
        o = {}
        o["lat"] = options.getOpt("lat")
        o["lon"] = options.getOpt("lon")
        o["datasource"] = datasource
        o["layers"] = maps.LayerGroup()
        if options.hasOpt("radius"):
            value = options.getOpt("radius")
            try:
                num = int(value)
                o["radius"] = num
            except:
                o["radius_field"] = value
        if options.hasOpt("text"):
            o["text"] = options.getOpt("text")
        self._circles.append(o)
        self.loadCircles(o)
        datasource.addDelegate(self)

    def addPolygons(self,datasource,**kwargs):
        options = tools.Options(**kwargs)
        o = {}
        o["coords"] = options.getOpt("coords")
        o["datasource"] = datasource
        o["layers"] = maps.LayerGroup()
        if options.hasOpt("radius"):
            value = options.getOpt("radius")
            try:
                num = int(value)
                o["radius"] = num
            except:
                o["radius_field"] = value
        if options.hasOpt("text"):
            o["text"] = options.getOpt("text")
        o["order"] = options.getOpt("order","lat_lon")
        self._polygons.append(o)
        self.loadPolygons(o)
        datasource.addDelegate(self)

    def dataChanged(self,datasource,data,clear):

        o = None

        for circle in self._circles:
            if circle["datasource"] == datasource:
                o = circle
                break

        #with self._lock:
        if True:
            if o != None:
                self.loadCircles(o)
            else:
                for polygon in self._polygons:
                    if polygon["datasource"] == datasource:
                        o = polygon
                        break
                if o != None:
                    self.loadPolygons(o)
    
    def loadCircles(self,o):
        o["layers"].clear_layers()

        data = o["datasource"].getData()

        for key,value in data.items():
            lat = value[o["lat"]]
            lon = value[o["lon"]]

            radius = 50

            if "radius" in o:
                radius = o["radius"]
            elif "radius_field" in o:
                num = float(value[o["radius_field"]])
                radius = int(num)

            circle = maps.Circle(location=(lat,lon),radius=radius)

            borderWidth = self.getOpt("circle_border_width",2)
            circle.weight = borderWidth

            if borderWidth > 0:
                circle.stroke = True
                circle.color = self.getOpt("circle_border_color","black")
            else:
                circle.stroke = False

            circle.fill_color = self.getOpt("circle_fill_color","white")
            circle.fill_opacity = self.getOpt("circle_fill_opacity",.2)

            if "text" in o:
                text = value[o["text"]]
                circle.popup = widgets.HTML(value=text)
            o["layers"].add_layer(circle)

        if self._map != None:
            if (o["layers"] in self._map.layers) == False:
                self._map.add_layer(o["layers"])
 
    def loadPolygons(self,o):
        o["layers"].clear_layers()

        data = o["datasource"].getData()

        lonlat = (o["order"] == "lon_lat")

        with self._lock:
            for key,value in data.items():
                coords = value[o["coords"]]
                a = coords.split(" ")
                points = []
                i = 0
                while True:
                    if lonlat:
                        points.append((a[i + 1],a[i]))
                    else:
                        points.append((a[i],a[i + 1]))
                    i += 2
                    if i >= len(a):
                        break
                if len(points) == 1:
                    radius = 100

                    if "radius" in o:
                        radius = o["radius"]
                    elif "radius_field" in o:
                        num = float(value[o["radius_field"]])
                        radius = int(num)

                    circle = maps.Circle(location=(points[0][0],points[0][1]),radius=radius)

                    borderWidth = self.getOpt("circle_border_width",2)
                    circle.weight = borderWidth

                    if borderWidth > 0:
                        circle.stroke = True
                        circle.color = self.getOpt("circle_border_color","black")
                    else:
                        circle.stroke = False

                    circle.fill_color = self.getOpt("circle_fill_color","white")
                    circle.fill_opacity = self.getOpt("circle_fill_opacity",.2)

                    if "text" in o:
                        text = value[o["text"]]
                        circle.popup = widgets.HTML(value=text)

                    o["layers"].add_layer(circle)
                else:
                    polygon = maps.Polygon(locations=points)

                    borderWidth = self.getOpt("poly_border_width",1)
                    polygon.weight = borderWidth

                    if borderWidth > 0:
                        polygon.stroke = True
                        polygon.color = self.getOpt("poly_border_color","black")
                    else:
                        polygon.stroke = False

                    polygon.fill_color = self.getOpt("poly_fill_color","white")
                    polygon.fill_opacity = self.getOpt("poly_fill_opacity",.2)

                    if "text" in o:
                        text = value[o["text"]]
                        polygon.popup = widgets.HTML(value=text)

                    o["layers"].add_layer(polygon)

        if self._map != None:
            if (o["layers"] in self._map.layers) == False:
                self._map.add_layer(o["layers"])

    @property
    def map(self):
        return(self._map)

    @property
    def icon(self):
        return(self._icon)

    @icon.setter
    def icon(self,value):
        self._icon = value

class Gauge(Chart):
    def __init__(self,visuals,datasource,layout = None,**kwargs):

        #self.remove_class(self._visuals.css + "_chart")
        #self.add_class(self._visuals.css + "_gauge")

        self._gopts = None
        self._colors = None
        self._entries = {}

        Chart.__init__(self,visuals,datasource,layout,**kwargs)

        self.range = self.getOpt("range",(0,100))

    def createContent(self):
        segments = self.getOpt("segments",3)

        self._colors = []

        if self.hasOpt("colors"):
            self._colors.extend(self.getOpt("colors"))
        elif self.hasOpt("gradient"):
            self._gopts = tools.Options(**self.getOpt("gradient"))
            logging.info("GRADIENT: " + str(self._gopts))
            color = self._visuals._colors.getColor(self._gopts.getOpt("color","lightest"))
            delta = self._gopts.getOpt("delta",20)

            for i in range(0,segments):
                self._colors.append(colors.Colors.darken(color,i * delta))
        else:
            if self.hasOpt("color"):
                opts = {}
                opts["color"] = self.getOpt("color")
                opts["end"] = self.getOpt("gradient_end",False)
                opts["num"] = segments
                opts["delta"] = self.getOpt("gradient_delta",15)
                self._colors.extend(colors.Colors.createGradientColors(**opts))

        intervalSize = (self._range[1] - self._range[0]) / segments

        self._intervals = np.arange(self._range[0],self._range[1],intervalSize)

    def draw(self,data = None,clear = False):

        if data == None or len(data) == 0:
            data = self._datasource.getList()

        gauges = None

        field = self.getOpt("value")

        if field == None:
            return

        items = None

        currentLength = len(self._entries)

        layout = False

        if clear:
            self._entries = {}
            layout = True

        for o in data:
            if ("@key" in o) == False:
                continue
            key = o["@key"]

            if "@opcode" in o and o["@opcode"] == "delete":
                if key in self._entries:
                    self._entries.pop(key)
                    layout = True
            else:
                if (key in self._entries) == False:
                    entry = GaugeEntry(self)
                    entry.setOpt("title",key)
                    self._entries[key] = entry
                    layout = True

                    for d in self._delegates:
                        if tools.supports(d,"entryCreated"):
                            d.entryCreated(self,o,entry)

                else:
                    entry = self._entries[key]

                value = float(o[field])
                entry.value = value

                for d in self._delegates:
                    if tools.supports(d,"entryRendered"):
                        d.entryRendered(self,o,entry)

        if self._gopts != None:
            entries = []
            values = []

            for key,entry in self._entries.items():
                entries.append(entry)
                values.append(entry.value)

            opts = {"gradient":self._gopts.options}

            colors = self._visuals._colors.createColors(values,**opts)

            for i in range(0,len(entries)):
                entries[i].setOpt("bg",colors[i])

        if layout:
            a = []

            for key,value in self._entries.items():
                a.append(value)

            self._content.children = a

            if False:
                if self.getOpt("show_controls",False):
                    if self._controls == None:
                        self._controls = ControlPanel(self._datasource) 
                    a.append(self._controls)

        if len(self._entries) > 1:
            if currentLength <= 1:
                for entry in self._entries.values():
                    entry.setTitle()
        elif len(self._entries) == 1:
            if currentLength > 1:
                for entry in self._entries.values():
                    entry.setTitle()

        self.setTitle()

    def setTitle(self,title = None):
        title = self.getOpt("title","")

        if len(self._entries) == 1 and self.getOpt("singleton",False):
            for gauge in self._entries.values():
                if len(title) == 0:
                    title = gauge.getOpt("title","")
                break
            
        Chart.setTitle(self,title)

    def getLabel(self,num):
        if num == 0:
            label = "0"
        elif num >= 1000000:
            label = str(num / 1000000) + "M"
        elif num >= 100000:
            label = str(int(num / 10000)) + "K"
        elif self._range[1] - self._range[0] < 10:
            label = str(num)
        else:
            label = str(int(num))

        return(label)

    @property
    def range(self):
        return(self._range)

    @range.setter
    def range(self,value):
        if value[1] > value[0]:
            self._range = value
            self.create()

class GaugeEntry(Chart):
    def __init__(self,gauge,**kwargs):
        Chart.__init__(self,gauge._visuals,**kwargs)
        self._gauge = gauge
        self._data = None
        self._figure = None
        self._indicator = None
        self._value = 0
        self._reference = 0

    def create(self):

        shape = self._gauge.getOpt("shape","angular")

        if shape == "bullet":
            size = self._gauge.getOpt("size",(300,50))
        else:
            size = self._gauge.getOpt("size",(300,200))

        layout = None

        if type(size) is tuple:
            layout = go.Layout(width=size[0],height=size[1])
        else:
            layout = go.Layout(width=size,height=size)

        layout["paper_bgcolor"] = self._gauge.getOpt("bg","white")

        margin = (0,0,0,0)

        if True:
            if shape == "bullet":
                margin = self._gauge.getOpt("margin",(10,10,40,40))
            else:
                margin = self._gauge.getOpt("margin",(30,30,30,10))

        if type(margin) is tuple:
            layout["margin"] = dict(l=margin[0],t=margin[1],r=margin[2],b=margin[3])
        else:
            layout["margin"] = dict(l=margin,t=margin,r=margin,b=margin)

        self._data = []

        axis = {}
        axis["range"] = self._gauge.range
        steps = []

        prev = 0

        for i in range(1,len(self._gauge._intervals)):
            lower = self._gauge._intervals[i - 1]
            upper = self._gauge._intervals[i]
            steps.append({"range":[lower,upper],"color":self._gauge._colors[i - 1]})

        lower = self._gauge._intervals[-1]
        upper = self._gauge._range[1]
        steps.append({"range":[lower,upper],"color":self._gauge._colors[-1]})
        
        mode = ""

        if self._gauge.getOpt("gauge",True):
            if len(mode) > 0:
                mode += "+"
            mode += "gauge"

        if self._gauge.getOpt("number",True):
            if len(mode) > 0:
                mode += "+"
            mode += "number"

        if self._gauge.getOpt("delta",False):
            if len(mode) > 0:
                mode += "+"
            mode += "delta"

        shape = self._gauge.getOpt("shape","angular")

        bar = {}
        bar["color"] = self._gauge.getOpt("bar_color","black")
        bar["thickness"] = self._gauge.getOpt("bar_width",.2)

        self._indicator = go.Indicator(mode=mode,gauge={"shape":shape,"axis":axis,"steps":steps,"bar":bar},value=0)

        self._data.append(self._indicator)
        self._figure = go.FigureWidget(data=self._data,layout=layout)
        self._content.children = [self._figure]
 
    def draw(self,data = None,clear = False):

        if self._figure == None:
            self.create()

        if self.hasOpt("bg"):
            self._figure.update_layout(paper_bgcolor=self.getOpt("bg"))

        self._figure.update_traces(value=self._value)

        if self._gauge.getOpt("delta",False):
            self._figure.update_traces(value=self._value,delta={"reference":self._reference})
        else:
            self._figure.update_traces(value=self._value)

        self.setTitle()

    def setTitle(self):

        if self._gauge.getOpt("singleton",False):
            self.layout.border = "0"
            return

        title = self.getOpt("title")

        if title != None:
            self._header.value = self._gauge._visuals.formatTitle(title)

        self.layout.border = self._gauge.getOpt("border","1px solid #d8d8d8")

    @property
    def value(self):
        return(self._value)

    @value.setter
    def value(self,value):

        if self._gauge.getOpt("delta",False):
            self._reference = self._value

        if self._gauge.getOpt("integer",False):
            self._value = int(value)
        else:
            self._value = value
        self.draw()

    @property
    def title(self):
        return(self._header)

class Compass(Chart):
    def __init__(self,visuals,datasource,layout = None,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)

        #self.remove_class(self._visuals.css + "_chart")
        #self.add_class(self._visuals.css + "_compass")

        self._colors = None
        self._entries = {}

    def createContent(self):
        segments = 8

        self._values = [13,12,13,12,13,12,13,12]
        self._labels = ["N","NE","E","SE","S","SW","W","NW"]

    def draw(self,data = None,clear = False):

        if data == None or len(data) == 0:
            data = self._datasource.getList()

        field = self.getOpt("heading")

        if field == None:
            return

        layout = False

        if clear:
            self._entries = {}
            layout = True

        for o in data:
            if ("@key" in o) == False:
                continue

            key = o["@key"]

            if "@opcode" in o and o["@opcode"] == "delete":
                if key in self._entries:
                    self._entries.pop(key)
                    layout = True
            else:
                if (key in self._entries) == False:
                    entry = CompassEntry(self)
                    entry.setOpt("title",key)
                    self._entries[key] = entry
                    layout = True

                    for d in self._delegates:
                        if tools.supports(d,"entryCreated"):
                            d.entryCreated(self,o,entry)
                else:
                    entry = self._entries[key]

                value = float(o[field])
                entry.heading = value
                for d in self._delegates:
                    if tools.supports(d,"entryRendered"):
                        d.entryRendered(self,o,entry)

        if layout:
            a = []

            for key,value in self._entries.items():
                a.append(value)

            self._content.children = a

        self.setTitle()

    def setTitle(self,title = None):
        title = self.getOpt("title","")

        if len(self._entries) == 1 and self.getOpt("singleton",False):
            for entry in self._entries.values():
                if len(title) == 0:
                    title = entry.getOpt("title","")
                title = str.format("{} ({})".format(title,int(entry._heading)))
                break
            
        Chart.setTitle(self,title)

class CompassEntry(Options,widgets.VBox):
    def __init__(self,compass,**kwargs):
        Options.__init__(self,**kwargs)
        widgets.VBox.__init__(self,layout=widgets.Layout(margin="10px"))

        self._compass = compass
        self._figure = None
        self._layout = None
        self._heading = 0
        self._header = widgets.HTML(layout=widgets.Layout(overflow="hidden"))
        self._header.add_class(self._compass._visuals.css + "_header")
        self._innerCircle = None

    def create(self):
        size = self._compass.getOpt("size",300)

        self._layout = go.Layout(width=size,height=size)

        self._data = []

        color = colors.Colors.getColorFromName(self._compass.getOpt("outer_color","white"))

        linewidth = self._compass.getOpt("line_width",1)

        self._intervals = go.Pie(values=self._compass._values,
            #domain={"x":[0,.5]},
            labels=self._compass._labels,
            marker=dict(colors=[color] * 20,line_width=self._compass.getOpt("line_width",linewidth)),
            hole=self._compass.getOpt("compass_size",.90),
            sort=False,
            direction="clockwise",
            showlegend=False,
            hoverinfo="none",
            textposition="outside",
            textinfo="none")

        self._data.append(self._intervals)

        margin = self._compass.getOpt("margin",10)
        self._layout["margin"] = dict(l=margin,r=margin,b=margin,t=margin)

        labels = []

        #headings = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        headings = ["N", "E", "S", "W"]

        hole = self._intervals["hole"]
        radius = hole / 2
        center = dict(x=.5,y=.5)

        color = self._compass.getOpt("bg_color","#f8f8f8")

        textangle = 0
        angle = 0

        for i,text in enumerate(headings):
            if angle == 135 or angle == 315:
                h = (450 - angle - 3) % 360
            else:
                h = (450 - angle) % 360

            radians = h * (math.pi / 180)
            #if i % 2 == 1:
            #    fontsize = 12
            #    length = radius - .08
            #else:
            #    fontsize = 18
            #    length = radius - .1

            if i == 1:
                length = radius - .12
            elif i == 3:
                length = radius - .12
            else:
                length = radius - .10

            fontsize = 18

            x = center["x"] + (length * math.cos(radians))
            y = center["y"] + (length * math.sin(radians))
            #label = go.layout.Annotation(text=headings[i],x=x,y=y,showarrow=False,font=dict(size=fontsize),textangle=textangle)
            label = go.layout.Annotation(text=headings[i],x=x,y=y,showarrow=False,font=dict(size=fontsize))
            #label = go.layout.Annotation(text="<b>" + headings[i] + "</b>",x=x,y=y,showarrow=False,font=dict(size=fontsize))
            labels.append(label)
            angle += (360 / len(headings))
            textangle += 45

        textangle = 0
        angle = 0

        for i in range(0,8):
            if angle == 135 or angle == 315:
                h = (450 - angle - 2) % 360
            else:
                h = (450 - angle) % 360
            radians = h * (math.pi / 180)

            if i == 3 or i == 5 or i == 7:
                 length = radius + .05
            elif i % 2 == 1:
                 length = radius + .03
            else:
                 length = radius

            x = center["x"] + (length * math.cos(radians))
            y = center["y"] + (length * math.sin(radians))
            label = go.layout.Annotation(text=str(angle),x=x,y=y,showarrow=False,font=dict(size=10),textangle=textangle)
            labels.append(label)
            angle += 45
            textangle += 45

        h = (hole / 2) - .1
        self._innerCircle = go.layout.Shape(type="circle",x0=center["x"] - h,y0=center["y"] - h,x1=center["x"] + h,y1=center["y"] + h,fillcolor=color,line_width=linewidth)

        self._layout["annotations"] = labels
        self._layout["shapes"] = [self._innerCircle]

        self._figure = go.FigureWidget(data=self._data,layout=self._layout)
 
        height = size
        height += 30
        self.children = [self._header,self._figure]
 
    def draw(self,data = None,clear = False):
        if self._figure == None:
            self.create()

        heading = (450 - self._heading) % 360
        hole = self._intervals["hole"]
        radius = hole / 2

        center = dict(x=.5,y=.5)

        if self._intervals.domain.x != None:
            center["x"] = float((self._intervals.domain.x[1] - self._intervals.domain.x[0]) / 2)

        if self._intervals.domain.y != None:
            center["y"] = float((self._intervals.domain.y[1] - self._intervals.domain.y[0]) / 2)
        linewidth = self._compass.getOpt("line_width",1)

        r = .05

        # Heading Pointer

        radians = heading * (math.pi / 180)

        length = radius - .20

        x = center["x"] + (length * math.cos(radians))
        y = center["y"] + (length * math.sin(radians))

        radians = (heading - 75) * (math.pi / 180)
        x0 = center["x"] + (r * math.cos(radians))
        y0 = center["y"] + (r * math.sin(radians))

        radians = (heading + 75) * (math.pi / 180)
        x1 = center["x"] + (r * math.cos(radians))
        y1 = center["y"] + (r * math.sin(radians))

        path = ""
        path += "M " + str(x0) + " " + str(y0)
        path += "L " + str(x) + " " + str(y)
        path += "L " + str(x1) + " " + str(y1)

        color = self._compass.getOpt("needle_color","white")
        headingPointer = go.layout.Shape(type="path",path=path,fillcolor=color,line_width=linewidth)

        # End Heading Pointer

        # Reciprocal Pointer

        radians = ((heading + 180) % 360) * (math.pi / 180)

        length = radius - .30

        x = center["x"] + (length * math.cos(radians))
        y = center["y"] + (length * math.sin(radians))

        radians = (heading - 75) * (math.pi / 180)
        x0 = center["x"] + (r * math.cos(radians))
        y0 = center["y"] + (r * math.sin(radians))

        radians = (heading + 75) * (math.pi / 180)
        x1 = center["x"] + (r * math.cos(radians))
        y1 = center["y"] + (r * math.sin(radians))

        path = ""
        path += "M " + str(x0) + " " + str(y0)
        path += "L " + str(x) + " " + str(y)
        path += "L " + str(x1) + " " + str(y1)

        color = self._compass.getOpt("reciprocal_color","white")
        reciprocalPointer = go.layout.Shape(type="path",path=path,fillcolor=color,line_width=linewidth)

        # End Reciprocal Pointer

        rr = r + .010
        color = self._compass.getOpt("center_color",self._compass.getOpt("outer_color","white"))
        needleCenter = go.layout.Shape(type="circle",x0=center["x"] - rr,y0=center["y"] - rr,x1=center["x"] + rr,y1=center["y"] + rr,fillcolor=color,line_width=linewidth)

        shapes = [headingPointer,reciprocalPointer]
        shapes = [self._innerCircle,headingPointer,reciprocalPointer,needleCenter]
        labels = []
        label = go.layout.Annotation(text=str(int(self._heading)),x=center["x"],y=center["y"],showarrow=False,font=dict(size=12))
        #labels.append(label)

        color = colors.Colors.getColorFromName(self._compass.getOpt("entry_bg","white"))
        self._figure.layout["paper_bgcolor"] = color

        self._figure.update_layout(shapes=shapes,annotations=labels)

        self.setTitle()

    def setTitle(self):
        if self._compass.getOpt("singleton",False):
            self.layout.border = "0"
            return

        title = self.getOpt("title")

        if title != None:
            html = ""
            html += "<table style='"
            html += self._compass._visuals.getHeaderStyle()
            html += ";width:100%"
            html += "'>"
            html += "<tr>"
            html += "<td style='text-align:left'>" + title + "</td>"
            html += "<td style='text-align:right'>" + str(int(self._heading)) + "</td>"
            html += "</tr>"
            html += "</table>"
            s = str(int(self._heading))
            tmp = ""
            for i in range(3 - len(s)):
                tmp += "&nbsp;"
            tmp += s
            text = "{title} ({heading})".format(title=title,heading=tmp)
            tmp = "<div style='font:fixed'>" + text + "</div>"
            self._header.value = self._compass._visuals.formatTitle(html)

        self.layout.border = self._compass.getOpt("border","1px solid #d8d8d8")

    @property
    def heading(self):
        return(self._heading)

    @heading.setter
    def heading(self,value):
        self._heading = value
        self.draw()

    @property
    def title(self):
        return(self._header)

class ControlPanel(widgets.HBox):
    def __init__(self,datasource):

        widgets.HBox.__init__(self)
        self.layout = widgets.Layout(padding="5px")

        self.add_class("chart_controls")

        self._datasource = datasource

        self._info = None

        if isinstance(self._datasource,connections.EventCollection):
            self._info = self._datasource.getInfo()

        components = []

        self._filter = widgets.Text(description="Filter",layout=widgets.Layout(width="50%"))
        b = widgets.Button(description="Set",layout=widgets.Layout(width="50px"))
        b.on_click(self.filter)

        self._buttons = None

        self._playPauseButton = widgets.Button(icon="fa-pause",layout=widgets.Layout(width="40px",margin="0px 0px 0px 20px"))
        self._playPauseButton.on_click(self.playPause)

        if self._datasource._paused:
            self._playPauseButton.icon = "fa-play"

        if isinstance(self._datasource,connections.EventCollection):
            iconLayout = widgets.Layout(width="40px")

            self._nextButton = widgets.Button(icon="fa-step-forward",layout=iconLayout)
            self._prevButton = widgets.Button(icon="fa-step-backward",layout=iconLayout)
            self._firstButton = widgets.Button(icon="fa-backward",layout=iconLayout)
            self._lastButton = widgets.Button(icon="fa-forward",layout=iconLayout)

            self._nextButton.on_click(self.next)
            self._prevButton.on_click(self.prev)
            self._firstButton.on_click(self.first)
            self._lastButton.on_click(self.last)

            self._buttons = widgets.HBox([self._nextButton,self._prevButton,self._firstButton,self._lastButton,self._playPauseButton],layout=widgets.Layout(width="300px"))

        else:
            self._buttons = widgets.HBox([self._playPauseButton],layout=widgets.Layout(width="100px"))

            #if self._datasource._pages == 1:
                #self._buttons.layout.display = "none"
            #else:
                #self._buttons.layout.display = "block"

        components.append(self._buttons)

        w = "80%"
        justify = "flex-end"
        if self._info == None or self._datasource._pages == 1:
            w = "100%"
            justify = "center"
        self._filterBox = widgets.HBox([self._filter,b],layout=widgets.Layout(width=w,justify_content=justify))
        components.append(self._filterBox)

        #self._panel = widgets.HBox(components,layout=widgets.Layout(width="100%",border="1px solid #d8d8d8"))

        self._filter.value = self._datasource.getFilter()

        self.children = components

    def next(self,b):
        self._datasource.next()

    def prev(self,b):
        self._datasource.prev()

    def first(self,b):
        self._datasource.first()

    def last(self,b):
        self._datasource.last()

    def playPause(self,b):
        code = self._datasource.togglePlay()
        if code:
            self._playPauseButton.icon = "fa-pause"
        else:
            self._playPauseButton.icon = "fa-play"

    def filter(self,b):
        self._datasource.setFilter(self._filter.value)
        self._datasource.load()

    def processInfo(self):
        if isinstance(self._datasource,connections.EventCollection):
            #if self._datasource._pages == 1:
                #self._buttons.layout.display = "none"
            #else:
                #self._buttons.layout.display = "block"

            self._firstButton.disabled = (self._datasource._pages == 1)
            self._lastButton.disabled = (self._datasource._pages == 1)
            self._nextButton.disabled = (self._datasource._page == (self._datasource._pages - 1))
            self._prevButton.disabled = (self._datasource._page == 0)

            if self._datasource._paused:
                self._playPauseButton.icon = "fa-play"
            else:
                self._playPauseButton.icon = "fa-pause"

class Controls(Chart):

    def __init__(self,visuals,datasource,layout,**kwargs):
        Chart.__init__(self,visuals,datasource,layout,**kwargs)
        self._panel = ControlPanel(self._datasource)
        self.children = [self._header,self._panel]

    def draw(self,data,clear):
        pass

    def info(self,data):
        self._panel.processInfo()
        self.setTitle()

class Wrapper(Chart):

    def __init__(self,visuals,widget,layout,**kwargs):
        Chart.__init__(self,visuals,None,layout,**kwargs)
        self._widget = widget
        self._content
        self._content.children = [self._widget]
        self.setDisplay()
        self.draw(None,False)

    def draw(self,data,clear):
        self.setTitle()

    @property
    def widget(self):
        return(self._widget)

class XDashboard(Chart):
    def __init__(self,visuals,layout,**kwargs):
        Chart.__init__(self,visuals,layout,**kwargs)

        self._content = widgets.Box(layout=widgets.Layout(width="100%",height="auto",display="inline_flex",flex_flow="row wrap",justify_content="center"))

        #self.remove_class(self._visuals.css + "_chart")
        #self.add_class(self._visuals.css + "_dashboard")

        #self._container.remove_class(self._visuals.css + "_container")
        #self._widget.add_class(self._visuals.css + "_dashboard_container")

        #self._header.remove_class(self._visuals.css + "_header")
        #self._header.add_class(self._visuals.css + "_dashboard_header")

        self._children = []

        #self.draw(None,False)

    def add(self,value):
        if isinstance(value,list):
            for v in value:
                self._children.append(v)
        else:
            self._children.append(value)

        self._content.children = self._children

    def draw(self,data,clear):
        self.setTitle()

class Dashboard(Chart):
    def __init__(self,visuals,layout,**kwargs):
        Chart.__init__(self,visuals,None,layout,**kwargs)

        #self._content.layout.flex_flow = "column wrap"

        self.remove_class(self._visuals.css + "_chart")
        self.add_class(self._visuals.css + "_dashboard")

        self._container.layout.justify_content = "flex-start"
        self._content.layout.height = "100%"

        #self._container.remove_class(self._visuals.css + "_container")
        #self._widget.add_class(self._visuals.css + "_dashboard_container")

        self._header.remove_class(self._visuals.css + "_header")
        self._header.add_class(self._visuals.css + "_dashboard_header")

        self._children = []

        self.draw(None,False)

    def add(self,value):
        if isinstance(value,list):
            for v in value:
                self._children.append(v)
        else:
            self._children.append(value)

        self._content.children = self._children

    def draw(self,data,clear):
        self.setTitle()
