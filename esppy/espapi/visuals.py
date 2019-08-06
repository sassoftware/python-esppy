from plotly import tools as ptools
import plotly.io as pio

import plotly.graph_objs as go

import esppy.espapi.connections as connections
import esppy.espapi.tools as tools
import esppy.espapi.viewers as viewers
import esppy.espapi.dashboard as dashboard

from esppy.espapi.tools import Options

import sys

import ipywidgets as widgets
import ipyleaflet as maps

import numpy as np

import logging

import math

import random

#pio.templates.default = "xgridoff"

class Visuals(Options):

    def __init__(self,**kwargs):
        Options.__init__(self,**kwargs)
        self._visuals = []
        self._border = None
        self._mapbox = None

        colormap = self.getOpt("colors")
        self._colors = tools.Colors(colormap)

        self._axisWidth = 1

    def createBarChart(self,datasource,**kwargs):
        datasource.addDelegate(self)
        chart = BarChart(self,datasource,**kwargs)
        self._visuals.append(chart)
        return(chart)

    def createLineChart(self,datasource,**kwargs):
        datasource.addDelegate(self)
        chart = LineChart(self,datasource,**kwargs)
        self._visuals.append(chart)
        return(chart)

    def createTimeSeries(self,datasource,**kwargs):
        datasource.addDelegate(self)
        chart = TimeSeries(self,datasource,**kwargs)
        self._visuals.append(chart)
        return(chart)

    def createBubbleChart(self,datasource,**kwargs):
        datasource.addDelegate(self)
        chart = BubbleChart(self,datasource,**kwargs)
        self._visuals.append(chart)
        return(chart)

    def createPieChart(self,datasource,**kwargs):
        datasource.addDelegate(self)
        chart = PieChart(self,datasource,**kwargs)
        self._visuals.append(chart)
        return(chart)

    def createMap(self,datasource,**kwargs):
        datasource.addDelegate(self)
        chart = Map(self,datasource,**kwargs)
        self._visuals.append(chart)
        return(chart)

    def createGauge(self,datasource,**kwargs):
        datasource.addDelegate(self)
        chart = Gauge(self,datasource,**kwargs)
        self._visuals.append(chart)
        return(chart)

    def createTable(self,datasource,**kwargs):
        datasource.addDelegate(self)
        chart = Table(self,datasource,**kwargs)
        self._visuals.append(chart)
        return(chart)

    def createModelViewer(self,connection,**kwargs):
        return(viewers.ModelViewer(self,connection,**kwargs))

    def createLogViewer(self,connection,**kwargs):
        return(viewers.LogViewer(self,connection,**kwargs))

    def createStatsViewer(self,connection,**kwargs):
        return(viewers.StatsViewer(self,connection,**kwargs))

    def createDashboard(self,**kwargs):
        return(dashboard.Dashboard(**kwargs))

    def dataChanged(self,datasource,data):
        for v in self._visuals:
            if v._datasource == datasource:
                v.draw()

    def infoChanged(self,datasource):
        for v in self._visuals:
            if v._datasource == datasource:
                v.info()

    def handleStats(self,datasource):
        for v in self._visuals:
            if v._datasource == datasource:
                v.draw()

    def clear(self):
        self._visuals = []

    @property
    def mapbox(self):
        return(self._mapbox)

    @mapbox.setter
    def mapbox(self,value):
        self._mapbox = value

class Chart(Options):
    def __init__(self,visuals,datasource,**kwargs):
        Options.__init__(self,**kwargs)
        self._visuals = visuals
        self._datasource = datasource
        self._dashboard = None
        self._figure = None
        self._data = None
        self._box = None
        self._layout = None
        self._controls = None

    def setWidth(self,value):
        self.setOpt("width",value)

    def setHeight(self,value):
        self.setOpt("height",value)

    @property
    def display(self):
        if self._box == None:
            self.build()
            self._figure = go.FigureWidget(data=self._data,layout=self._layout)
            w = [self._figure]
            if self.getOpt("showcontrols",False):
                self._controls = ControlPanel(self._datasource) 
                w.append(self._controls.display)
            border = self._visuals.getOpt("border")
            padding = self._visuals.getOpt("padding")
            if padding != None:
                padding = str(padding) + "px"

            layout = widgets.Layout()
            if border != None:
                layout.border = border
            if padding != None:
                layout.padding = padding
            self._box = widgets.VBox(w,layout=layout)
            self.draw()
        return(self._box)

    def build(self):
        width = self.getOpt("width")
        height = self.getOpt("height")

        self._layout = go.Layout(width=width,height=height)

        xRange = self.getOpt("xrange")
        if xRange != None:
            self._layout["xaxis"]["range"] = xRange
        yRange = self.getOpt("yrange")
        if yRange != None:
            self._layout["yaxis"]["range"] = yRange

        self._layout["xaxis"]["showticklabels"] = self.getOpt("showticks",True)
        self._layout["xaxis"]["showline"] = False

    def setTitle(self):
        if self._figure == None:
            return

        title = self.getOpt("title")
        if title == None:
            title = self._datasource._path

        if isinstance(self._datasource,connections.EventCollection):
            if self._datasource._pages > 1:
                    title += " (Page " + str(self._datasource._page + 1) + " of " + str(self._datasource._pages) + ")"

        filter = self._datasource.getOpt("filter")

        if filter != None:
            title += "<br>"
            title += filter

        self._figure.layout.title = title

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

    def info(self):
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
    def __init__(self,visuals,datasource,**kwargs):
        Chart.__init__(self,visuals,datasource,**kwargs)
        self._orientation = self.getOpt("orientation","vertical")

    def build(self):
        Chart.build(self)

        values = self.getValues("y")
        colors = self._visuals._colors.getFirst(len(values))
        opacity = self.getOpt("opacity")

        self._data = []

        if self._orientation == "horizontal":
            for i,v in enumerate(values):
                self._data.append(go.Bar(x=[0],y=[""],name=v,orientation="h",marker_color=colors[i]))

        else:
            for i,v in enumerate(values):
                self._data.append(go.Bar(x=[""],y=[0],name=v,opacity=opacity,marker_color=colors[i]))

    def draw(self):
        if self._figure == None:
            return

        self._figure.update_xaxes(showline=True,linewidth=self._visuals._axisWidth)
        self._figure.update_yaxes(showline=True,linewidth=self._visuals._axisWidth)

        x = self.getValues("x")
        values = self.getValues("y")

        if self._orientation == "horizontal":

            if len(x) > 0:
                data = self._datasource.getValuesBy(x,values)

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

        self.setTitle()

class LineChart(Chart):
    def __init__(self,visuals,datasource,**kwargs):
        Chart.__init__(self,visuals,datasource,**kwargs)

    def build(self):
        Chart.build(self)

        values = self.getValues("y")

        self._data = []

        width = self.getOpt("linewidth",2)
        shape = "linear"
        if self.getOpt("curved",False):
            shape = "spline"
        line = {"width":width,"shape":shape}

        fill = self.getOpt("fill",False)

        colors = self._visuals._colors.getFirst(len(values))

        for i,v in enumerate(values):
            if fill:
                if i == 0:
                    self._data.append(go.Scatter(x=[""],y=[0],name=v,mode="none",fill="tozeroy",fillcolor=colors[i]))
                else:
                    self._data.append(go.Scatter(x=[""],y=[0],name=v,mode="none",fill="tonexty",fillcolor=colors[i]))
            else:
                line["color"] = colors[i]
                self._data.append(go.Scatter(x=[""],y=[0],name=v,mode="lines",line=line))

    def draw(self):
        if self._figure == None:
            return

        self._figure.update_xaxes(showline=True,linewidth=self._visuals._axisWidth)
        self._figure.update_yaxes(showline=True,linewidth=self._visuals._axisWidth)

        values = self.getValues("y")
        x = self.getValues("x")

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

            for i,v in enumerate(values):
                y = self._datasource.getValues(v)
                self._figure.data[i].x = keys
                self._figure.data[i].y = y

        self.setTitle()

class TimeSeries(LineChart):
    def __init__(self,visuals,datasource,**kwargs):
        LineChart.__init__(self,visuals,datasource,**kwargs)

        if self.hasOpt("time") == False:
            raise Exception("must specify time field for a TimeSeries")
 
        self.setOpt("x",self.getOpt("time"))

class PieChart(Chart):
    def __init__(self,visuals,datasource,**kwargs):
        Chart.__init__(self,visuals,datasource,**kwargs)

    def build(self):
        Chart.build(self)

        value = self.getValues("value")

        self._data = []

        if len(value) == 1:
            self._data.append(go.Pie(labels=[""],values=[0],name=value[0]))

    def draw(self):
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
    def __init__(self,visuals,datasource,**kwargs):
        Chart.__init__(self,visuals,datasource,**kwargs)

    def build(self):
        Chart.build(self)

        values = self.getValues("y")

        self._data = []

        for v in values:
            self._data.append(go.Scatter(x=[""],y=[0],name=v,mode="markers"))

    def draw(self):
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

        self.setTitle()

class Table(Chart):
    def __init__(self,visuals,datasource,**kwargs):
        Chart.__init__(self,visuals,datasource,**kwargs)

    def build(self):
        Chart.build(self)
        fields = self._datasource.getFields()

        header = []

        for f in fields:
            header.append(f["name"])

        self._data = []
        self._data.append(go.Table(header=dict(values=header)))

    def draw(self):
        if self._figure == None:
            return

        allfields = self._datasource.getFields()
        columns = self.getValues("values")

        fields = []

        for f in allfields:
            if f["isKey"]:
                fields.append(f)
            elif len(columns) > 0:
                if f["name"] in columns:
                    fields.append(f)
            else:
                fields.append(f)

        header = []

        for f in fields:
            header.append(f["name"])

        color = self.getOpt("color")

        fill = None
        bg = None
        numcolors = 100

        cells = []

        numrows = 0

        for i,f in enumerate(fields):
            v = self._datasource.getValues(f["name"])
            if len(v) > 0:
                cells.append(v)
                if color != None and f["name"] == color:
                    baseColor = self._visuals._colors.lightest
                    gradient = tools.Gradient(baseColor,levels=100,min=min(v),max=max(v))
                    fill = []
                    bg = []
                    for value in v:
                        fill.append(gradient.darken(value))
                        bg.append("#ffffff")

        self._figure.data[0].header.values = header
        self._figure.data[0].cells.values = cells

        if fill != None:
            a = []
            for f in fields:
                if f["name"] == color:
                    a.append(fill)
                else:
                    a.append(bg)
            self._figure.data[0].cells.fill = dict(color=a)

        self.setTitle()

class Map(Chart):
    def __init__(self,visuals,datasource,**kwargs):
        Chart.__init__(self,visuals,datasource,**kwargs)

        self._map = maps.Map()
        self._lat = None
        self._lon = None
        self._markers = {}
        self._colorbar = None

        self._circles = []
        self._polygons = []

        self._box = None

        self._icon = None

    def build(self):
        if self._box == None:

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

            components = []

            if self.hasOpt("title"):
                components.append(widgets.Label(value=self.getOpt("title")))

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

            self._box = widgets.VBox(components,layout=widgets.Layout(width="100%"))

    def draw(self):
        if self._box == None:
            return

        data = self._datasource.getData()

        popup = self.getValues("popup")

        value = self.getOpt("size")

        c = self._visuals._colors.getColor(self.getOpt("color"))

        sizePixels = None
        sizeField = None

        if value != None: 
            try:
                sizePixels = int(value)
            except:
                sizeField = value

        colorField = None

        value = self.getOpt("color")

        if value != None: 
            colorField = value

        minSize = sys.maxsize
        maxSize = 0

        minColor = sys.maxsize
        maxColor = 0

        sizes = self.getOpt("sizes",(1,20))

        sizeRange = []
        colorRange = None

        gradient = None

        if sizeField != None or colorField != None:
            if isinstance(data,dict):
                for key,value in data.items():
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
                colorRange = tools.ColorRange(self._visuals._colors,minColor,maxColor)
                if self._colorbar != None:
                    self._colorbar.data[0].marker.cmin = minColor
                    self._colorbar.data[0].marker.cmax = maxColor
                #baseColor = self.getOpt("base_color",self._visuals._colors.lightest)
                #gradient = tools.Gradient(baseColor,levels=200,min=minColor,max=maxColor)

        opacity = self.getOpt("marker_opacity",.5)
        border = self.getOpt("marker_border",True)

        if isinstance(data,dict):
            for key,value in data.items():
                if key in self._markers:
                    marker = self._markers[key]
                else:
                    if self._icon != None:
                        icon = maps.Icon(icon_url=self._icon,icon_size=[40,30])
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

                if sizePixels != None:
                    marker.radius = sizePixels
                elif len(sizeRange) > 0:
                    size = 0
                    if sizeField in value:
                        size = float(value[sizeField])
                    index = np.where(size >= sizeRange)[0]
                    marker.radius = len(index) - 1

                if colorRange != None:
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
                        text += s + "=" + value[s]
                    marker.popup.value = text

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
        if options.hasOpt("text"):
            o["text"] = options.getOpt("text")
        self._polygons.append(o)
        self.loadPolygons(o)
        datasource.addDelegate(self)

    def dataChanged(self,datasource,data):
        o = None

        for circle in self._circles:
            if circle["datasource"] == datasource:
                o = circle
                break

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
            circle.color = "red"
            circle.fill_color = "red"
            circle.stroke = True
            circle.weight = 1
            if "text" in o:
                text = value[o["text"]]
                circle.popup = widgets.HTML(value=text)
            o["layers"].add_layer(circle)

        if self._map != None:
            self._map.add_layer(o["layers"])
 
    def loadPolygons(self,o):
        o["layers"].clear_layers()

        data = o["datasource"].getData()

        for key,value in data.items():
            coords = value[o["coords"]]
            a = coords.split(" ")
            points = []
            i = 0
            while True:
                points.append((a[i + 1],a[i]))
                i += 2
                if i >= len(a):
                    break
            polygon = maps.Polygon(locations=points)
            polygon.stroke = True
            polygon.weight = 1
            if "text" in o:
                text = value[o["text"]]
                polygon.popup = widgets.HTML(value=text)
            polygon.color = "green"
            polygon.fill_color = "green"
            o["layers"].add_layer(polygon)

        if self._map != None:
            self._map.add_layer(o["layers"])

    @property
    def display(self):
        if self._box == None:
            self.build()

        self.draw()

        return(self._box)

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
    def __init__(self,visuals,datasource,**kwargs):
        Chart.__init__(self,visuals,datasource,**kwargs)
        self._colors = None
        self._intervalColors = None
        self._gauges = {}
        self.range = self.getOpt("range",(0,100))
        self._box = None

    def build(self):
        Chart.build(self)

        segments = self.getOpt("segments",3)

        self._space = 50
        self._space = 70

        interval = self._space / segments
        mod = self._space % segments

        self._intervalColors = ["lightblue"]
        self._intervalColors = ["white"]

        if self._colors != None:
            self._intervalColors.extend(self._colors)
        else:
            color = self._visuals._colors.lightest
            for i in range(0,segments):
                self._intervalColors.append(tools.darken(color,(i * 20)))

        self._intervalValues = [100 - self._space]
        self._intervalLabels = [" "]
        self._ticValues = [100 - self._space - 10]
        self._ticLabels = [" "]

        intervalSize = (self._range[1] - self._range[0]) / segments

        self._intervals = np.arange(self._range[0],self._range[1],intervalSize)

        for i in range(0,segments):
            value = int(interval)
            if mod > 0:
                value += 1
                mod -= 1
            self._intervalValues.append(value)
            self._intervalLabels.append("i" + str(i))

            self._ticValues.append(value)
            self._ticLabels.append(str(int(intervalSize * i)))

            if i == segments - 1:
                if mod > 0:
                    value += 1
                self._ticValues.append(value)
                self._ticLabels.append(str(int(intervalSize * (i + 1))))

        logging.info("VAL: " + str(self._intervalValues))
        logging.info("LABEL VAL: " + str(self._ticValues))
        logging.info("TICS: " + str(self._ticLabels))

        logging.info("+++++++: " + str(self._intervalValues))
        logging.info("+++++++: " + str(self._ticValues))

        if self._box == None:
            Chart.build(self)
            self._box = widgets.VBox()

    def draw(self):
        if self._box == None:
            return

        data = self._datasource.getData()

        gauges = None

        field = self.getOpt("value")

        if field == None:
            return

        if isinstance(data,dict):
            for key,o in data.items():
                if (key in self._gauges) == False:
                    gauge = GaugeEntry(self)
                    gauge.setOpts(title=key)
                    self._gauges[key] = gauge
                    if gauges == None:
                        gauges = []
                    gauges.append(gauge.display)
                else:
                    gauge = self._gauges[key]

                value = float(o[field])
                gauge.value = value

        if gauges != None:
            a = []
            if self._box.children != None:
                a.extend(self._box.children)
            a.extend(gauges)
            self._box.children = a

    @property
    def colors(self):
        return(self._colors)

    @colors.setter
    def colors(self,value):
        self._colors = value

    @property
    def range(self):
        return(self._range)

    @range.setter
    def range(self,value):
        if value[1] > value[0]:
            self._range = value

    @property
    def display(self):
        if self._box == None:
            self.build()
            self.draw()
        return(self._box)

class GaugeEntry(Options):
    def __init__(self,gauge,**kwargs):
        Options.__init__(self,**kwargs)
        self._gauge = gauge
        self._figure = None
        self._layout = None
        self._box = None
        self._value = 0

    def build(self):
        size = self._gauge.getOpt("size",400)

        self._layout = go.Layout(width=size,height=size)

        #angleRange = int((self._gauge._space / 100) * 360)

        self._data = []

        self._intervals = go.Pie(values=self._gauge._intervalValues,
            labels=self._gauge._intervalLabels,
            #domain={"x": [0, .50]},
            text=self._gauge._ticLabels,
            marker_colors=self._gauge._intervalColors,
            hole=.5,
            sort=False,
            direction="clockwise",
            rotation=int(((self._gauge._space / 100) * 360) / 2),
            showlegend=False,
            hoverinfo="none",
            #textposition="inside",
            textposition="outside",
            textinfo="none")

        self._tics = go.Pie(values=self._gauge._ticValues,labels=self._gauge._ticLabels,
            #marker=dict(colors=['white']*7, line_width=1),
            direction="clockwise",
            #domain={"x": [.5, 1]},
            rotation=int((((self._gauge._space + 10) / 100) * 360) / 2),
            hole=.5,
            sort=False,
            showlegend=False,
            hoverinfo="none",
            textposition="outside",
            textinfo="label")

        self._data.append(self._tics)
        self._data.append(self._intervals)

        self._figure = go.FigureWidget(data=self._data,layout=self._layout)
        
        self._box = widgets.Box([self._figure],layout=widgets.Layout(border="1px solid #d8d8d8",width=str(size) + "px",height=str(size) + "px"))
 
    def draw(self):
        if self._figure == None:
            return

        self._value = 100
        self._value = 200
        self._value = 0

        num = self._value / (self._gauge._range[1] - self._gauge._range[0])

        angleRange = (self._gauge._space / 100) * 360
        start = 360 * (self._gauge._space / 100)
        start = 360 - (360 - angleRange)

        start = 220

        logging.info("+++++++: " + str(360 - angleRange))
        logging.info("Start: " + str(start))
        logging.info("NUM: " + str(num) + " :: " + str(angleRange))

        #angle = 180 - (180 * num)
        #angle = angleRange - (angleRange * num)
        angle = start - (360 * num)
        angle = start - (angleRange * num)
        #angle = 235
        logging.info("ANGLE: " + str(angle))

        radians = angle * (math.pi / 180)

        radius = 1
        radius = self._intervals["hole"] / 2
        #radius -= .05

        center = dict(x=.5,y=.5)

        if self._intervals.domain.x != None:
            center["x"] = float((self._intervals.domain.x[1] - self._intervals.domain.x[0]) / 2)

        if self._intervals.domain.y != None:
            center["y"] = float((self._intervals.domain.y[1] - self._intervals.domain.y[0]) / 2)

        x = center["x"] + (radius * math.cos(radians))
        y = center["y"] + (radius * math.sin(radians))

        r = .05

        radians = (angle - 75) * (math.pi / 180)
        x0 = center["x"] + (r * math.cos(radians))
        y0 = center["y"] + (r * math.sin(radians))

        radians = (angle + 75) * (math.pi / 180)
        x1 = center["x"] + (r * math.cos(radians))
        y1 = center["y"] + (r * math.sin(radians))

        path = ""
        path += "M " + str(x0) + " " + str(y0)
        path += "L " + str(x) + " " + str(y)
        path += "L " + str(x1) + " " + str(y1)

        interval = np.where(self._value >= self._gauge._intervals)[0]

        fillcolor = self._gauge._intervalColors[len(interval)]

        shape = go.layout.Shape(type="path",path=path,fillcolor=fillcolor,line_width=0)
        c = go.layout.Shape(type="circle",x0=center["x"] - r,y0=center["y"] - r,x1=center["x"] + r,y1=center["y"] + r,fillcolor=fillcolor,line_width=0)

        shapes = [shape,c]

        label = go.layout.Annotation(text=str(int(self._value)),x=center["x"],y=center["y"] - (r * 2),showarrow=False,font=dict(size=20))

        self._figure.update_layout(shapes=shapes,annotations=[label])

        #self.setTitle()

    @property
    def value(self):
        return(self._value)

    @value.setter
    def value(self,value):
        self._value = value
        self.draw()

    @property
    def display(self):
        if self._box == None:
            self.build()
        return(self._box)

class ControlPanel(object):
    def __init__(self,datasource):
        self._datasource = datasource

        self._info = None

        if isinstance(self._datasource,connections.EventCollection):
            self._info = self._datasource.getInfo()

        components = []

        self._filter = widgets.Text(description="Filter",layout=widgets.Layout(width="50%"))
        b = widgets.Button(description="Set",layout=widgets.Layout(width="50px"))
        b.on_click(self.filter)

        self._buttons = None

        if isinstance(self._datasource,connections.EventCollection):
            iconLayout = widgets.Layout(width="40px")
            self._nextButton = widgets.Button(icon="fa-step-forward",layout=iconLayout)
            self._prevButton = widgets.Button(icon="fa-step-backward",layout=iconLayout)
            self._firstButton = widgets.Button(icon="fa-backward",layout=iconLayout)
            self._lastButton = widgets.Button(icon="fa-forward",layout=iconLayout)
            self._playPauseButton = widgets.Button(icon="fa-pause",layout=widgets.Layout(width="40px",margin="0px 0px 0px 20px"))

            if self._datasource._paused:
                self._playPauseButton.icon = "fa-play"

            self._nextButton.on_click(self.next)
            self._prevButton.on_click(self.prev)
            self._firstButton.on_click(self.first)
            self._lastButton.on_click(self.last)
            self._playPauseButton.on_click(self.playPause)

            #self._buttons = widgets.HBox([self._nextButton,self._prevButton,self._firstButton,self._lastButton],layout=widgets.Layout(width="200px"))
            self._buttons = widgets.HBox([self._nextButton,self._prevButton,self._firstButton,self._lastButton,self._playPauseButton],layout=widgets.Layout(width="300px"))

            if self._datasource._pages == 1:
                self._buttons.layout.display = "none"
            else:
                self._buttons.layout.display = "block"

            components.append(self._buttons)

        w = "80%"
        justify = "flex-end"
        if self._info == None or self._datasource._pages == 1:
            w = "100%"
            justify = "center"
        self._filterBox = widgets.HBox([self._filter,b],layout=widgets.Layout(width=w,justify_content=justify))
        components.append(self._filterBox)

        #self._panel = widgets.HBox(components,layout=widgets.Layout(width="100%",border="1px solid #d8d8d8"))
        self._panel = widgets.HBox(components,layout=widgets.Layout(width="100%"))

        self._filter.value = self._datasource.getFilter()

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
            if self._datasource._pages == 1:
                self._buttons.layout.display = "none"
            else:
                self._buttons.layout.display = "block"

            self._nextButton.disabled = (self._datasource._page == (self._datasource._pages - 1))
            self._prevButton.disabled = (self._datasource._page == 0)

            if self._datasource._paused:
                self._playPauseButton.icon = "fa-play"
            else:
                self._playPauseButton.icon = "fa-pause"

    @property
    def display(self):
        return(self._panel)
