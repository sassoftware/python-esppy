from plotly import tools as ptools

import plotly.graph_objs as go

import esppy.espapi.connections as connections
import esppy.espapi.tools as tools
import esppy.espapi.viewers as viewers
import esppy.espapi.dashboard as dashboard

import ipywidgets as widgets

import logging

class Visuals(object):

    def __init__(self,**kwargs):
        self._options = tools.Options(**kwargs)
        self._charts = []

        colormap = self._options.get("colors")
        self._colors = tools.Colors(colormap)

    def createChart(self,datasource,**kwargs):

        datasource.addDelegate(self)

        chart = Chart(self,datasource,**kwargs)
        self._charts.append(chart)

        return(chart)

    def createModelViewer(self,connection,**kwargs):
        return(viewers.ModelViewer(self,connection,colors=self._options.get("colors"),**kwargs))

    def createLogViewer(self,connection,**kwargs):
        return(viewers.LogViewer(self,connection,colors=self._options.get("colors"),**kwargs))

    def createDashboard(self,**kwargs):
        return(dashboard.Dashboard(**kwargs))

    def dataChanged(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.draw()

    def infoChanged(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.info()

    def handleStats(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.draw()

    def clear(self):
        self._charts = []

class Chart(object):
    def __init__(self,charts,datasource,**kwargs):
        self._charts = charts
        self._datasource = datasource
        self._options = tools.Options(**kwargs)
        self._type = self._options.get("type","vbar")
        self._dashboard = None
        self._figure = None
        self._data = None
        self._box = None
        self._layout = None
        self._controls = None

    def setWidth(self,value):
        self._options.set("width",value)

    def setHeight(self,value):
        self._options.set("height",value)

    @property
    def display(self):
        if self._box == None:
            figure = self.build()
            w = [figure]
            if self._options.get("showcontrols",False):
                self._controls = ControlPanel(self._datasource) 
                w.append(self._controls.display)
            self._box = widgets.VBox(w)
            self.draw()
        return(self._box)

    def build(self):
        opacity = self._options.get("opacity")

        width = self._options.get("width")
        height = self._options.get("height")

        self._layout = go.Layout(width=width,height=height)

        xRange = self._options.get("xrange")
        if xRange != None:
            self._layout["xaxis"]["range"] = xRange
        yRange = self._options.get("yrange")
        if yRange != None:
            self._layout["yaxis"]["range"] = yRange

        self._layout["xaxis"]["showticklabels"] = self._options.get("showticks",True)

        if self._type == "vbar":

            keys = self._datasource.getKeyValues()
            values = self.getValues("y")

            colors = self._charts._colors.getFirst(len(values))

            self._data = []

            for i,v in enumerate(values):
                y = self._datasource.getValues(v)
                #self._data.append(go.Bar(x=keys,y=y,name=v))
                self._data.append(go.Bar(x=keys,y=y,name=v,opacity=opacity,marker_color=colors[i]))

        elif self._type == "hbar":

            keys = self._datasource.getKeyValues()
            values = self.getValues("y")

            colors = self._charts._colors.getFirst(len(values))

            self._data = []

            for i,v in enumerate(values):
                y = self._datasource.getValues(v)
                self._data.append(go.Bar(x=y,y=keys,name=v,orientation="h",marker_color=colors[i]))

        elif self._type == "line":

            keys = self._datasource.getKeyValues()
            values = self.getValues("y")

            self._data = []

            width = self._options.get("linewidth",2)
            shape = self._options.get("linetype")
            line = {"width":width,"shape":shape}

            fill = self._options.get("fill",False)

            colors = self._charts._colors.getFirst(len(values))

            for i,v in enumerate(values):
                y = self._datasource.getValues(v)
                if fill:
                    if i == 0:
                        self._data.append(go.Scatter(x=keys,y=y,name=v,mode="none",fill="tozeroy"))
                    else:
                        self._data.append(go.Scatter(x=keys,y=y,name=v,mode="none",fill="tonexty"))
                else:
                    line["color"] = colors[i]
                    self._data.append(go.Scatter(x=keys,y=y,name=v,mode="lines",line=line))

        elif self._type == "bubble":

            keys = self._datasource.getKeyValues()
            values = self.getValues("y")

            self._data = []

            for v in values:
                y = self._datasource.getValues(v)
                self._data.append(go.Scatter(x=keys,y=y,name=v,mode="markers"))

        elif self._type == "pie":

            keys = self._datasource.getKeyValues()
            values = self.getValues("values")

            self._data = []

            for v in values:
                value = self._datasource.getValues(v)
                self._data.append(go.Pie(labels=keys,values=value,name=v))

        elif self._type == "map":

            keys = self._datasource.getKeyValues()

            values = self.getValues("values")
            lat = []
            lon = []

            opt = self._options.get("lat")
            if opt != None:
                lat = self._datasource.getValues(opt)

            opt = self._options.get("lon")
            if opt != None:
                lon = self._datasource.getValues(opt)

            self._data = []

            geo = go.layout.Geo()
            geo.scope = "usa"
            #geo.projection = go.layout.geo.Projection(type='albers usa')
            #geo.showland = True
            #geo.landcolor = "rgb(217, 217, 217)"
            self._layout.geo = geo

            self._data.append(go.Scattergeo(text=keys,lat=lat,lon=lon,locationmode="USA-states"))

        elif self._type == "table":
            fields = self._datasource.getFields()

            header = []

            for f in fields:
                header.append(f["name"])

            self._data = []
            self._data.append(go.Table(header=dict(values=header)))

        self._figure = go.FigureWidget(data=self._data,layout=self._layout)

        return(self._figure)

    def draw(self):

        if self._figure == None:
            return

        if self._type == "vbar":

            keys = self._datasource.getKeyValues()
            values = self.getValues("y")

            for i,v in enumerate(values):
                y = self._datasource.getValues(v)
                self._figure.data[i].x = keys
                self._figure.data[i].y = y

        elif self._type == "hbar":

            keys = self._datasource.getKeyValues()
            values = self.getValues("y")

            for i,v in enumerate(values):
                y = self._datasource.getValues(v)
                self._figure.data[i].x = y
                self._figure.data[i].y = keys

        elif self._type == "line":

            keys = self._datasource.getKeyValues()
            values = self.getValues("y")

            for i,v in enumerate(values):
                y = self._datasource.getValues(v)
                self._figure.data[i].x = keys
                self._figure.data[i].y = y

        elif self._type == "bubble":

            keys = self._datasource.getKeyValues()
            values = self.getValues("y")

            marker = {}

            size = self._options.get("size")
            color = self._options.get("color")

            text = None

            if size != None or color != None:

                text = []

                for i in range(0,len(keys)):
                    text.append("")

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
                            text[i] += size + "=" + str(v)

                #["Greens", "YlOrRd", "Bluered", "RdBu", "Reds", "Blues", "Picnic", "Rainbow", "Portland", "Jet", "Hot", "Blackbody", "Earth", "Electric", "Viridis", "Cividis"]

                color = self._options.get("color")

                if color != None:
                    s = self._datasource.getValues(color)
                    if s != None:
                        marker["color"] = s
                        marker["showscale"] = True
                        marker["colorscale"] = self._charts._colors.colorscale

                        for i,v in enumerate(s):
                            if size != None:
                                text[i] += "<br>"
                            text[i] += color + "=" + str(v)

            for i,v in enumerate(values):
                y = self._datasource.getValues(v)
                self._figure.data[i].x = keys
                self._figure.data[i].y = y
                self._figure.data[i].marker = marker
                self._figure.data[i].text = text

        elif self._type == "pie":

            keys = self._datasource.getKeyValues()
            values = self.getValues("values")

            for i,v in enumerate(values):
                value = self._datasource.getValues(v)
                self._figure.data[i].labels = keys
                self._figure.data[i].values = value

        elif self._type == "map":

            keys = self._datasource.getKeyValues()

            lat = []
            lon = []

            opt = self._options.get("lat")
            if opt != None:
                lat = self._datasource.getValues(opt)

            opt = self._options.get("lon")
            if opt != None:
                lon = self._datasource.getValues(opt)

            o = self.createMarkers()

            i = 0
            self._figure.data[i].lat = lat
            self._figure.data[i].lon = lon
            self._figure.data[i].text = keys
            self._figure.data[i].marker = o["marker"]
            self._figure.data[i].text = o["text"]

        elif self._type == "table":

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

            color = self._options.get("color")

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
                        baseColor = self._charts._colors.lightest
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

    def setTitle(self):
        title = self._options.get("title")
        if title == None:
            title = self._datasource._path

        if isinstance(self._datasource,connections.EventCollection):
            if self._datasource._pages > 1:
                    title += " (Page " + str(self._datasource._page + 1) + " of " + str(self._datasource._pages) + ")"

        filter = self._datasource.getOption("filter")

        if filter != None:
            title += "<br>"
            title += filter

        self._figure.layout.title = title

    def createMarkers(self):
        o = {}
        #marker = {}
        marker = go.scattergeo.Marker()

        keys = self._datasource.getKeyValues()

        text = []

        for i,key in enumerate(keys):
            text.append(key)

        size = self._options.get("size")
        color = self._options.get("color")

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

            #["Greens", "YlOrRd", "Bluered", "RdBu", "Reds", "Blues", "Picnic", "Rainbow", "Portland", "Jet", "Hot", "Blackbody", "Earth", "Electric", "Viridis", "Cividis"]

            color = self._options.get("color")

            if color != None:
                s = self._datasource.getValues(color)
                marker["color"] = s
                marker["showscale"] = True
                marker["colorscale"] = self._charts._colors.colorscale

                if size == None or color != size:
                    for i,v in enumerate(s):
                        if size != None and v in size == False:
                            text[i] += "<br>"
                        text[i] += color + "=" + str(v)

        o["marker"] = marker
        o["text"] = text

        return(o)

    def info(self):
        if self._controls != None:
            self._controls.processInfo()
            self.setTitle()

    def getValues(self,name):
        values = []

        value = self._options.get(name)

        if value != None:
            if type(value) is list:
                for v in value:
                    values.append(v)
            else:
                values.append(value)

        return(values)

    def setOption(self,name,value):
        self._options.set(name,value)

    def getOption(self,name,dv):
        return(self._options.get(name,dv))

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
            iconLayout = layout=widgets.Layout(width="40px")
            self._nextButton = widgets.Button(icon="fa-step-forward",layout=iconLayout)
            self._prevButton = widgets.Button(icon="fa-step-backward",layout=iconLayout)
            self._firstButton = widgets.Button(icon="fa-backward",layout=iconLayout)
            self._lastButton = widgets.Button(icon="fa-forward",layout=iconLayout)

            self._nextButton.on_click(self.next)
            self._prevButton.on_click(self.prev)
            self._firstButton.on_click(self.first)
            self._lastButton.on_click(self.last)

            self._buttons = widgets.HBox([self._nextButton,self._prevButton,self._firstButton,self._lastButton],layout=widgets.Layout(width="200px"))

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

    @property
    def display(self):
        return(self._panel)
