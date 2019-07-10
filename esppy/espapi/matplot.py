import matplotlib.animation as animation
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import pandas as pd
import threading
import time
import seaborn as sns
import matplotlib
import numpy as np

import matplotlib.ticker as ticker

import esppy.espapi.connections as connections

import ipywidgets as widgets

import logging

import esppy.espapi.tools as tools

#print(plt.style.available)

class Charts(object):

    def __init__(self,**kwargs):
        self._options = tools.Options(**kwargs)
        self._charts = []

        self._jupyter = False

        try:
            get_ipython
            self._jupyter = True
        except:
            pass

        plt.ioff()

        if self._options.has("style"):
            matplotlib.style.use(self._options.get("style"))

        if self._options.has("palette"):
            sns.set_palette(self._options.get("palette"))

    def display(self):
        plt.show()

    def createChart(self,datasource,**kwargs):

        datasource.addDelegate(self)

        chart = Chart(self,datasource,**kwargs)
        self._charts.append(chart)

        return(chart)

    def createDashboard(self,**kwargs):
        dashboard = Dashboard(self,**kwargs)
        return(dashboard)

    def createControlPanels(self):
        panels = ControlPanels()
        return(panels)

    def dataChanged(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.draw()

    def infoChanged(self,datasource):
        pass

    def handleStats(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.draw()

    def clear(self):
        self._charts = []
        plt.close()

class Chart(object):
    def __init__(self,charts,datasource,**kwargs):
        self._charts = charts
        self._datasource = datasource
        self._options = tools.Options(**kwargs)
        self._type = self._options.get("type","vbar")
        self._dashboard = None
        self._figure = None
        self._axis = None

    def display(self):
        logging.info("display")
        self.clear()
        width = self.getOption("width",10)
        height = self.getOption("height",5)

        name = self._options.get("name")
        if name != None:
            self._figure = plt.figure(num=name,figsize=(width,height));
        else:
            self._figure = plt.figure(figsize=(width,height));

        #self._figure.subplots_adjust(top=1,bottom=1)

        self._axis = self._figure.add_subplot(111)
        if self._charts._jupyter:
            plt.show()
        self.draw()

    def displayInDashboard(self,dashboard,dim,coordinate,rowspan,colspan):
        self.clear()
        self._dashboard = dashboard
        plt.figure(self._dashboard._figure.number);
        self._figure = self._dashboard._figure
        self._axis = plt.subplot2grid(dim,coordinate,rowspan=rowspan,colspan=colspan);
        name = self._options.get("name")
        if name != None:
            self._axis.set_title(name)

    def clear(self):
        if self._dashboard == None:
            if self._figure != None:
                plt.figure(self._figure.number);
                plt.close()

        self._figure = None
        self._axis = None

    def draw(self):
        if self._axis == None:
            return

        self._axis.clear()

        xmin = self._options.get("xmin")
        xmax = self._options.get("xmax")

        if xmin != None:
            self._axis.set_xlim(left=xmin)
        if xmax != None:
            self._axis.set_xlim(right=xmax)

        ymin = self._options.get("ymin")
        ymax = self._options.get("ymax")

        if ymin != None:
            self._axis.set_ylim(bottom=ymin)
        if ymax != None:
            self._axis.set_ylim(top=ymax)

        if self._type == "vbarx":

            keys = self._datasource.getKeyFields()
            y = self.getValues("y")

            values = []
            values.extend(keys)
            values.extend(y)

            df = self._datasource.getDataFrame(values)

            if df.empty:
                return

            df = df.melt(id_vars=keys)

            chart = sns.barplot(x=keys[0],y="value",hue="variable",ax=self._axis,data=df)
            #chart.set_xticklabels(chart.get_xticklabels(),rotation=45,horizontalAlignment="right",fontweight="light")
            self._axis.legend(loc="upper center",bbox_to_anchor=(0.5,-0.05),fancybox=True,shadow=True,ncol=len(values))

        if self._type == "vbar":

            keys = self._datasource.getKeyFields()
            y = self.getValues("y")

            values = []
            values.extend(keys)
            values.extend(y)

            df = self._datasource.getDataFrame(values)

            if df.empty:
                return

            #df = df.melt(id_vars=keys)

            legendLoc = self._options.get("legend","upper right")

            if len(keys) > 1:
                if len(keys) == 3:
                    df = df.melt(id_vars=keys)
                    #logging.info(str(df))
                    #sns.catplot(y="value",x=keys[0],kind="bar",data=df)
                    #sns.catplot(y="cpu",x="window",row="project",col="contquery",kind="bar",data=df,ax=self._axis)
                    #sns.catplot(y="cpu",x="project",row="project",col="contquery",kind="bar",data=df,ax=self._axis)
                    #g = sns.catplot(y="value",x="project",row="contquery",col="window",kind="bar",data=df,ax=self._axis)
                    #g = sns.catplot(y="value",x="project",row="contquery",col="window",kind="bar",data=df,ax=self._axis)
                    sns.catplot(x="project",y="value",kind="bar",data=df,ax=self._axis)
            else:
                df = df.melt(id_vars=keys)
                chart = sns.barplot(x=keys[0],y="value",hue="variable",ax=self._axis,data=df)
                #chart.set_xticklabels(chart.get_xticklabels(),rotation=45,horizontalAlignment="right",fontweight="light")
                #self._axis.legend(loc="upper center",bbox_to_anchor=(0.5,-0.05),fancybox=True,shadow=True,ncol=len(values))
                #self._axis.legend(loc="best",bbox_to_anchor=(0.5,-0.05),fancybox=True,shadow=True,ncol=len(values))
                self._axis.legend(loc=legendLoc,fancybox=True,shadow=True)

            self._axis.set_xlabel("")
            self._axis.set_ylabel("")
            self._axis.xaxis.labelpad = 50

        elif self._type == "hbar":

            keys = self._datasource.getKeyFields()
            y = self.getValues("y")

            values = []
            values.extend(keys)
            values.extend(y)

            df = self._datasource.getDataFrame(values)

            if df.empty:
                return

            df = df.melt(id_vars=keys)

            legendLoc = self._options.get("legend","upper right")

            sns.barplot(x="value",y=keys[0],hue="variable",ax=self._axis,data=df,orient="h")
            #self._axis.legend(loc="upper center",bbox_to_anchor=(0.5,-0.05),fancybox=True,shadow=True,ncol=len(values))
            self._axis.legend(loc=legendLoc,fancybox=True,shadow=True)

            self._axis.set_xlabel("")
            self._axis.set_ylabel("")
            #self._axis.tick_params(axis="x",rotation=70)

        elif self._type == "line":

            keys = self._datasource.getKeyFields()
            y = self.getValues("y")

            values = []
            values.extend(keys)
            values.extend(y)

            df = self._datasource.getDataFrame(values)

            if df.empty:
                return

            df = df.melt(id_vars=keys)

            lineWidth = self.getOption("linewidth",2)

            legendLoc = self._options.get("legend","upper right")

            chart = sns.lineplot(x=keys[0],y="value",hue="variable",ax=self._axis,data=df,linewidth=lineWidth)
            self._axis.legend(loc=legendLoc,fancybox=True,shadow=True)

            self._axis.set_xlabel("")
            self._axis.set_ylabel("")

            if isinstance(self._datasource,connections.EventStream):
                labels = chart.get_xticklabels()
                empty = [""] * len(labels)
                chart.set_xticklabels(empty)

            #chart.set_xticklabels(chart.get_xticklabels(),rotation=45,horizontalAlignment="right",fontweight="light")

        elif self._type == "series":
            lineWidth = self.getOption("linewidth",1)
            lineStyle = self.getOption("linestyle","solid")
            keys = self._datasource.getKeyFields()
            y = self.getValues("y")
            values = []
            values.extend(keys)
            values.extend(y)
            df = self._datasource.getDataFrame(values)
            df = df.melt(id_vars=keys)
            if df.empty:
                return
            #sns.pointplot(x=keys[0],y="value",hue="variable",ax=self._axis,data=df,markers="None")
            sns.pointplot(x=keys[0],y="value",hue="variable",ax=self._axis,data=df,markers="o")
            #self._axis.legend(loc="upper center",bbox_to_anchor=(0.5,-0.05),fancybox=True,shadow=True,ncol=len(values))
            #self._axis.xaxis.set_major_locator(ticker.MultipleLocator(10))

        elif self._type == "pie":
            values = self._datasource.getValuesForFields(self.getValues("values"))
            keys = self._datasource.getKeyValues()
            for key,value in values.items():
                self._axis.pie(value,labels=keys,shadow=True,autopct='%1.1f%%')

        elif self._type == "scatter":
            keys = self._datasource.getKeyFields()
            y = self._options.get("y")
            size = self._options.get("size")
            color = self._options.get("color")

            values = []
            labels = []

            values.extend(keys)

            if y != None:
                if (y in values) == False:
                    values.append(y)
                    labels.append(y)

            if size != None:
                if (size in values) == False:
                    values.append(size)
                    labels.append(size)

            if color != None:
                if (color in values) == False:
                    values.append(color)
                    labels.append(color)

            df = self._datasource.getDataFrame(values)

            if df.empty:
                return

            #chart = sns.scatterplot(x=keys[0],y=y,size=size,hue=color,data=df,ax=self._axis,legend="brief")
            chart = sns.scatterplot(x=keys[0],y=y,size=size,hue=color,data=df,ax=self._axis,legend=False,sizes=(100,300))

            labels = self.getValues("labels")

            if labels != None:

                a = []

                ind = np.arange(len(chart.get_xticklabels()))

                data = self._datasource.getValues(y)

                for i in range(0,len(ind)):
                    a.append("")

                for i,l in enumerate(labels):
                    v = self._datasource.getValues(l)
                    if v != None:
                        for j,label in enumerate(v):
                            if i > 0:
                                a[j] += "\n"
                            a[j] += l + "=" + str(label)

                for i,label in enumerate(a):
                    self._axis.annotate(label,xy=(ind[i] + .1,data[i]))

            self._axis.set_xlabel("")
            self._axis.set_ylabel("")

            #legendLoc = self._options.get("legend","upper right")
            #self._axis.legend(loc="best",fancybox=True,shadow=True)

        elif self._type == "table":
            self._axis.axis("off")
            values = self.getValues("values")
            data = self._datasource.getTableData(values)
            if len(data["rows"]) > 0:
                table = self._axis.table(cellText=data["cells"],rowLabels=data["rows"],colLabels=data["columns"],loc="center")

        self._figure.canvas.draw()

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

class Dashboard(object):

    def __init__(self,charts,**kwargs):
        self._charts = charts
        self._options = tools.Options(**kwargs)
        self._rows = []
        self._figure = None

    def addRow(self,height = 5):
        row = DashboardRow(height)
        self._rows.append(row)
        return(row)

    def display(self):

        height = 0
        maxcols = 0

        for row in self._rows:
            height += row.height
            if row.size > maxcols:
                maxcols = row.size

        name = self._options.get("name")
        width = self._options.get("width",16)

        if name != None:
            self._figure = plt.figure(num=name,figsize=(width,height));
        else:
            self._figure = plt.figure(figsize=(width,height));

        dim = (len(self._rows),maxcols)

        rownum = 0

        for row in self._rows:
            colnum = 0
            for chart in row._charts:
                coordinate = (rownum,colnum)
                colspan = 1
                if colnum == (row.size - 1):
                    if colnum < maxcols:
                        colspan = (maxcols - row.size + 1)
                chart.displayInDashboard(self,dim,coordinate,1,colspan)
                colnum += 1
            rownum += 1

            #thread = threading.Thread(target = self.show)
            #thread.daemon = True
            #thread.start()

        time.sleep(1)

        plt.show()

        for row in self._rows:
            for chart in row._charts:
                chart.draw()

    def show(self):
        time.sleep(1)
        plt.show()

class DashboardRow(object):
    def __init__(self,height):
        self._height = height;
        self._charts = []

    def add(self,chart):
        self._charts.append(chart)

    def get(self,index):
        chart = None
        if index < self.size:
            chart = self._charts[index]
        return(chart)

    @property
    def height(self):
        return(self._height)

    @property
    def size(self):
        return(len(self._charts))

class ControlPanels(object):
    def __init__(self):
        self._panels = []

    def addPanel(self,datasource):
        datasource.addDelegate(self)
        panel = ControlPanel(datasource)
        self._panels.append(panel)

    def dataChanged(self,datasource):
        for p in self._panels:
            if p._datasource == datasource:
                p.processInfo()

    def infoChanged(self,datasource):
        for p in self._panels:
            if p._datasource == datasource:
                p.processInfo()

    def display(self):
        components = []
        for p in self._panels:
            components.append(p._panel)
        box = widgets.VBox(components)
        return(box)
        
class ControlPanel(object):
    def __init__(self,datasource):
        self._datasource = datasource

        self._info = None

        if tools.supports(self._datasource,"getInfo"):
            self._info = self._datasource.getInfo()

        title = ""
        if isinstance(self._datasource,connections.EventCollection):
            title += "collection: "
        else:
            title += "stream: "
        title += self._datasource._path

        self._title = widgets.Label(value=title)

        components = []

        components.append(self._title)

        self._filter = widgets.Text(description="Filter")
        b = widgets.Button(description="Set Filter")
        b.on_click(self.filter)

        components.append(widgets.HBox([self._filter,b],layout=widgets.Layout(padding="5px 5px 20px 5px")))

        self._buttons = None

        if isinstance(self._datasource,connections.EventCollection):
            self._nextButton = widgets.Button(description="Next")
            self._prevButton = widgets.Button(description="Prev")
            self._firstButton = widgets.Button(description="First")
            self._lastButton = widgets.Button(description="Last")

            self._nextButton.on_click(self.next)
            self._prevButton.on_click(self.prev)
            self._firstButton.on_click(self.first)
            self._lastButton.on_click(self.last)

            self._buttons = widgets.HBox([self._nextButton,self._prevButton,self._firstButton,self._lastButton])

            components.append(self._buttons)

        self._panel = widgets.VBox(components)

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
            self._info = self._datasource.getInfo()

            page = int(self._info["page"])
            pages = int(self._info["pages"])

            if pages == 1:
                self._buttons.layout.display = "none"
            else:
                self._buttons.layout.display = "block"

            self._nextButton.disabled = (page == (pages - 1))
            self._prevButton.disabled = (page == 0)

            title = ""
            title += "collection: "
            title += self._datasource._path

            if pages > 1:
                title += " (Page " + str(page + 1) + " of " + str(pages) + ")"

            self._title.value = title

