import matplotlib.animation as animation
import matplotlib.patches as patches
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

from IPython import display

import logging

import esppy.espapi.tools as tools

#print(plt.style.available)

class Charts(object):

    def __init__(self,options = None):
        self._options = tools.Options(options)
        self._collections = {}
        self._charts = []

        self._jupyter = False

        try:
            get_ipython
            self._jupyter = True
        except:
            pass

        plt.ioff()
        self.setStyle()

    def display(self):
        plt.show()

    def setOption(self,name,value):
        self._options.set(name,value)
        if len(self._delegates) > 0:
            self.set()

    def getOption(self,name,dv):
        return(self._options.get(name,dv))

    def createChart(self,type,datasource,values,options = None):

        datasource.addDelegate(self)

        chart = Chart(self,type,datasource,values,options)
        self._charts.append(chart)

        return(chart)

    def createDashboard(self,options):
        dashboard = Dashboard(self,options)
        return(dashboard)

    def dataChanged(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                chart.draw()

    def infoChanged(self,datasource):
        for chart in self._charts:
            if chart._datasource == datasource:
                pass

    def addTable(self,datasource,values,row,column):
        datasource.addDelegate(self)
        c = EspTable(self,datasource,values)
        c.coordinate = (row,column)
        self._charts.append(c)
        c.draw()
        return(c)

    def clear(self):
        self._charts = []
        plt.close()

    def setStyle(self):
        matplotlib.style.use("seaborn-dark")
        matplotlib.style.use("seaborn-darkgrid")
        matplotlib.style.use("seaborn-ticks")
        matplotlib.style.use("fivethirtyeight")
        matplotlib.style.use("seaborn-whitegrid")
        matplotlib.style.use("classic")
        matplotlib.style.use("_classic_test")
        matplotlib.style.use("fast")
        matplotlib.style.use("seaborn-talk")
        matplotlib.style.use("seaborn-dark-palette")
        matplotlib.style.use("grayscale")
        matplotlib.style.use("seaborn-notebook")
        matplotlib.style.use("ggplot")
        matplotlib.style.use("seaborn-colorblind")
        matplotlib.style.use("seaborn-muted")
        matplotlib.style.use("seaborn")
        matplotlib.style.use("Solarize_Light2")
        matplotlib.style.use("bmh")
        matplotlib.style.use("dark_background")
        matplotlib.style.use("seaborn-poster")
        matplotlib.style.use("seaborn-deep")
        matplotlib.style.use("seaborn-paper")
        matplotlib.style.use("seaborn-white")
        matplotlib.style.use("seaborn-bright")
        matplotlib.style.use("tableau-colorblind10")
        matplotlib.style.use("seaborn-pastel")

        #fontsize = 18
        #plt.rc("font",size=fontsize)

class Chart(object):
    def __init__(self,charts,type,datasource,values,options = None):
        self._charts = charts
        self._type = type
        self._datasource = datasource
        self._dashboard = None
        self.values = values
        self._options = tools.Options(options)
        self._figure = None
        self._axis = None

    def display(self):
        self.clear()
        width = self.getOption("width",10)
        height = self.getOption("height",5)

        name = self._options.get("name")
        if name != None:
            self._figure = plt.figure(num=name,figsize=(width,height));
        else:
            self._figure = plt.figure(figsize=(width,height));

        #self._figure.rasterized = True
        self._axis = self._figure.add_subplot(111)
        self.draw()
        if self._charts._jupyter:
            plt.show()

    def displayInDashboard(self,dashboard,dim,coordinate,rowspan,colspan):
        self.clear()
        self._dashboard = dashboard
        plt.figure(self._dashboard._figure.number);
        self._figure = self._dashboard._figure
        self._axis = plt.subplot2grid(dim,coordinate,rowspan=rowspan,colspan=colspan);
        name = self._options.get("name")
        if name != None:
            self._axis.set_title(name)
        self.draw()
        if self._charts._jupyter:
            plt.show()

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
        if self._type == "vbar":
            x = self._datasource.getKeyTuple()
            tuples = self._datasource.getTuples(self.values)

            ind = np.arange(len(x))

            if len(tuples) == 0:
                return

            w = 0.8 / len(tuples)
            num = ind - w

            for key,value in tuples.items():
                y = value
                if len(y) > 0:
                    self._axis.bar(num,y,label=key)
                    self._axis.legend()
                    num += w
            self._axis.set_xticks(ind - (w / len(tuples)))
            self._axis.set_xticklabels(x)
        elif self._type == "hbar":
            x = self._datasource.getKeyTuple()
            tuples = self._datasource.getTuples(self.values)

            ind = np.arange(len(x))

            if len(tuples) == 0:
                return

            w = 0.8 / len(tuples)
            num = ind - w

            for key,value in tuples.items():
                y = value
                if len(y) > 0:
                    self._axis.barh(num,y,label=key)
                    self._axis.legend()
                    num += w
            self._axis.set_yticks(ind - (w / len(tuples)))
            self._axis.set_yticklabels(x)
        elif self._type == "pie":
            tuples = self._datasource.getTuples(self.values)
            keyTuple = self._datasource.getKeyTuple()
            for t in tuples:
                y = tuples[t]
                self._axis.pie(y,labels=keyTuple,shadow=False,autopct='%1.1f%%')
        elif self._type == "series":
            lineWidth = self.getOption("linewidth",4)
            lineStyle = self.getOption("linestyle","solid")
            x = self._datasource.getKeyTuple()
            tuples = self._datasource.getTuples(self.values)
            for t in tuples:
                y = tuples[t]
                self._axis.plot(x,y,linewidth=lineWidth,linestyle=lineStyle,solid_joinstyle="round",label=t)
            self._axis.legend()
        elif self._type == "table":
            self._axis.axis("off")
            data = self._datasource.getTableData(self.values)
            if len(data["rows"]) > 0:
                table = self._axis.table(cellText=data["cells"],rowLabels=data["rows"],colLabels=data["columns"],loc="center")

        self._figure.canvas.draw()

    def setOption(self,name,value):
        self._options.set(name,value)
        if len(self._delegates) > 0:
            self.set()

    def getOption(self,name,dv):
        return(self._options.get(name,dv))

    @property
    def values(self):
        return(self._values)
    @values.setter
    def values(self,value):
        self._values = []

        if value != None:
            if type(value) is list:
                for v in value:
                    self._values.append(v)
            else:
                self._values.append(value)

class Dashboard(object):

    def __init__(self,charts,options):
        self._charts = charts
        self._options = tools.Options(options)
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

        #self._figure.rasterized = True

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
