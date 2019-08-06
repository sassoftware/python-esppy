import esppy.espapi.tools as tools

import ipywidgets as widgets

import logging

from esppy.espapi.tools import Options

class Dashboard(Options):

    def __init__(self,**kwargs):
        Options.__init__(self,**kwargs)
        self._rows = []
        self._container = None

    def addRow(self,height = 300):
        row = DashboardRow(height)
        self._rows.append(row)
        return(row)

    @property
    def display(self):

        height = 0
        maxcols = 0

        for row in self._rows:
            height += row.height
            if (row.size) > maxcols:
                maxcols = row.size

        if maxcols == 0:
            return(None)

        rows = ""
        columns = ""

        percentage = int(100 / maxcols)

        i = 0

        for row in self._rows:
            if row.size == 0:
                continue

            if i > 0:
                rows += " "
            rows += "auto"

        for i in range(0,maxcols):
            if i > 0:
                columns += " "
            columns += str(percentage) + "%"

        areas = ""

        border = self.getOpt("border","1px solid #d8d8d8")

        margin = self.getOpt("spacing")

        if margin != None:
            margin = str(margin) + "px"
        else:
            margin = "3px"

        charts = []

        for i,row in enumerate(self._rows):
            if row.size == 0:
                continue

            areas += "\n"
            areas += "\""
            for j,chart in enumerate(row._charts):
                if j > 0:
                    areas += " "
                areas += "cell" + str(i) + "_" + str(j)
                area="cell" + str(i) + "_" + str(j)
                chart.setHeight(row.height)
                layout = widgets.Layout(grid_area=area,justify_content="center",overflow="auto")
                layout.border = border
                layout.margin = margin
                charts.append(widgets.HBox([chart.display],layout=layout))
                if j == (row.size - 1):
                    if j < (maxcols - 1):
                        while j < (maxcols - 1):
                            areas += " cell" + str(i) + "_" + str(j)
                            j += 1

            areas += "\""

        self._container = widgets.GridBox(charts,layout=widgets.Layout(grid_template_rows=rows,grid_template_columns=columns,grid_template_areas=areas))

        return(self._container)

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
