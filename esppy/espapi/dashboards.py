import esppy.espapi.api as api

class Dashboard(object):

    def __init__(self):
        self._rows = []
        self._current = None

    def addRow(self,height = 300):
        row = DashboardRow(height)
        self._rows.append(row)
        return(row)

    def _repr_html_(self):
        html = ""

        maxcols = 0

        for row in self._rows:
            if row.size > maxcols:
                maxcols = row.size

        html += '''
        <style type="text/css">
        .dashboardTable
        {
            width:100%;
            border:2px solid red;
        }

        td.dashboardCell
        {
            background:white;
            padding:0;
        }

        .dashboardContainer
        {
            padding:10px;
        }

        </style>
        '''

        html += "<table class='dashboardTable'>"

        for row in self._rows:
            html += "<tr>"
            for i in range(0,row.size):
                component = row.get(i)
                component.setHeight(row.height);
                html += "<td class='dashboardCell'"
                if i == 0 and row.size < maxcols:
                    html += " colspan='" + str(maxcols - row.size + 1) + "'"
                html += ">"
                html += "<div class='dashboardContainer' style='height:" + str(row.height) + "px'>";
                html += component.getHtml()
                html += "</div>";
                html += "</td>"
            html += "</tr>"

        html += "</table>"

        return(html)

class DashboardRow(object):
    def __init__(self,height = 300):
        self._height = height;
        self._components = []

    def add(self,component):
        if api.Delegate.supports(component,"setHeight") == False:
            raise Exception("Dashboard component must support the setHeight() method")
        if api.Delegate.supports(component,"getHtml") == False:
            raise Exception("Dashboard component must support the getHtml() method")
        self._components.append(component)

    def get(self,index):
        component = None
        if index < self.size:
            component = self._components[index]
        return(component)

    @property
    def height(self):
        return(self._height)

    @property
    def size(self):
        return(len(self._components))

class DashboardText(object):
    def __init__(self,text = "",css = {}):
        self._text = text
        self._css = css
        self._height = 20

    def getHeight(self):
        return(self._height)

    def setHeight(self,value):
        self._height = value

    def getHtml(self):
        html = ""
        html += "<div style='height:100%"
        for name,value in self._css.items():
            html += (";" + name + ":" + value)
        html += "'>"

        html += '''
        %(text)s</div>
        ''' % dict(text=self._text)

        return(html)

    @property
    def text(self):
        return(self._text)

    @text.setter
    def text(self,value):
        self._text = value

