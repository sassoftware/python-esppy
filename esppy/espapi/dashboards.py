import esppy.espapi.api as api
import logging
import uuid

class Dashboard(object):

    def __init__(self):
        self._id = str(uuid.uuid4()).replace('-', '_')
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
        div.dashboard
        {
            border:0;
            border:1px solid #d8d8d8;
            margin:auto;
            overflow:auto;
            padding:0;
            padding:5px;
        }

        table.dashboard
        {
            width:98%%;
            width:100%%;
            //border:1px solid red;
        }

        td.dashboard
        {
            background:white;
            padding:5px;
        }

        div.dashboardContainer
        {
            border:1px solid #d8d8d8;
            padding:0;
            overflow:auto;
        }

        </style>

        <script language="javascript">
        function
        size_%(id)s()
        {
            var div = document.getElementById("%(id)s_div");
            var d = div;

            while (d != null)
            {
                if (d.className != null)
                {
                    if (d.className.indexOf("output_html") != -1)
                    {
                        div.style.width = (d.offsetWidth - 10) + "px";
                        div.style.height = d.offsetHeight + "px";
                        break;
                    }
                }
                d = d.parentNode;
            }
        }

        </script>

        <div class='dashboard' id='%(id)s_div'>

        ''' % dict(id=self._id)

        for row in self._rows:
            html += "<table class='dashboard'>"
            html += "<tr>"
            for i in range(0,row.size):
                component = row.get(i)
                component.setHeight(row.height);
                html += "<td class='dashboard'"
                if i == 0 and row.size < maxcols:
                    html += " colspan='" + str(maxcols - row.size + 1) + "'"
                html += ">"
                #html += "<div class='dashboardContainer' style='height:" + str(row.height) + "px'>";
                html += "<div class='dashboardContainer'>";
                html += component.getHtml()
                html += "</div>";
                html += "</td>"
            html += "</tr>"
            html += "</table>"

        html += "</div>"
        html += "\n"

        html += '''
        <script language="javascript">
        size_%(id)s();
        </script>
        ''' % dict(id=self._id)

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

