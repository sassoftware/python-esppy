import ipywidgets as widgets
import logging

import esppy.espapi.tools as tools

class LogViewer(object):
    def __init__(self,connection,**kwargs):
        self._connection = connection
        self._options = tools.Options(**kwargs)
        self._connection.getLog().addDelegate(self)

        width = self._options.get("width","800px")
        height = self._options.get("height","800px")

        self._max = self._options.get("max",5);

        self._log = widgets.Textarea(value="",layout=widgets.Layout(width=width,height=height))

        self._messages = []

    def handleLog(self,connection,message):
        self._messages.insert(0,message)

        if self._max != None and len(self._messages) > self._max:
            diff = len(self._messages) - self._max

            for i in range(0,diff):
                self._messages.pop(self._max + i)

        s = ""

        for message in self._messages:
            s += message;
            s += "\n"

        self._log.value = s

    def display(self):
        return(self._log)
