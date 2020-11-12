#!/usr/bin/env python
# encoding: utf-8
#
# Copyright SAS Institute
#
#  Licensed under the Apache License, Version 2.0 (the License);
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

''' ESP Websocket Client '''

import threading
import websocket
import requests
import logging
import ssl

class WebSocketClient(object):
    '''
    Websocket Client

    Parameters
    ----------
    url : string
        The URL to connect to
    on_open : function, optional
        Function to call when the websocket is connected
    on_close : function, optional
        Function to call when the websocket is closed
    on_message : function, optional
        Function to call when a message is received
    on_data: function, optional
        Function to call when binary data is received
    on_error : function, optional
        Function to call when an error occurs
    **kwargs : keyword-arguments, optional
        Extra parameters to the ws4py socket client

    Returns
    -------
    :class:`WebSocketClient`

    '''

    def __init__(self, url, session, on_open=None, on_close=None, on_message=None, on_data=None,
                 on_error=None, **kwargs):
        self.callbacks = dict(on_open=on_open, on_close=on_close,
                              on_message=on_message, on_data=on_data, on_error=on_error)
        self._url = url
        self._session = session
        self._websocket = None

    def connect(self):
        if self._websocket != None:
            self._websocket.close()

        if self._session.verify:
            self._websocket = websocket.WebSocket()
        else:
            self._websocket = websocket.WebSocket(sslopt={"cert_reqs":ssl.CERT_NONE})

        self._websocket.connect(self._url)

        if self.callbacks.get("on_open"):
            self.callbacks["on_open"](self)

        self.start()

    def start(self):
        thread = threading.Thread(target = self.run)
        thread.daemon = True
        thread.start()

    def close(self):
        if self._websocket != None:
            self._websocket.close()
            self._websocket = None

    def send(self,data):
        if self._websocket != None:
            self._websocket.send(data)

    def runx(self):
        while True:
            opcode, frame = self._websocket.recv_data_frame()

            if opcode == websocket.ABNF.OPCODE_BINARY:
                if self.callbacks.get("on_data"):
                    self.callbacks["on_data"](self,frame.data)
            if opcode == websocket.ABNF.OPCODE_TEXT:
                if self.callbacks.get("on_message"):
                    data = frame.data.decode("utf-8")
                    self.callbacks["on_message"](self,data)

    def run(self):
        while True:
            try:
                opcode, frame = self._websocket.recv_data_frame()

                if opcode == websocket.ABNF.OPCODE_BINARY:
                    if self.callbacks.get("on_data"):
                        self.callbacks["on_data"](self,frame.data)
                if opcode == websocket.ABNF.OPCODE_TEXT:
                    if self.callbacks.get("on_message"):
                        data = frame.data.decode("utf-8")
                        self.callbacks["on_message"](self,data)
            except Exception as e:
                logging.info("got exception in ws")
                logging.info(str(e))
                break

    def unhandled_error(self, error):
        '''
        Handle an error from the server

        Parameters
        ----------
        error : Exception
           The exception associated with the error

        '''
        if self.callbacks.get('on_error'):
            self.callbacks['on_error'](self, error)

    def closed(self, code, reason=None):
        '''
        Handle the closing of a web socket connection

        Parameters
        ----------
        code : int
            The code the server was closed with
        reason : string, optional
            The reason the server connection was closed

        '''
        if self.callbacks.get('on_close'):
            self.callbacks['on_close'](self, code, reason=reason)
