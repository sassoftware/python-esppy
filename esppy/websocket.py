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

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import warnings
import six
try:
    import wsaccel
except ImportError:
    wsaccel = None
from six.moves import urllib
from ws4py.client.threadedclient import WebSocketClient as WS4PyWebSocketClient
from ws4py.messaging import BinaryMessage

try:
    if wsaccel is not None:
        wsaccel.patch_ws4py()
except Exception as exc:
    warnings.warn('Could not import wsaccel: %s' % exc, RuntimeWarning)


class WebSocketClient(WS4PyWebSocketClient):
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

    def __init__(self, url, on_open=None, on_close=None, on_message=None, on_data=None,
                 on_error=None, **kwargs):
        self.callbacks = dict(on_open=on_open, on_close=on_close,
                              on_message=on_message, on_data=on_data, on_error=on_error)
        WS4PyWebSocketClient.__init__(self, url, **kwargs)

    def received_message(self, message):
        '''
        Handle a message from the server

        Parameters
        ----------
        message : string
            The data from the server

        '''
        if isinstance(message,BinaryMessage):
            if self.callbacks.get('on_data'):
                return self.callbacks['on_data'](self, message.data)
        else:
            if self.callbacks.get('on_message'):
                return self.callbacks['on_message'](self, message.data.decode(message.encoding))

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

    def opened(self):
        ''' Handle the opening of a web socket connection '''
        if self.callbacks.get('on_open'):
            self.callbacks['on_open'](self)

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
