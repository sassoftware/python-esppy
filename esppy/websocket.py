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

import logging
import ssl
import os

from urllib.parse import urlparse

from esppy.ws4py import WebSocketClient as Ws4Py
from esppy.wsclient import WebSocketClient as WsClient

def createWebSocket(url,
                    session,
                    on_open=None,
                    on_close=None,
                    on_message=None,
                    on_data=None,
                    on_error=None,
                    headers=None,
                    ws4py = True,
                    **kwargs):

    o = urlparse(url)

    protocol = o[0]

    useWs4Py = ws4py

    if useWs4Py:
        if protocol == "wss":
            if os.getenv("https_proxy") != None:
                useWs4Py = False
        elif protocol == "ws":
            if os.getenv("http_proxy") != None:
                useWs4Py = False

    if useWs4Py:
        ws = Ws4Py(url,session,on_message=on_message,on_data=on_data,on_error=on_error,on_open=on_open,on_close=on_close,headers=headers)
    else:
        ws = WsClient(url,session,on_message=on_message,on_data=on_data,on_error=on_error,on_open=on_open,on_close=on_close,headers=headers)

    return(ws)

