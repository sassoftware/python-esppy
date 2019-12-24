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

''' ESP Window Publisher '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import copy
import csv
import datetime
import functools
import itertools
import os
import pandas as pd
import re
import requests
import six
import sys
import threading
import types
import weakref
import xml.etree.ElementTree as ET
from six.moves import urllib
from .utils import verify_window
from ..utils.authorization import Authorization
from ..base import ESPObject, attribute
from ..config import get_option
from ..exceptions import ESPError
from ..plotting import StreamingChart, StreamingImages, split_chart_params
from ..schema import Schema
from ..utils.keyword import dekeywordify
from ..utils import xml
from ..utils.notebook import scale_svg
from ..utils.rest import get_params
from ..utils.data import get_project_data, gen_name, get_server_info
from ..utils.events import get_events, get_dataframe, get_schema
from ..websocket import WebSocketClient


class Publisher(object):
    '''
    Create a publisher for the given window

    Attributes
    ----------
    blocksize : int
        Number of events to put into an event block
    dateformat : string
        Format for date fields
    format : string
        The data format of inputs: 'csv', 'xml', 'json', 'properties'
    is_active : bool
        Is the web socket currently active?
    opcode : string
        Opcode to use if an input event does not include one:
        'insert', 'upsert', 'delete'
    pause : int
        Number of milliseconds to pause between each injection of events
    rate : int
        Maximum number of events to inject per second
    separator : string
        The separator string to use between events in 'properties' format
    window_schema : Schema
        The schema of the window that was subscribed to
    window_url : string
        The publisher URL of the window

    Parameters
    ----------
    window : Window
        The window object to create the subscriber for
    blocksize : int, optional
        Number of events to put into an event block
    rate : int, optional
        Maximum number of events to inject per second
    pause : int, optional
        Number of milliseconds to pause between each injection of events
    dateformat : string, optional
        Format for date fields
    opcode : string, optional
        Opcode to use if an input event does not include one:
        'insert', 'upsert', 'delete'
    format : string, optional
        The data format of inputs: 'csv', 'xml', 'json', 'properties'
    separator : string
        The separator string to use between events in 'properties' format

    Examples
    --------
    Create the publisher instance using CSV and an event rate of 200
    events per millisecond.

    >>> pub = Publisher(window, rate=200)

    Send the CSV data.

    >>> pub.send('1,2,3')

    Close the connection.

    >>> pub.close()

    Returns
    -------
    :class:`Publisher`

    '''

    def __init__(self, window, blocksize=1, rate=0, pause=0,
                 dateformat='%Y%m%dT%H:%M:%S.%f', opcode='insert',
                 format='csv', separator=None):
        self.blocksize = int(blocksize)
        self.rate = int(rate)
        self.pause = int(pause)
        self.dateformat = dateformat
        self.opcode = opcode
        self.format = format
        self.separator = separator
        self.session = window.session
        self.window_fullname = window.fullname
        self.window_schema = get_schema(window, window)
        self.window_url = window.publisher_url

        if not verify_window(window):
            raise ESPError('There is no window at %s' % window.fullname)

        if get_option('debug.requests'):
            sys.stderr.write('WEBSOCKET %s\n' % self.url)

        headers = []

        auth = Authorization.getInstance(self.session)

        if auth.isEnabled:
            headers.append(("Authorization",auth.authorization));

        self._ws = WebSocketClient(self.url,headers=headers)
        self._ws.connect()

    @property
    def url(self):
        '''
        Return the URL of the subscriber

        Returns
        -------
        string

        '''
        url_params = get_params(**{'rate': self.rate,
                                   'blocksize': self.blocksize,
                                   'pause': self.pause,
                                   'format': self.format,
                                   'dateformat': self.dateformat,
                                   'opcode': self.opcode,
                                   'separator': self.separator})
        url_params = urllib.parse.urlencode(sorted(url_params.items()))
        return self.window_url + '?' + url_params.replace('+', '%20')

    @property
    def is_active(self):
        '''
        Is the web socket active?

        Returns
        -------
        bool

        '''
        return self._ws is not None

    def send(self, data):
        '''
        Send data to the web socket

        Examples
        --------
        Create the publisher instance using CSV and an event rate of 200
        events per millisecond.

        >>> pub = Publisher(window, rate=200)

        Send the CSV data.

        >>> pub.send('1,2,3')

        Parameters
        ----------
        data : string
            The data to send

        '''
        if self._ws is None:
            raise ValueError('The connection is closed')
        return self._ws.send(data)

    def close(self):
        '''
        Close the web socket connection

        Examples
        --------
        Create the publisher instance using CSV and an event rate of 200
        events per millisecond.

        >>> pub = Publisher(window, rate=200)

        Send the CSV data.

        >>> pub.send('1,2,3')

        Close the connection.

        >>> pub.close()

        '''
        if self._ws is not None:
            self._ws.close()
            self._ws = None
