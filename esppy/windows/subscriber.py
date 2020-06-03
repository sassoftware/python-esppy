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

''' ESP Window Subscriber '''

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
from ..utils.authorization import Authorization
from six.moves import urllib
from .utils import verify_window
from ..base import ESPObject, attribute
from ..config import get_option, CONCAT_OPTIONS
from ..exceptions import ESPError
from ..schema import Schema
from ..utils.keyword import dekeywordify
from ..utils import xml
from ..utils.notebook import scale_svg
from ..utils.rest import get_params
from ..utils.data import get_project_data, gen_name, get_server_info
from ..utils.events import get_events, get_dataframe, get_schema
from ..websocket import WebSocketClient


class Subscriber(object):
    '''
    Create a subscriber for the given window

    Attributes
    ----------
    callbacks : dict
        The dictionary of callback functions
    filter : string
        Functional filter to subset events
    format : string, optional
        The format of the received data: 'xml', 'json', 'csv', 'properties'
    interval : int
        Interval between event sends in milliseconds
    is_active : bool
        Is the web socket currently active?
    mode : string
        The mode of subscriber: 'updating' or 'streaming'
    pagesize : int
        The maximum number of events in a page
    separator : string, optional
        The separator to use between events in the 'properties' format
    schema : bool
        Should the schema be sent with the first event?
    sort : string
        Sort order for the events (updating mode only)
    window_schema : Schema
        The schema of the window being subscribed to
    window_url : string
        The subscriber URL of the window

    Parameters
    ----------
    window : Window
        The window object to create the subscriber for
    mode : string, optional
        The mode of subscriber: 'updating' or 'streaming'
    pagesize : int, optional
        The maximum number of events in a page
    filter : string, optional
        Functional filter to subset events
    sort : string, optional
        Sort order for the events (updating mode only)
    format : string, optional
        The format of the received data: 'xml', 'json', 'csv', 'properties'
    separator : string, optional
        The separator to use between events in the 'properties' format
    interval : int, optional
        Interval between event sends in milliseconds
    precision : int, optional
        The floating point precision
    schema : bool, optional
        Should the schema be sent with the first event?
    on_event : callable, optional
        The object to call for events. The argument to this object will
        be a DataFrame of the events that occurred.
    on_message : callable, optional
        The object to call for each websocket message
    on_error : callable, optional
        The object to call for each websocket error
    on_close : callable, optional
        The object to call when the websocket is opened
    on_open : callable, optional
        The object to call when the websocket is closed

    Examples
    --------
    Create event callback; ``event`` is a DataFrame

    >>> def on_event(event):
    ...     print(event.columns)

    Create the subscriber instance

    >>> sub = Subscriber(window, on_event=on_event)

    Start event processing (runs a thread in the background)

    >>> sub.start()

    Stop event processing (stops background thread)

    >>> sub.stop()

    Returns
    -------
    :class:`Subscriber`

    '''

    def __init__(self, window, mode='updating', pagesize=50, filter=None,
                 sort=None, format='xml', separator=None, interval=None,
                 schema=False, on_event=None, on_message=None, on_error=None,
                 on_close=None, on_open=None, precision=6):
        self._ws = None
        self.mode = mode
        self.pagesize = pagesize
        self.filter = filter
        self.sort = sort
        self.format = format
        self.separator = separator
        self.interval = interval
        self.precision = precision
        self.schema = schema
        self.server_info = get_server_info(window)
        self.session = window.session
        self.window_schema = get_schema(window, window)
        self.window_url = window.subscriber_url
        self.window_fullname = window.fullname
        self.callbacks = {k: v for k, v in dict(on_message=on_message,
                                                on_error=on_error,
                                                on_event=on_event,
                                                on_close=on_close,
                                                on_open=on_open).items()
                          if v is not None}

    @property
    def url(self):
        '''
        Return the URL of the subscriber

        Returns
        -------
        string

        '''
        url_params = get_params(**{'mode': self.mode,
                                   'pagesize': self.pagesize,
                                   'filter': self.filter,
                                   'format': self.format,
                                   'separator': self.separator,
                                   'sort': self.sort,
                                   'interval': self.interval,
                                   'precision': self.precision,
                                   'schema': True})
        url_params = '&'.join(['%s=%s' % (k, v) for k, v in sorted(url_params.items())])
        return self.window_url + '?' + url_params

    @property
    def is_active(self):
        '''
        Is the web socket active?

        Returns
        -------
        bool

        '''
        return self._ws is not None

    @property
    def mode(self):
        '''
        The mode of the subscriber: 'updating' or 'streaming'

        Returns
        -------
        string

        '''
        return self._mode

    @mode.setter
    def mode(self, value):
        ''' Set the mode of the subscriber '''
        self._mode = value
        if self._ws is not None and self._mode is not None:
            self._ws.send('<properties mode="%s"></properties>' % self._mode)

    @property
    def pagesize(self):
        '''
        The maximum number of events in a page

        Returns
        -------
        int

        '''
        return self._pagesize

    @pagesize.setter
    def pagesize(self, value):
        ''' Set the pagesize '''
        self._pagesize = value
        if self._ws is not None and self._pagesize is not None:
            self._ws.send('<properties pagesize="%s"></properties>' % self._pagesize)

    @property
    def sort(self):
        '''
        Sort order for the events (updating mode only)

        Returns
        -------
        string

        '''
        return self._sort

    @sort.setter
    def sort(self, value):
        ''' Set the sort order for the events '''
        self._sort = value
        if self._ws is not None and self._sort is not None:
            self._ws.send('<properties sort="%s"></properties>' % self._sort)

    @property
    def interval(self):
        '''
        Interval between event sends in milliseconds

        Returns
        -------
        int

        '''
        return self._interval

    @interval.setter
    def interval(self, value):
        ''' Set the event interval '''
        self._interval = value
        if self._ws is not None and self._interval is not None:
            self._ws.send('<properties interval="%s"></properties>' % self._interval)

    @property
    def filter(self):
        '''
        Functional filter to subset events

        Returns
        -------
        string

        '''
        return self._filter

    @filter.setter
    def filter(self, value):
        ''' Set the filter string '''
        self._filter = value
        if self._ws is not None and self._filter is not None:
            self._ws.send(('<properties><filter><![CDATA[%s]]>'
                           '</filter></properties>') % self._filter)

    @property
    def separator(self):
        '''
        Separator to use between events in the 'properties' format

        Returns
        -------
        string

        '''
        return self._separator

    @separator.setter
    def separator(self, value):
        ''' Set the separator string '''
        self._separator = value
        if self._ws is not None and self._separator is not None:
            self._ws.send('<properties separator="%s"></properties>' % self._separator)

    def start(self):
        '''
        Initialize the web socket and start it in its own thread

        Notes
        -----
        The thread created in the background will continue to run unless
        explicitly stopped using the :meth:`stop` method.

        Examples
        --------
        Create subscriber instance

        >>> sub = Subscriber(window, on_event=on_event)

        Start processing events

        >>> sub.start()

        '''
        if self._ws is not None:
            return

        if not verify_window(self.window_fullname, session=self.session):
            raise ESPError('There is no window at %s' % self.window_fullname)

        state = dict(status=None, schema=None, dataframe=None)

        def on_message(sock, message):
            # HTTP status messages
            if state['status'] is None and re.match(r'^\s*\w+\s*\:\s*\d+\s*\n', message):
                state['status'] = int(re.match(r'^\s*\w+\s*\:\s*(\d+)', message).group(1))
                if state['status'] >= 400:
                    raise ESPError('Subscriber message returned with status: %s' %
                                   state['status'])
                return

            if state['schema'] is None:
                if message.startswith('<schema>'):
                    state['schema'] = Schema.from_xml(message)

                elif re.match(r'^\s*{\s*["\']?schema["\']?\s*:', message):
                    state['schema'] = Schema.from_json(message)

                else:
                    raise ValueError('Unrecognized schema definition format: %s...' %
                                     message[:40])

                state['dataframe'] = get_dataframe(state['schema'])
                if self.schema:
                    return message
                return

            if 'on_message' in self.callbacks:
                self.callbacks['on_message'](sock, message)

            if 'on_event' in self.callbacks:
                try:
                    df = get_events(state['schema'], message,
                                    single=True, format=self.format,
                                    separator=self.separator,
                                    server_info=self.server_info)
                    #self.callbacks['on_event'](sock, pd.concat([state['dataframe'], df], **CONCAT_OPTIONS))
                    self.callbacks['on_event'](sock, pd.concat([state['dataframe'], df]))
                except:
                    import traceback
                    traceback.print_exc()
                    raise

        def on_error(sock, error):
            if 'on_error' in self.callbacks:
                self.callbacks['on_error'](sock, error)

        def on_open(sock):
            if 'on_open' in self.callbacks:
                self.callbacks['on_open'](sock)

        def on_close(sock, code, reason=None):
            if 'on_close' in self.callbacks:
                self.callbacks['on_close'](sock, code, reason=None)

        if get_option('debug.requests'):
            sys.stderr.write('WEBSOCKET %s\n' % self.url)

        headers = []

        auth = Authorization.getInstance(self.session)

        if auth.isEnabled:
            headers.append(("Authorization",auth.authorization));

        self._ws = WebSocketClient(self.url,
                                   on_message=on_message,
                                   on_error=on_error,
                                   on_open=on_open,
                                   on_close=on_close,
                                   headers=headers)

        self._ws.connect()

        ws_thread = threading.Thread(name='%s-%s' % (id(self), self.window_url),
                                     target=self._ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()

    def stop(self):
        '''
        Stop processing events and close the web socket

        Examples
        --------
        Create subscriber instance

        >>> sub = Subscriber(window, on_event=on_event)

        Start processing events

        >>> sub.start()

        Stop processing events

        >>> sub.stop()

        '''
        if self._ws is not None:
            self._ws.close()
            self._ws = None

    close = stop
