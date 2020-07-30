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

''' ESP Windows '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import copy
import warnings
import csv
import datetime
import functools
import inspect
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
from .features import (WindowFeature, SplitterExpressionFeature,
                       SplitterPluginFeature, FinalizedCallbackFeature,
                       ConnectorsFeature, SchemaFeature)
from .subscriber import Subscriber
from .publisher import Publisher
from .utils import listify, get_args, ensure_element, connectors_to_end
from .. import transformers
from ..base import ESPObject, attribute
from ..config import get_option, ESP_ROOT, CONCAT_OPTIONS
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

INDEX_TYPES = {
    'rbtree': 'pi_RBTREE',
    'hash': 'pi_HASH',
    'ln_hash': 'pi_LN_HASH',
    'cl_hash': 'pi_CL_HASH',
    'fw_hash': 'pi_FW_HASH',
    'empty': 'pi_EMPTY'
}


def get_window_class(name):
    '''
    Return the class for the specified window name

    Parameters
    ----------
    name : string
        The name of the window type

    Returns
    -------
    :class:`BaseWindow` subclass

    '''
    if not name.startswith('window-'):
        name = 'window-%s' % name
    return BaseWindow.window_classes[name]


def var_mapper(data, mapping):
    '''
    Map data column values to generated values

    Parameters
    ----------
    data : DataFrame
        The DataFrame to access data from
    mapping : dict
        A nested dictionary where the top-level keys are the column
        names.  The values are a dictionary where the keys are
        the variable to generate and the values are dictionaries
        with the old-value / new-value pairs.

    Returns
    -------
    dict of lists

    '''
    out = {}
    for col, attrs in mapping.items():
        for aname, attrmap in attrs.items():
            if isinstance(attrmap, dict):
                out[aname] = list(data[col].map(attrmap))
            else:
                out[aname] = list(attrmap(data[col]))
    return out


def param_iter(params):
    '''
    Iterate over all combinations of parameters

    Parameters
    ----------
    params : dict or list of dicts
        The sets of parameters

    Examples
    --------
    >>> for item in param_iter(dict(a=1, b=['x', 'y', 'z'], c=100)):
    ...      print(item)
    ...      if item['b'] == 'z':
    ...          break
    {'a': 1, b: 'x', c: 100}
    {'a': 1, b: 'y', c: 100}
    {'a': 1, b: 'z', c: 100}

    >>> for item in param_iter([dict(a=1, b='x', c=100),
    ...                         dict(a=1, b='y'),
    ...                         dict(a=2, c=200)]):
    ...    print(item)
    ...    if item['a'] == 2:
    ...        break
    {'a': 1, 'b': 'x', 'c': 100}
    {'a': 1, 'b': 'y'}
    {'a': 2, 'c': 200}

    Yields
    ------
    dict

    '''
    while True:
        if not params:
            yield {}
        elif isinstance(params, collections.abc.Mapping):
            keys, values = zip(*params.items())
            values = list(values)
            n_items = 1
            for val in values:
                if isinstance(val, (tuple, list, set)):
                    n_items = max(n_items, len(val))
            for i, val in enumerate(values):
                if isinstance(val, six.string_types) or \
                        not isinstance(val, collections.abc.Sequence):
                    values[i] = [val] * n_items
            for i in range(n_items):
                out = {}
                for key, value in zip(keys, values):
                    out[key] = value[i]
                yield out
        else:
            for value in params:
                yield copy.copy(value)


class Target(object):
    '''
    Window target

    Parameters
    ----------
    name : string
        The name of the target window
    role : string, optional
        The role of the connection

    Returns
    -------
    :class:`Target`

    '''
    _index = 0
    _index_lock = threading.Lock()

    def __init__(self, name, template=None, role=None, slot=None):
        self.base_name = name
        self.template = template
        self.role = role
        self.slot = slot
        self._index = self._get_new_index()

    @property
    def name(self):
        '''
        The true name of the window

        Returns
        -------
        string

        '''
        if not self.template:
            return self.base_name
        return '%s_%s' % (self.template.name, self.base_name)

    @classmethod
    def _get_new_index(cls):
        with cls._index_lock:
            cls._index += 1
            return cls._index

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return hash(other) == hash(self)

    def __str__(self):
        if self.role is None and self.slot is None:
            return '%s(%s)' % (type(self).__name__, repr(self.name))
        if self.role is not None and self.slot is not None:
            return '%s(%s, role=%s, slot=%s)' % (type(self).__name__, repr(self.name),
                                                 repr(self.role), repr(self.slot))
        if self.role is not None:
            return '%s(%s, role=%s)' % (type(self).__name__, repr(self.name), repr(self.role))
        return '%s(%s, slot=%s)' % (type(self).__name__, repr(self.name), repr(self.slot))

    def __repr__(self):
        return str(self)


class BaseWindow(ESPObject):
    '''
    Base Window class

    Parameters
    ----------
    name : string, optional
        The name of the window

    Attributes
    ----------
    contquery : string
        The name of the continuous query the window is associated with
    data : pandas.DataFrame
        The cached rows of event data
    description : string
        A description of the window
    event_transformers : list
        A list of event transformer definitions
    project : string
        The name of the project the window is associated with
    targets : set
        Set of target definitions
    url : string
        The URL of the window

    Notes
    -----
    All constructor parameters are also available as attributes.

    Returns
    -------
    :class:`Window`

    '''

    window_type = None
    window_classes = {}
    is_hidden = False

    properties = ['name']
    base_name = attribute('name', dtype='string')
    pubsub = attribute('pubsub', dtype='bool')

    _all_windows = []

    def __init__(self, name=None, **kwargs):
        schema = kwargs.pop('schema', None)
        copyvars = kwargs.pop('copyvars', None)
        ESPObject.__init__(self, attrs=kwargs)
        self.schema = Schema()
        if schema is not None:
            self.schema = schema
        self.base_name = name or gen_name(prefix='w_')
        self.template = None
        self.contquery = None
        self.project = None
        self.targets = set()
        self.parents = list()
        self.data = None
        self.description = None
        self.event_transformers = []
        self._subscriber = None
        self._initialize_features()
        self.copyvars = list(copyvars or [])

        self._register_to_all_windows()

    def _initialize_features(self):
        features = WindowFeature.__subclasses__()
        for item in inspect.getmro(type(self)):
            if item in features:
                item.__init__(self)

    def _register_to_all_windows(self):
        if self not in type(self)._all_windows:
            type(self)._all_windows.append(self)

    @classmethod
    def all_windows_name(cls):
        '''
        Return the name for all registered windows

        Returns
        -------
        List

        '''
        return [win.name for win in cls._all_windows]

    def _register_to_project(self, project_handle=None):
        pass

    def _cast_parameter(self, value, dtype=None, vartype=None):
        '''
        Cast parameter to the appropriate type

        Parameters
        ----------
        value : any
            The value of the window parameter
        dtype : string, optional
            The data type of the parameter
        vartype : string, optional
            The sub-data type of the parameter.  Typically used when
            `dtype` is a list parameter type.

        Returns
        -------
        any

        '''
        if dtype == 'varlist':
            if isinstance(value, six.string_types):
                value = re.split(r'\s*,\s*', value.strip())
        return value

    def _export_copyvars(self, out=None):
        ''' Get schema for copyvars '''

        def get_field_parts(field, map_name, dtypes):
            key = False
            dtype = 'double'
            if '*' in field:
                field = field.replace('*', '')
                key = True
            if ':' in field:
                field, dtype = field.split(':', 1)
            elif map_name in dtypes:
                dtype = dtypes[map_name]
            # Remove array ranges
            if re.search(r'\[[^\]]+\]$', field):
                field = re.sub(r'\[[^\]]+\]$', r'', field)
                dtype = 'array(%s)' % dtype
                dtype = dtype.replace('double', 'dbl')
                dtype = dtype.replace('int64', 'i64')
                dtype = dtype.replace('int32', 'i32')
            return field, dtype, key

        if out is None:
            out = self._schema.copy()

        online = getattr(self, 'online_models', [])
        offline = getattr(self, 'offline_models', [])

        maps = []
        input_map_types = getattr(type(self), 'input_map_types', {})
        output_map_types = getattr(type(self), 'output_map_types', {})

        if type(self).window_type in ['train', 'calculate']:
            for cls in type(self).__subclasses__():
                if cls.__name__ == self.algorithm:
                    input_map_types = input_map_types or getattr(cls, 'input_map_types', {})
                    output_map_types = output_map_types or getattr(cls, 'output_map_types', {})
                    break

        if type(self).window_type != 'train':
            maps.append([getattr(self, 'input_map', {}), input_map_types])
            for model in online:
                maps.append([getattr(model, 'input_map', {}), input_map_types])
            for model in offline:
                maps.append([getattr(model, 'input_map', {}), input_map_types])

        maps.append([getattr(self, 'output_map', {}), output_map_types])
        for model in online:
            maps.append([getattr(model, 'output_map', {}), output_map_types])
        for model in offline:
            maps.append([getattr(model, 'output_map', {}), output_map_types])

        copied_fields = {}
        for item, dtypes in maps:
            for map_name, value in item.items():
                #               if 'id' not in out:
                #                   out['id'] = 'int64', True
                for field in listify(value or []):
                    name, dtype, is_key = get_field_parts(field, map_name, dtypes)
                    for part in re.split(r'\s*,\s*', name.strip()):
                        if part in self.copyvars:
                            copied_fields[part] = dtype, is_key
                        elif part + '*' in self.copyvars:
                            copied_fields[part + '*'] = dtype, True

        for item in self.copyvars:
            if item in copied_fields:
                out[item] = copied_fields[item]

        return out

    @property
    def path(self):
        return (self.project + "/" + self.contquery + "/" + self.name)

    @property
    def schema(self):
        '''
        Return the schema for the window

        Returns
        -------
        :class:`Schema`

        '''
        if not self._schema.fields or self.copyvars:
            # Build a transient schema based on input and output maps
            out = self._schema.copy()
            out = self._export_copyvars(out)
            return out

        return self._schema

    @schema.setter
    def schema(self, value):
        '''
        Set a new window schema

        Parameters
        ----------
        value : Schema or string or tuple
            The value to set the schema to.  If it is a string, it should
            be a valid schema string for an entire window.  If it is a
            tuple, each component of the tuple should be a schema string
            for that column.

        '''
        if isinstance(value, six.string_types):
            self._schema.schema_string = value
        elif isinstance(value, Schema):
            self._schema = value
        elif isinstance(value, (list, tuple)):
            self._schema.schema_string = ' '.join(value)
        else:
            raise TypeError('Unknown type for schema: %s' % value)

    @property
    def schema_string(self):
        '''
        The window schema as a string representation

        Returns
        -------
        string

        '''
        return self.schema.schema_string

    @schema_string.setter
    def schema_string(self, value):
        '''
        Set the window schema from a schema string

        Parameters
        ----------
        value : string
            The schema string to use for the window schema

        '''
        self._schema.schema_string = value

    @property
    def subscriber_url(self):
        '''
        Return the subscriber URL for web sockets

        Returns
        -------
        string

        '''
        # return re.sub(r'^\w+(:.+?/%s/)' % ESP_ROOT, r'ws\1subscribers/', self.url)

        s = re.findall("^\w+:", self.url)
        wsproto = (s[0] == "https:") and "wss" or "ws"
        value = re.sub(r'^\w+(:.+?/%s/)' % ESP_ROOT, r'%s\1subscribers/' % wsproto, self.url)
        return (value)

    @property
    def publisher_url(self):
        '''
        Return the publisher URL for web sockets

        Returns
        -------
        string

        '''
        # return re.sub(r'^\w+(:.+?/%s/)' % ESP_ROOT, r'ws\1publishers/', self.url)

        s = re.findall("^\w+:", self.url)
        wsproto = (s[0] == "https:") and "wss" or "ws"
        value = re.sub(r'^\w+(:.+?/%s/)' % ESP_ROOT, r'%s\1publishers/' % wsproto, self.url)
        return (value)

    def add_event_transformer(self, method, *args, **kwargs):
        '''
        Add an transformer to apply for each event

        Parameters
        ----------
        method : string or callable
            If a string, it is the name of a :class:`pandas.DataFrame`
            method to execute.  This method will be executed on the
            returned event DataFrame before the result is appended to
            the Window's DataFrame.
            If a callable, the first argument must be the event DataFrame,
            and the new DataFrame must be returned.
        *args : one-or-more arguments, optional
            Arguments to pass to the transformer
        **kwargs : keyword arguments, optional
            Keyword arguments to pass to the transformer

        Examples
        --------
        Add DataFrame method transformer.

        >>> win.add_event_transformer('clip', upper=20)

        Add an arbitrary function transformer.

        >>> def new_col(data):
        ...     data['new_col'] = (data['a'] + data['b'])**2
        ...     return data
        ...
        >>> win.add_event_transfromer(new_col)

        '''
        self.event_transformers.append((method, tuple(args), dict(kwargs)))
        if self.data is not None:
            if isinstance(method, six.string_types):
                if method in ['rgb2bgr', 'bgr2rgb', 'bytes2image']:
                    self.data = getattr(transformers, method)(data, *tuple(args), **dict(kwargs))
                else:
                    self.data = getattr(self.data, method)(*tuple(args), **dict(kwargs))
            else:
                self.data = method(self.data, *tuple(args), **dict(kwargs))

    def create_subscriber(self, mode='updating', pagesize=50, filter=None,
                          sort=None, format='xml', separator=None,
                          interval=None, schema=False,
                          on_event=None, on_message=None, on_error=None,
                          on_close=None, on_open=None, precision=6):
        '''
        Create a new websocket subscriber for the window

        Parameters
        ----------
        mode : string, optional
            The type of subscriber: 'updating' or 'streaming'
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
        Define a callback function for events

        >>> def on_event(event):
        ...     print(event.columns)

        Create a window subscrber

        >>> sub = win.create_subscriber(on_event=on_event)

        Start processing events (runs a background thread)

        >>> sub.start()

        Stop processing events (stops background thread)

        >>> sub.stop()

        Returns
        -------
        :class:`Subscriber`

        '''
        return Subscriber(self, mode=mode, pagesize=pagesize, filter=filter,
                          sort=sort, format=format, separator=separator,
                          interval=interval, schema=schema,
                          on_event=on_event, on_message=on_message,
                          on_error=on_error, on_close=on_close, on_open=on_open,
                          precision=precision)

    def create_publisher(self, blocksize=1, rate=0, pause=0,
                         dateformat='%Y%m%dT%H:%M:%S.%f', opcode='insert',
                         format='csv', separator=None):
        '''
        Create a publisher for the given window

        Parameters
        ----------
        blocksize : int, optional
            Number of events to put into an event block
        rate : int, optional
            Maximum number of events to inject per second
        pause : int, optional
            Number of milliseconds to pause between each injection of events
        dateformat : string, optional
            Format for date fields
        format : string, optional
            The data format of inputs: 'csv', 'xml', 'json', 'properties'
        separator : string, optional
            The separator to use between events in the 'properties' format
        opcode : string, optional
            Opcode to use if an input event does not include one:
            'insert', 'upsert', 'delete'

        Examples
        --------
        Create a publisher instance

        >>> pub = win.create_publisher(pause=200)

        Publish CSV data to subscriber

        >>> pub.send('1,2,3')

        Close the connection

        >>> pub.close()

        Returns
        -------
        :class:`Publisher`

        '''
        return Publisher(self, blocksize=blocksize, rate=rate, pause=pause,
                         dateformat=dateformat, format=format,
                         separator=separator)

    def publish_events(self, data, blocksize=1, rate=0, pause=0,
                       dateformat='%Y%m%dT%H:%M:%S.%f', opcode='insert',
                       format=None, separator=None):
        '''
        Publish events to the window

        Parameters
        ----------
        data : string or file-like or DataFrame
            The event data to publish
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
        Publish CSV data to window with at one event per 200 milliseconds.

        >>> with open('data.csv') as csvfile:
        ...     win.publish_events(csvfile, pause=200)

        Publish DataFrame data to window, only columns that match the
        window's schema will be pubished.

        >>> win.publish_events(dframe, pause=200)

        '''
        data_file = None

        if isinstance(data, pd.DataFrame):
            index = False
            if hasattr(self, 'schema') and self.schema.fields:
                data = data.reset_index().loc[:, list(self.schema.fields.keys())]
            else:
                if len(data.index.names) > 1 or data.index.names[0] is not None:
                    index = True
            data = data.to_csv(header=False, index=index, na_rep='')
            format = 'csv'

        elif isinstance(data, six.string_types):
            try:
                if os.path.isfile(data):
                    data_file = open(data, 'r')
                    data = data_file.read()
            except:
                pass
        elif hasattr(data, 'read'):
            data = data.read()

        else:
            raise TypeError('Unknown data type: %s' % data)

        if format is None:
            if data.startswith('<'):
                format = 'xml'
            elif data.startswith('{'):
                format = 'json'
            elif re.match(r'\w+=', data):
                format = 'properties'
            else:
                format = 'csv'

        try:
            pub = self.create_publisher(blocksize=blocksize, rate=rate, pause=pause,
                                        dateformat=dateformat, opcode=opcode,
                                        format=format, separator=separator)
            pub.send(data)
            pub.close()

        finally:
            if data_file is not None:
                data_file.close()

    def _get_event_horizon(self, value):
        '''
        Set a timespan or deadline for event collection

        Parameters
        ----------
        value : datetime or date or time or timedelta or int
            The deadline or timespan for event collection.  If a datetime,
            date, or time is given, streaming stops at the next event that
            occurs after that value.  If a timedelta is given, streaming stops
            at the first event after the timespan given in the timedelta
            passes.  An integer value, specifies a maximum number of events
            to retrieve.

        '''
        if value is None:
            return

        if not isinstance(value, (list, tuple)):
            value = [value]

        out = []
        for item in value:
            if item is None:
                pass
            elif isinstance(item, (datetime.datetime, six.string_types)):
                out.append(item)
            elif isinstance(item, datetime.timedelta):
                out.append(datetime.datetime.now() + item)
            elif isinstance(item, datetime.date):
                out.append(datetime.datetime(item.year, item.month, item.day))
            elif isinstance(item, datetime.time):
                today = datetime.datetime.combine(datetime.date.today(), datetime.time())
                tomorrow = datetime.datetime.combine(datetime.date.today() +
                                                     datetime.timedelta(days=1), datetime.time())
                if today > datetime.datetime.now():
                    out.append(tomorrow)
                else:
                    out.append(today)
            elif isinstance(item, (int, float)):
                out.append(int(item))
            else:
                raise TypeError('Unknown type for event horizon: %s' % value)

        return out

    #   def _get_sort_order(self, value):
    #      if value:
    #          return value
    #      if self.schema.fields:
    #           for field in self.schema.fields.values():
    #               if field.key:
    #                   return '%s:descending' % field.name

    def subscribe(self, mode='streaming', pagesize=50, filter=None,
                  sort=None, interval=None, limit=None, horizon=None, reset=True,
                  precision=6):
        '''
        Subscribe to events

        When invoked, this method creates a :class:`Subscriber` object to collect
        events for the window.  The data in these events are stored in
        the :attr:`data` attribute.

        To stop event processing, use the :meth:`unsubscribe` method.

        Parameters
        ----------
        mode : string, optional
            The mode of subscriber: 'updating' or 'streaming'
        pagesize : int, optional
            The maximum number of events in a page
        filter : string, optional
            Functional filter to subset events
        sort : string, optional
            Sort order for the events (updating mode only)
        interval : int, optional
            Interval between event sends in milliseconds
        precision: int, optional
            The floating point precision
        limit : int, optional
            The maximum number of rows of data to keep in the internal
            DataFrame object.
        horizon : int or datetime.datetime or string, optional
            Specifies a condition that stops the subscriber.
            If an int, the subscriber stops after than many events.
            If a datetime.datetime, the subscriber stops after the specified
            date and time.  If a string, the string is an expression
            applied to the event using the :meth:`DataFrame.query`
            method.  If that query returns any number of rows, the
            subscriber is stopped.
        reset : bool, optional
            If True, the internal data is reset on subsequent calls
            to the :meth:`subscribe` method.

        See Also
        --------
        :meth:`unsubscribe`
        :class:`Subscriber`

        '''
        self.unsubscribe()

        if self.data is None or reset:
            self.data = get_dataframe(self)

        state = dict(total=0, limit=limit or sys.maxsize,
                     horizon=self._get_event_horizon(horizon))

        if self.data is None:
            self.data = get_dataframe(self)

        def on_event(ws, event):
            if state['horizon'] is not None:
                for item in state['horizon']:
                    if isinstance(item, int):
                        if state['total'] >= item:
                            self.unsubscribe()
                            return
                    if isinstance(item, datetime.datetime):
                        if item < datetime.datetime.now():
                            self.unsubscribe()
                            return
                    if isinstance(item, six.string_types):
                        if len(event.query(item)):
                            self.unsubscribe()
                            return
            for method, args, kwargs in self.event_transformers:
                if isinstance(method, six.string_types):
                    event = getattr(event, method)(*args, **kwargs)
                else:
                    args = [event] + list(args)
                    event = method(*args, **kwargs)
            columns = self.data.columns
            self.data = pd.concat([self.data, event],
                                  **CONCAT_OPTIONS)[-state['limit']:][columns]
            state['total'] += len(event)

        self._subscriber = self.create_subscriber(mode=mode, pagesize=pagesize,
                                                  filter=filter, sort=sort,
                                                  interval=interval, format='xml',
                                                  on_event=on_event, precision=precision)
        self._subscriber.start()

    def unsubscribe(self):
        '''
        Stop event processing

        See Also
        --------
        :meth:`subscribe`

        '''
        if self._subscriber is not None:
            self._subscriber.close()
        self._subscriber = None

    def apply_transformers(self, data):
        '''
        Apply current set of data transformers to given data

        Parameters
        ----------
        data : DataFrame
            The data set to apply transformers to

        Examples
        --------
        >>> win.add_event_transformer('clip', upper=10, lower=0)

        >>> dframe = pd.DataFrame([[-1, 2, 3], [10, 20, 30]])
        >>> dframe
            0   1   2
        0  -1   2   3
        1  10  20  30

        >>> win.apply_transformers(dframe)
            0   1   2
        0   0   2   3
        1  10  10  10

        See Also
        --------
        :meth:`add_event_transformer`

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        for method, args, kwargs in self.event_transformers:
            if isinstance(method, six.string_types):
                data = getattr(data, method)(*args, **kwargs)
            else:
                args = [data] + list(args)
                data = method(*args, **kwargs)
        return data

    @property
    def template(self):
        '''
        The name of the template the window is associated with

        Returns
        -------
        string

        '''
        return self._template

    @template.setter
    def template(self, value):
        '''
        Set the template value

        Parameters
        ----------
        value : Template
            The name of the template.  If a :class:`Template`
            object is used, the name will be extracted from it.

        '''
        self._template = value
        if hasattr(value, 'contquery'):
            self.project = value.contquery
        if hasattr(value, 'project'):
            self.project = value.project

    @property
    def contquery(self):
        '''
        The name of the continuous query the window is associated with

        Returns
        -------
        string

        '''
        return self._contquery

    @contquery.setter
    def contquery(self, value):
        '''
        Set the continous_query value

        Parameters
        ----------
        value : string or ContinousQuery
            The name of the continuous query.  If a :class:`ContinousQuery`
            object is used, the name will be extracted from it.

        '''
        self._contquery = getattr(value, 'name', value)
        if hasattr(value, 'project'):
            self.project = value.project

    @property
    def project(self):
        '''
        The name of the project the window is associated with

        Returns
        -------
        string

        '''
        return self._project

    @project.setter
    def project(self, value):
        '''
        Set the project value

        Parameters
        ----------
        value : string or Project
            The name of the project.  If a :class:`Project`
            object is used, the name will be extracted from it.

        '''
        self._project = getattr(value, 'name', value)

    def create_event_generator(self, data=None, name=None, overwrite=False):
        '''
        Create an event generator

        Parameters
        ----------
        data : CSV or URL or DataFrame, optional
            The events to inject
        name : string, optional
            The name of the event generator.  If no name is given,
            a name will be generated.
        overwrite : bool, optional
            Should an existing event generator with the same name
            be overwritten?

        Examples
        --------
        Create the event generator

        >>> egen = win.create_event_generator('file:///path/to/file.csv')

        Insert events at a rate of one every 200 milliseconds

        >>> egen.start(pause=200)

        See Also
        --------
        :class:`EventGenerator`

        Returns
        -------
        :class:`EventGenerator`

        '''
        from ..evtgen import EventGenerator
        gen = EventGenerator(name=name)
        gen.session = self.session
        gen.publish_target = self
        if data is not None:
            gen.event_data = data
            gen.save(overwrite=overwrite)
        return gen

    def add_schema_field(self, name, type, key=False):
        '''
        Add a schema field

        Parameters
        ----------
        name : string
            Name of the field
        type : string
            Data type of the field
        key : bool, optional
            Indicates whether or not the field is a key field

        '''
        return self._schema.add_field(name, type, key)

    def delete_schema_field(self, *fields):
        '''
        Delete schema fields

        Parameters
        ----------
        fields : string
            Name of the fields
        '''
        for name in fields:
            if name in self.copyvars:
                self.copyvars.remove(name)
            else:
                try:
                    del self._schema.fields[name]
                except KeyError:
                    raise Warning('Unknown schema field name: %s' % name)

    delete_schema_fields = delete_schema_field

    def set_key(self, *fields, propagation=False):
        '''
        Set schema fields as key

        Parameters
        ----------
        fields : one-or-more schema fields
            The schema fields to be set as key
        propagation : indicate whether the change propagate to connected windows
            Default to be False
        '''
        for name in fields:
            if name in self.copyvars:
                to_copy = [each if each != name else each + '*' for each in self.copyvars]
                self.copyvars = to_copy
            else:
                try:
                    self._schema.fields[name].key = True
                except KeyError:
                    raise KeyError('Unknown schema field name: %s' % name)

        def _propagation_key(win):
            for target in win.targets:
                found = False
                if target.role and target.role != 'data':
                    continue

                for target_win in type(self)._all_windows:
                    if target_win.name == target.name and win in target_win.parents:
                        found = True
                        break

                if found:
                    if SchemaFeature in inspect.getmro(type(target_win)):
                        propagation_fields = [each for each in fields if each in target_win.schema]
                        if propagation_fields:
                            target_win.set_key(*propagation_fields, propagation=propagation)
                    else:
                        _propagation_key(target_win)

        if propagation:
            _propagation_key(self)

    def _get_target_window(self, window):
        '''
        Helper method to get target window object

        Parameters
        ----------
        window : Window or window names
            the window to search for

        Returns
        -------
        :class:`Window`
        '''
        item = window
        if isinstance(window, (BaseWindow, Window)):
            pass

        elif isinstance(window, six.string_types):
            try:
                item = self._all_windows[type(self).all_windows_name().index(window)]
            except ValueError:
                raise ValueError("No such window found")

        else:
            try:
                item = window.windows[window._input_windows[0]]
            except TypeError:
                raise TypeError("Wrong input type, only Template object and Windows are supported.")
        return item

    def add_targets(self, *windows, auto_schema=False, **kwargs):
        '''
        Add windows as targets

        Parameters
        ----------
        windows : one-or-more Windows or window names
            The windows to use as targets
        auto_schema : boolean, optional
            Indicates whether to automatically propagate the schema for target window or not,
            default to be False
        role : string, optional
            The role of the connection
        slot : string, optional
            Indicates the slot number to use from the splitting
            function for the window.

        Examples
        --------
        >>> train_w.add_target(score_w, role='model')
        >>> win.add_target(train_w, role='data').add_target(score_w, role='data')

        See Also
        --------
        :meth:`delete_target`
        :meth:`delete_targets`

        Returns
        -------
        ``self``

        '''

        def _flatten_map(p):
            l = list(p.values()) if p else []
            flat_list = []
            _ = [flat_list.extend(item) if isinstance(item, list)
                 else flat_list.append(re.sub(r'\[[^\]]+\]$', r'', item)) for item in l if item]
            return flat_list

        def _propagate_schema(*source_win, target_win):

            if SchemaFeature in inspect.getmro(type(target_win)):
                if any(SchemaFeature in inspect.getmro(type(each)) for each in source_win):
                    source_schema = ','.join(set([each_win.schema_string.strip() for each_win in source_win]))
                    check_to_copy = [x.strip().split(':', 1)[0] for x in re.split(r'\s*,\s*|\s+', source_schema)]

                    online = getattr(target_win, 'online_models', [])
                    offline = getattr(target_win, 'offline_models', [])
                    models = online + offline + [target_win]

                    to_copy = list()
                    copy_attrs = ['input_map', 'output_map']
                    for model in models:
                        for each_attr in copy_attrs:
                            to_copy.extend([item for item in _flatten_map(getattr(model, each_attr, None))
                                            if item not in check_to_copy and item + '*' not in check_to_copy])

                    target_win.copyvars = to_copy
                    target_win.schema = source_schema

                else:
                    parent_source_win = functools.reduce(lambda x, y: x + y, list(map(lambda x: x.parents,
                                                                                      source_win)), [])
                    _propagate_schema(*parent_source_win, target_win=target_win)

        def _auto_schema(source_win, target_win):
            # propagate schema from source_win to target_win
            for target in source_win.targets:
                if target.name == target_win.name:
                    if target.role and target.role != 'data':
                        warnings.warn('auto_schema feature for current role is disabled.', Warning)
                    else:
                        _propagate_schema(source_win, target_win=target_win)
                    break

            # propagate schema for connected windows in the same Template
            if target_win.template:
                for win in target_win.targets:
                    _auto_schema(target_win, target_win.template.windows[win.base_name])

        self.delete_targets(*windows)
        for each in windows:
            item = self._get_target_window(each)
            self.targets.add(Target(name=getattr(item, 'base_name', item).split('.')[-1],
                                    template=getattr(item, 'template', None),
                                    role=kwargs.get('role', None), slot=kwargs.get('slot', None)))

            item.parents.append(self)

            if auto_schema:
                _auto_schema(self, target_win=item)

    add_target = add_targets

    def delete_targets(self, *windows):
        '''
        Remove windows as targets

        Parameters
        ----------
        windows : one-or-more Windows or window names
            The windows to remove as targets

        Examples
        --------
        >>> win.delete_targets(train_w, score_w)

        See Also
        --------
        :meth:`add_target`
        :meth:`add_targets`

        Returns
        -------
        ``self``

        '''
        # windows = set([getattr(x, 'name', x).split('.')[-1] if isinstance(x, (six.string_types, BaseWindow))
        #                else x.windows[x._input_windows[0]].name for x in windows])
        windows = [self._get_target_window(each) for each in windows]
        windows_name = [each.name for each in windows]
        for target in set(self.targets):
            try:
                match_index = windows_name.index(target.name)
                windows[match_index].parents.remove(self)
                self.targets.remove(target)

            except ValueError:
                pass

    delete_target = delete_targets

    @property
    def name(self):
        '''
        The true name of the window

        Returns
        -------
        string

        '''
        if not self.template:
            return self.base_name
        return '%s_%s' % (self.template.name, self.base_name)

    @property
    def fullname(self):
        '''
        The fully-qualified name of the window (e.g., project.contquery.window)

        Returns
        -------
        string

        '''
        return '%s.%s.%s' % (self.project, self.contquery, self.name)

    @property
    def url(self):
        '''
        URL of the window

        Returns
        -------
        string

        '''
        self._verify_project()
        return urllib.parse.urljoin(self.base_url, '%s/%s/%s/' %
                                    (self.project, self.contquery, self.name))

    def _server_info(self):
        ''' Retrieve the server information '''
        return get_server_info(self)

    def _verify_project(self):
        '''
        Verify that the project and continuous query attributes are set

        '''
        if not self.contquery:
            raise ValueError('This window is not associated with a continuous query.')
        if not self.project:
            raise ValueError('This window is not associated with a project.')

    def copy(self, deep=False):
        '''
        Return a copy of the object

        Parameters
        ----------
        deep : bool, optional
            Should sub-objects be copied as well?

        Examples
        --------
        Shallow copy

        >>> new_win = win.copy()

        Deep copy

        >>> new_win = win.copy(deep=True)

        Returns
        -------
        :class:`Window`

        '''
        out = type(self)()

        out.session = self.session
        out.template = self.template
        out.contquery = self.contquery
        out.project = self.project
        out.targets = set(self.targets)
        out.description = self.description

        for key, value in self._get_attributes(use_xml_values=False).items():
            if key not in self.properties:
                setattr(out, key, value)

        if self.data is not None:
            out.data = self.data.copy(deep=True)

        out.event_transformers = list(self.event_transformers)

        features = WindowFeature.__subclasses__()
        for item in inspect.getmro(type(self)):
            if item in features:
                item._copy_feature(self, out, deep=deep)

        return out

    def __copy__(self):
        return self.copy(deep=False)

    def __deepcopy__(self, memo):
        return self.copy(deep=True)

    @classmethod
    def from_xml(cls, data, contquery=None, project=None, session=None):
        '''
        Construct window from XML definition

        Parameters
        ----------
        data : xml-string or ElementTree.Element
            XML window definition
        contquery : string or ContinuousQuery, optional
            The continuous query to associate the window with
        project : string or Project, optional
            The project to associate the window with
        session : requests.Session, optional
            The session object

        Examples
        --------
        Create a window from XML

        >>> win = Window.from_xml('<window-source name="w_data" insert-only="true">'
        ...                       '  <schema>'
        ...                       '    <fields>'
        ...                       '      <field name="id" type="int64" key="true" />'
        ...                       '      <field name="time" type="double" />'
        ...                       '    </fields>'
        ...                       '  </schema>'
        ...                       '</window-source>')
        >>> win
        SourceWindow(name='w_data', contquery=None, project=None)

        Display the schema

        >>> win.schema
        id*:int64,time:double

        Returns
        -------
        :class:`Window` subclass

        '''
        if isinstance(data, six.string_types):
            data = xml.from_xml(data)

        if cls in [BaseWindow, Window]:
            cls = cls.window_classes[data.tag]
            return cls.from_xml(data, contquery=contquery,
                                project=project, session=session)

        out = cls()
        out.session = session
        out.project = getattr(project, 'name', project)
        out.contquery = getattr(contquery, 'name', contquery)

        out._set_attributes(data.attrib)

        for item in data.findall('./description'):
            out.description = item.text

        for item in data.findall('./schema'):
            out.schema = Schema.from_xml(item, session=session)
        for item in data.findall('./schema-string'):
            out.schema = Schema.from_schema_string(item, session=session)

        for item in list(inspect.getmro(cls))[1:]:
            if issubclass(item, WindowFeature):
                item._feature_from_element(out, data)

        return out

    from_element = from_xml

    def to_element(self, query=None):
        '''
        Export window definition to ElementTree.Element

        Parameters
        ----------
        query : ContinuousQuery, optional
            The parent query object of the window

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        attrs = xml.get_attrs(self, exclude='project')
        attrs.pop('contquery', None)

        out = xml.new_elem('window-%s' % self.window_type, attrs)

        if self.description:
            xml.add_elem(out, 'description', text_content=self.description)

        features = WindowFeature.__subclasses__()
        for item in inspect.getmro(type(self)):
            if item in features:
                elem = item._feature_to_element(self)
                if elem is None:
                    pass
                elif isinstance(elem, list):
                    for subelem in elem:
                        xml.add_elem(out, subelem)
                else:
                    xml.add_elem(out, elem)

        connectors_to_end(out)

        return out

    def to_xml(self, pretty=False, query=None):
        '''
        Export window definition to XML

        Parameters
        ----------
        pretty : bool, optional
            Should the output include whitespace for readability?
        query : ContinuousQuery, optional
            The parent query object of this window

        Examples
        --------
        >>> win.to_xml(pretty=True)
        <window-source insert-only="true" name="w_data">
          <schema>
            <fields>
              <field key="true" name="id" type="int64" />
              <field key="false" name="time" type="double" />
            </fields>
          </schema>
        </window-source>

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(query=query), pretty=pretty)

    def save_xml(self, dest, mode='w', pretty=True, **kwargs):
        '''
        Save the window XML to a file

        Parameters
        ----------
        dest : string or file-like
            The destination of the XML content
        mode : string, optional
            The write mode for the output file (only used if `dest` is a string)
        pretty : boolean, optional
            Should the XML include whitespace for readability?

        '''
        if isinstance(dest, six.string_types):
            with open(dest, mode=mode, **kwargs) as output:
                output.write(self.to_xml(pretty=pretty))
        else:
            dest.write(self.to_xml(pretty=pretty))

    def to_graph(self, graph=None, schema=True):
        '''
        Export window definition to graphviz.Digraph

        Parameters
        ----------
        graph : graphviz.Graph, optional
            The parent graph to add to
        schema : bool, optional
            Should window schemas be included?

        Returns
        -------
        :class:`graphviz.Digraph`

        '''
        try:
            import graphviz as gv
        except ImportError:
            raise ImportError('The graphviz module is required for exporting to graphs.')

        if graph is None:
            graph = gv.Digraph(format='svg')
            graph.attr('node', shape='rect')
            graph.attr('graph', rankdir='LR')

        if schema and hasattr(self, 'schema'):
            key_fmt = '<tr><td>&#x1f511;<font color="#c49810">%s</font></td></tr>'
            col_fmt = '<tr><td><font color="#808080">%s</font></td></tr>'
            out = self.schema.to_element()
            fields = [x.attrib for x in out.findall('.//field')]
            label = []
            label.append('<<table border="0" cellspacing="0" cellpadding="0">')
            label.append('<tr><td>%s</td></tr>' % self.name)
            label.append('<tr><td> </td></tr>')
            n_fields = 0
            for field in fields:
                fmt = (field.get('key', 'false') == 'true') and key_fmt or col_fmt
                if get_option('display.show_field_type'):
                    label.append(fmt % ' : '.join((field['name'], field['type'])))
                else:
                    label.append(fmt % field['name'])
                n_fields += 1
                if n_fields >= get_option('display.max_fields'):
                    if len(fields) - n_fields > 1:
                        label.append(('<tr><td><font color="#808080">[%s more]'
                                      '</font></td></tr>') % (len(fields) - n_fields))
                        break
            label.append('</table>>')
            label = ''.join(label)
        else:
            label = self.name

        graph.node(self.fullname, label,
                   style='filled,bold', color='#58a0d3',
                   fillcolor='#c8f0ff', fontcolor='black',
                   margin='.25,.17', fontname='helvetica')

        return graph

    def _repr_svg_(self):
        try:
            return scale_svg(self.to_graph()._repr_svg_())
        except ImportError:
            raise AttributeError('_repr_svg_')

    def __getitem__(self, key):
        return self.data[key]

    def __getattr__(self, name):
        if name in self._schema.fields or name in dir(pd.DataFrame):
            return getattr(self.data, name)
        raise AttributeError(name)

    def __setitem__(self, name, value):
        raise RuntimeError('Events are read-only')

    def __delitem__(self, name):
        raise RuntimeError('Events are read-only')

    def __len__(self):
        if self.data is None:
            return 0
        return len(self.data)

    def __iter__(self):
        if self.data is None:
            return iter([])
        return iter(self.data)

    def enable_tracing(self):
        ''' Enable console tracing for the window '''
        self._verify_project()
        self._put(urllib.parse.urljoin(self.base_url,
                                       'windows/%s/%s/%s/state' % (self.project,
                                                                   self.contquery,
                                                                   self.name)),
                  params=get_params(value='tracingOn'))

    def disable_tracing(self):
        ''' Disable console tracing for the window '''
        self._verify_project()
        self._put(urllib.parse.urljoin(self.base_url,
                                       'windows/%s/%s/%s/state' % (self.project,
                                                                   self.contquery,
                                                                   self.name)),
                  params=get_params(value='tracingOff'))

    def get_events(self, filter=None, sort_by=None, limit=None):
        '''
        Retrieve events from the window

        Parameters
        ----------
        filter : string or list-of-strings, optional
            Functional filter indicating the events to return
        sort_by : string, optional
            Field to sort results on.  It should be in the form
            ``field:order``, where ``field`` is the field name and
            ``order`` is either ``ascending`` or ``descending``.
        limit : int, optional
            Maximum number of events to return

        See Also
        --------
        :meth:`subscribe`
        :meth:`create_subscriber`

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        self._verify_project()
        out = get_events(self, self._get(urllib.parse.urljoin(self.base_url,
                                                              'events/%s/%s/%s/' % (self.project,
                                                                                    self.contquery,
                                                                                    self.name)),
                                         params=get_params(filter=filter,
                                                           sort_by=sort_by,
                                                           limit=limit)),
                         server_info=get_server_info(self))
        if out:
            return list(out.values())[0]

        out = pd.DataFrame(columns=self.schema.fields.keys())
        index = [x.name for x in self.schema.fields.values() if x.key]
        if index:
            out = out.set_index(index)
        return out

    def get_pattern_events(self, sort_by=None, limit=None):
        '''
        Retrieve events residing in open patterns in the window

        Parameters
        ----------
        sort_by : string, optional
            Field to sort results on.  It should be in the form
            ``field:order``, where ``field`` is the field name and
            ``order`` is either ``ascending`` or ``descending``.
        limit : int, optional
            Maximum number of events to return

        Returns
        -------
        :class:`pandas.DataFrame`

        '''
        self._verify_project()
        out = get_events(self,
                         self._get(urllib.parse.urljoin(self.base_url,
                                                        'patternEvents/%s/%s/%s/' %
                                                        (self.project,
                                                         self.contquery,
                                                         self.name)),
                                   params=get_params(sort_by=sort_by, limit=limit)),
                         server_info=get_server_info(self))
        if out:
            return list(out.values())[0]

        out = pd.DataFrame(columns=self.schema.fields.keys())
        index = [x.name for x in self.schema.fields.values() if x.key]
        if index:
            out = out.set_index(index)
        return out

    def __str__(self):
        if self.data is None:
            return '%s(name=%s, contquery=%s, project=%s)' % \
                   (type(self).__name__, repr(self.name),
                    repr(self.contquery), repr(self.project))
        return str(self.data)

    def __repr__(self):
        return str(self)

    def to_data_callback(self, x=None, y=None, extra=None, max_data=None,
                         interval=None, var_generator=None):
        '''
        Create a data callback function for streaming plots

        Parameters
        ----------
        x : string, optional
            x-axis variable name
        y : string or list-of-strings, optional
            y-axis variable names
        extra : string or list-of-strings, optional
            Extra variable names
        max_data : int, optional
            The maximum number of data points to return
        interval : int, optional
            The subscriber interval
        var_generator : callable, optional
            Function to call on new data to generate additional variables

        Examples
        --------
        Define function to generate colors

        >>> def var_gen(data):
        ...     return dict(x_colors=(data['x'] > 5).map({True: 'green',
        ...                                               False: 'red'}))

        Create callback function

        >>> func = win.to_data_callback(x='time', y=['x', 'y', 'z'], max_data=5,
        ...                             var_generator=var_gen)
        >>> func()
        {'time': [11.46, 11.49, 11.52, 11.55, 11.58],
         'x': [-2.9829, -2.9829, -3.0237, -2.9556, -2.9148],
         'x_colors': id
         375    red
         376    red
         377    red
         378    red
         379    red
         Name: x, dtype: object,
         'y': [9.112, 9.1529, 9.2346, 9.1937, 9.1529],
         'z': [-1.1169, -1.1169, -1.076, -1.0351, -1.1169]}

        Returns
        -------
        function

        '''
        if var_generator is None:
            var_generator = lambda x: dict()
        elif isinstance(var_generator, dict):
            var_generator = functools.partial(var_mapper, mapping=var_generator)

        empty = self.apply_transformers(get_dataframe(self))
        state = dict(events=empty, reset=False)

        def on_event(sock, event):
            try:
                if state['reset']:
                    state['reset'] = False
                    state['events'] = self.apply_transformers(event)[:max_data]
                else:
                    state['events'] = pd.concat([state['events'],
                                                 self.apply_transformers(event)],
                                                **CONCAT_OPTIONS)[:max_data]
            except:
                import traceback
                traceback.print_exc()

        subscriber = self.create_subscriber(mode='streaming',
                                            format='xml',
                                            # interval=interval,
                                            sort='%s:descending' % x,
                                            on_event=on_event)
        subscriber.start()

        if y is None:
            y = []
        elif isinstance(y, six.string_types):
            y = [y]

        if x is None:
            x = []
        elif isinstance(x, six.string_types):
            x = [x]

        if extra is None:
            extra = []
        elif isinstance(extra, six.string_types):
            extra = [extra]

        def data_callback(initial=False, max_data=None, terminate=False):
            if terminate:
                subscriber.stop()
                state['events'] = None
                return

            data = state['events']
            state['reset'] = True

            if data is None:
                return {}

            out = {}
            for name in x + y + extra:
                if name in data.index.names:
                    out[name] = data.index.get_level_values(name).tolist()
                else:
                    out[name] = data[name].tolist()

            out.update(var_generator(data))

            return out

        return data_callback

    def _get_colors(self, num, palette=None):
        '''
        Generate colors for plot

        Parameters
        ----------
        palette : function
            Function that takes an integer and returns that number of colors
        num : int
            The number of colors to return

        Returns
        -------
        list-of-strings

        '''
        from bokeh.palettes import viridis
        palette = palette or viridis
        return palette(num + 1)

    def streaming_line(self, x, y, steps=1e5, interval=1000,
                       max_data=100, var_generator=None, **kwargs):
        '''
        Display a streaming line plot in a Jupyter notebook

        Notes
        -----
        Streaming figures are only supported in Jupyter notebooks.

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string or list-of-strings
            The y-axis variable names
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        max_data : int, optional
            The maximum number of data points to display
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        **kwargs : keyword arguments, optional
            Arguments to send to the :func:`bokeh.plotting.figure` function

        Examples
        --------
        Create the streaming figure.  Evaluating this in a Jupyter notebook
        will display the streaming plot.

        >>> fig = dataw.streaming_line(x='time', y=['x', 'y', 'z'])
        >>> fig
        <esppy.plotting.StreamingChart at 0x7f98c0250e10>

        See Also
        --------
        :meth:`streaming_scatter`

        Returns
        -------
        :class:`StreamingChart`

        '''
        y = listify(y)

        chart_params, mthd_params = split_chart_params(**kwargs)

        out = StreamingChart(self, var_generator=var_generator,
                             steps=steps, interval=interval,
                             max_data=max_data, **chart_params)

        for item, params in zip(y, param_iter(mthd_params)):
            out.line(x, item, **params)

        return out

    def streaming_area(self, x, y, steps=1e5, interval=1000,
                       max_data=100, var_generator=None, **kwargs):
        '''
        Display a streaming area plot in a Jupyter notebook

        Notes
        -----
        Streaming figures are only supported in Jupyter notebooks.

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string or list-of-strings
            The y-axis variable names
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        max_data : int, optional
            The maximum number of data points to display
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        **kwargs : keyword arguments, optional
            Arguments to send to the :func:`bokeh.plotting.figure` function

        Examples
        --------
        Create the streaming figure.  Evaluating this in a Jupyter notebook
        will display the streaming plot.

        >>> fig = dataw.streaming_area(x='time', y=['x', 'y', 'z'])
        >>> fig
        <esppy.plotting.StreamingChart at 0x7f98c0250e10>

        See Also
        --------
        :meth:`streaming_scatter`

        Returns
        -------
        :class:`StreamingChart`

        '''
        y = listify(y)

        chart_params, mthd_params = split_chart_params(**kwargs)

        out = StreamingChart(self, var_generator=var_generator,
                             steps=steps, interval=interval,
                             max_data=max_data, **chart_params)

        for item, params in zip(y, param_iter(mthd_params)):
            out.area(x, item, **params)

        return out

    def streaming_hist(self, centers=None, heights=None, steps=1e5, interval=1000,
                       var_generator=None, **kwargs):
        '''
        Display a streaming histogram in a Jupyter notebook

        Notes
        -----
        Streaming figures are only supported in Jupyter notebooks.

        Parameters
        ----------
        centers : list-of-strings or regex, optional
            The names of the columns containing the bin centers
        heights : list-of-strings or regex, optional
            The names of the columns containing the bin heights
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        max_data : int, optional
            The maximum number of data points to display
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        **kwargs : keyword arguments, optional
            Arguments to send to the :func:`bokeh.plotting.figure` function

        See Also
        --------
        :meth:`streaming_area`

        Returns
        -------
        :class:`StreamingChart`

        '''
        if centers is None:
            centers = getattr(self, 'output_map', {}).get('binCentersOut', None)
        if heights is None:
            heights = getattr(self, 'output_map', {}).get('binHeightsOut', None)

        max_data = getattr(self, 'parameters', {}).get('nBins', 0)

        use_arrays = False
        centers_start = 0
        centers_end = 0
        heights_start = 0
        heights_end = 0

        # Check for array types centers[#-#]
        def extract_range(value):
            ''' Extract name and number range '''
            parts = re.match(r'^\s*(\w+)\s*\[\s*(\d+)\s*-\s*(\d+)\s*\]\s*$', value)
            name = parts.group(1)
            start = int(parts.group(2))
            end = int(parts.group(3))
            return name, start, end

        if isinstance(centers, six.string_types) and \
                re.match(r'^\s*\w+\s*\[\s*\d+\s*-\s*\d+\s*\]\s*$', centers):
            centers, centers_start, centers_end = extract_range(centers)
            use_arrays = True

        if isinstance(heights, six.string_types) and \
                re.match(r'^\s*\w+\s*\[\s*\d+\s*-\s*\d+\s*\]\s*$', heights):
            heights, heights_start, heights_end = extract_range(heights)
            use_arrays = True

        if not centers:
            raise ValueError('No center column names were specified')
        if not heights:
            raise ValueError('No height column names were specified')

        # Centers / heights are an array in one cell
        if use_arrays:
            if (heights_end - heights_start) != (centers_end - centers_start):
                raise ValueError('Center and height arrays are different lengths')

            if max_data == 0:
                max_data = heights_start - heights_end + 1

        # Centers / heights are individual columns
        else:
            centers = list(centers)
            heights = list(heights)

            if len(centers) != len(heights):
                raise ValueError('Center and height arrays are different lengths')

            if max_data == 0:
                max_data = len(centers)

        def hist_var_generator(data, centers=centers, heights=heights,
                               var_generator=var_generator):
            if not len(data):
                out = {'_centers_': [], '_heights_': []}
            else:
                out = {'_centers_': list(data[centers].iloc[-1]),
                       '_heights_': list(data[heights].iloc[-1])}
            if var_generator is not None:
                out.update(var_generator(data))
            return out

        kwargs.setdefault('x_axis_type', 'linear')
        kwargs.setdefault('line_width', 2)
        kwargs.setdefault('point_radius', 3)
        kwargs.setdefault('label', 'height')

        return self.streaming_area(x='_centers_', y='_heights_',
                                   interval=interval, steps=steps,
                                   var_generator=hist_var_generator,
                                   max_data=max_data, **kwargs)

    def streaming_scatter(self, x, y, steps=1e5, interval=1000,
                          max_data=100, var_generator=None, **kwargs):
        '''
        Display a streaming scatter plot in a Jupyter notebook

        Notes
        -----
        Streaming figures are only supported in Jupyter notebooks.

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string or list-of-strings
            The y-axis variable names
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        max_data : int, optional
            The maximum number of data points to display
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        **kwargs : keyword arguments, optional
            Arguments to send to the :func:`bokeh.plotting.figure` function

        Examples
        --------
        Create the streaming figure.  Evaluating this in a Jupyter notebook
        will display the streaming plot.

        >>> fig = dataw.streaming_scatter(x='time', y=['x', 'y', 'z'])
        >>> fig
        <esppy.plotting.StreamingChart at 0x7f98c0250e10>

        See Also
        --------
        :meth:`streaming_line`

        Returns
        -------
        :class:`StreamingChart`

        '''
        y = listify(y)

        chart_params, mthd_params = split_chart_params(**kwargs)

        out = StreamingChart(self, var_generator=var_generator,
                             steps=steps, interval=interval,
                             max_data=max_data, **chart_params)

        for item, params in zip(y, param_iter(mthd_params)):
            out.scatter(x, item, **params)

        return out

    def streaming_bubble(self, x, y, radius, steps=1e5, interval=1000,
                         max_data=100, var_generator=None, **kwargs):
        '''
        Display a streaming bubble plot in a Jupyter notebook

        Notes
        -----
        Streaming figures are only supported in Jupyter notebooks.

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string or list-of-strings
            The y-axis variable names
        r : string or list-of-strings
            The variable name that gives the point radius
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        max_data : int, optional
            The maximum number of data points to display
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        **kwargs : keyword arguments, optional
            Arguments to send to the :func:`bokeh.plotting.figure` function

        Examples
        --------
        Create the streaming figure.  Evaluating this in a Jupyter notebook
        will display the streaming plot.

        >>> fig = dataw.streaming_scatter(x='time', y=['x', 'y', 'z'])
        >>> fig
        <esppy.plotting.StreamingChart at 0x7f98c0250e10>

        See Also
        --------
        :meth:`streaming_line`

        Returns
        -------
        :class:`StreamingChart`

        '''
        y = listify(y)
        radius = listify(radius)

        if len(y) > 1 and len(radius) == 1:
            radius = radius * len(y)

        chart_params, mthd_params = split_chart_params(**kwargs)

        out = StreamingChart(self, var_generator=var_generator,
                             steps=steps, interval=interval,
                             max_data=max_data, **chart_params)

        for item, rad, params in zip(y, radius, param_iter(mthd_params)):
            out.bubble(x, item, rad, **params)

        return out

    def streaming_bar(self, x, y, steps=1e5, interval=1000,
                      max_data=100, var_generator=None, displayed_labels=None, **kwargs):
        '''
        Display a streaming bar plot in a Jupyter notebook

        Notes
        -----
        Streaming figures are only supported in Jupyter notebooks.

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string or list-of-strings
            The y-axis variable names
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        max_data : int, optional
            The maximum number of data points to display
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        **kwargs : keyword arguments, optional
            Arguments to send to the :func:`bokeh.plotting.figure` function

        Examples
        --------
        Create the streaming figure.  Evaluating this in a Jupyter notebook
        will display the streaming plot.

        >>> fig = dataw.streaming_bar(x='time', y=['x', 'y', 'z'])
        >>> fig
        <esppy.plotting.StreamingChart at 0x7f98c0250e10>

        See Also
        --------
        :meth:`streaming_line`

        Returns
        -------
        :class:`StreamingChart`

        '''
        y = listify(y)

        chart_params, mthd_params = split_chart_params(**kwargs)

        out = StreamingChart(self, var_generator=var_generator,
                             steps=steps, interval=interval,
                             max_data=max_data, **chart_params)

        for item, params in zip(y, param_iter(mthd_params)):
            out.bar(x, item, displayed_labels=displayed_labels, **params)

        return out

    def streaming_hbar(self, x, y, steps=1e5, interval=1000,
                       max_data=100, var_generator=None, **kwargs):
        '''
        Display a streaming horizontal bar plot in a Jupyter notebook

        Notes
        -----
        Streaming figures are only supported in Jupyter notebooks.

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string or list-of-strings
            The y-axis variable names
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        max_data : int, optional
            The maximum number of data points to display
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        **kwargs : keyword arguments, optional
            Arguments to send to the :func:`bokeh.plotting.figure` function

        Examples
        --------
        Create the streaming figure.  Evaluating this in a Jupyter notebook
        will display the streaming plot.

        >>> fig = dataw.streaming_bar(x='time', y=['x', 'y', 'z'])
        >>> fig
        <esppy.plotting.StreamingChart at 0x7f98c0250e10>

        See Also
        --------
        :meth:`streaming_line`

        Returns
        -------
        :class:`StreamingChart`

        '''
        y = listify(y)

        chart_params, mthd_params = split_chart_params(**kwargs)

        out = StreamingChart(self, var_generator=var_generator,
                             steps=steps, interval=interval,
                             max_data=max_data, **chart_params)

        for item, params in zip(y, param_iter(mthd_params)):
            out.hbar(x, item, **params)

        return out

    def streaming_donut(self, x, y, steps=1e5, interval=1000,
                        max_data=100, var_generator=None, **kwargs):
        '''
        Display a streaming donut plot in a Jupyter notebook

        Notes
        -----
        Streaming figures are only supported in Jupyter notebooks.

        Parameters
        ----------
        x : string
            The slice categories
        y : string or list-of-strings
            The values of the slices
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        max_data : int, optional
            The maximum number of data points to display
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        **kwargs : keyword arguments, optional
            Arguments to send to the StreamingChart object and donut method

        Examples
        --------
        Create the streaming figure.  Evaluating this in a Jupyter notebook
        will display the streaming plot.

        >>> fig = dataw.streaming_donut(x='time', y=['x', 'y', 'z'])
        >>> fig
        <esppy.plotting.StreamingChart at 0x7f98c0250e10>

        See Also
        --------
        :meth:`streaming_line`

        Returns
        -------
        :class:`StreamingChart`

        '''
        y = listify(y)

        chart_params, mthd_params = split_chart_params(**kwargs)

        out = StreamingChart(self, var_generator=var_generator,
                             steps=steps, interval=interval,
                             max_data=max_data, **chart_params)

        for item, params in zip(y, param_iter(mthd_params)):
            out.donut(x, item, **params)

        return out

    def streaming_pie(self, x, y, steps=1e5, interval=1000,
                      max_data=100, var_generator=None, **kwargs):
        '''
        Display a streaming pie plot in a Jupyter notebook

        Notes
        -----
        Streaming figures are only supported in Jupyter notebooks.

        Parameters
        ----------
        x : string
            The slice categories
        y : string or list-of-strings
            The values of the slices
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        max_data : int, optional
            The maximum number of data points to display
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        **kwargs : keyword arguments, optional
            Arguments to send to the StreamingChart object and pie method

        Examples
        --------
        Create the streaming figure.  Evaluating this in a Jupyter notebook
        will display the streaming plot.

        >>> fig = dataw.streaming_pie(x='time', y=['x', 'y', 'z'])
        >>> fig
        <esppy.plotting.StreamingChart at 0x7f98c0250e10>

        See Also
        --------
        :meth:`streaming_line`

        Returns
        -------
        :class:`StreamingChart`

        '''
        y = listify(y)

        chart_params, mthd_params = split_chart_params(**kwargs)

        out = StreamingChart(self, var_generator=var_generator,
                             steps=steps, interval=interval,
                             max_data=max_data, **chart_params)

        for item, params in zip(y, param_iter(mthd_params)):
            out.pie(x, item, **params)

        return out

    def streaming_images(self, image_col, steps=1e5, interval=1000, size='contain',
                         var_generator=None, annotations=None, **kwargs):
        '''
        Display streaming images in a Jupyter notebook

        Notes
        -----
        Streaming images are only supported in Jupyter notebooks.

        Parameters
        ----------
        image_col : string
            The column which contains the image
        steps : int, optional
            The maximum number of steps to iterate
        interval : int, optional
            The length of each step in milliseconds
        size : string or int or tuple, optional
            The size of the displayed image.  It can take the following values:
                * 'contain' - to indicate that the image should be scaled to
                  fit inside the plot area
                * 'cover' - to indicate that the image should scale to cover
                  the entire plot area
                * int - to indicate a square with a pixel width of height of
                  the given integer
                * (int, int) - to indicate width and height values in pixels
        var_generator : callable, optional
            Function to call on new data to generate additional variables
        plot_width : int, optional
            The width of the plot in pixels
        plot_height : int, optional
            The height of the plot in pixels

        Returns
        -------
        :class:`StreamingImages`

        '''
        return StreamingImages(self.to_data_callback(extra=image_col,
                                                     var_generator=var_generator,
                                                     interval=interval),
                               steps=steps, interval=interval, size=size,
                               annotations=annotations, **kwargs)


class Window(BaseWindow, SplitterExpressionFeature,
             SplitterPluginFeature, FinalizedCallbackFeature,
             ConnectorsFeature):
    '''
    Standard ESP Window

    Parameters
    ----------
    name : string, optional
        The name of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for
        the window and false disables it.
    description : string, optional
        Description of the window
    output_insert_only : bool, optional
        When true, prevents the window from passing non-insert events to
        other windows.
    collapse_updates : bool, optional
        When true, multiple update blocks are collapsed into a single
        update block
    pulse_interval : string, optional
        Output a canonical batch of updates at the specified interval
    exp_max_string : int, optional
        Specifies the maximum size of strings that the expression engine
        uses for the window. Default value is 1024.
    index_type : string, optional
        Index type for the window
        Valid values: 'rbtree', 'hash', 'ln_hash', 'cl_hash', 'fw_hash', 'empty'
    pubsub_index_type : string, optional
        Publish/subscribe index type.  Valid values are the same as for the
        `index_type` parameter.

    Attributes
    ----------
    connectors : list-of-Connectors
        The window connectors
    contquery : string
        The name of the continuous query the window is associated with
    data : pandas.DataFrame
        The cached rows of event data
    description : string
        A description of the window
    event_transformers : list
        A list of event transformer definitions
    finalized_callback : FinalizeCallback
        Shared library function for finalize callback
    project : string
        The name of the project the window is associated with
    splitter : SplitterExpression or SplitterPlugin
        Expression or plugin that directs events to one of n
        different output slots
    targets : set
        Set of target definitions
    url : string
        The URL of the window

    Returns
    -------
    :class:`Window`

    '''

    output_insert_only = attribute('output-insert-only', dtype='bool')
    collapse_updates = attribute('collapse-updates', dtype='bool')
    pulse_interval = attribute('pulse-interval', dtype='string')
    exp_max_string = attribute('exp-max-string', dtype='int')
    index_type = attribute('index', dtype='string', values=INDEX_TYPES)
    pubsub_index_type = attribute('pubsub-index', dtype='string', values=INDEX_TYPES)

    def __init__(self, name=None, pubsub=None, description=None,
                 output_insert_only=None, collapse_updates=None,
                 pulse_interval=None, exp_map_string=None, index_type=None,
                 pubsub_index_type=None, **kwargs):
        kwargs.update(**get_args(locals()))
        BaseWindow.__init__(self, **kwargs)
