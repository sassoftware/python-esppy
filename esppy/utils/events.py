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

''' Event Parsing Utilities '''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
import csv
import datetime
import decimal
import json
import numpy as np
import os
import pandas as pd
import re
import six
import sys
import xml.etree.ElementTree as ET
from six.moves import urllib
from ..base import ESPObject
from ..config import get_option

EPOCH = datetime.datetime(1970, 1, 1)


def str_to_float(value):
    ''' Convert value to float '''
    if isinstance(value, six.string_types):
        if value.strip() == '' or 'nan' in value.lower():
            return np.nan
    return np.float64(value)


def str_to_int32(value):
    ''' Convert value to float '''
    if isinstance(value, six.string_types):
        if value.strip() == '' or 'nan' in value.lower():
            return np.nan
    return np.int32(value)


def str_to_int64(value):
    ''' Convert value to float '''
    if isinstance(value, six.string_types):
        if value.strip() == '' or 'nan' in value.lower():
            return np.nan
    return np.int64(value)


def double_array(value):
    ''' Convert value to array of doubles '''
    if isinstance(value, six.string_types):
        if re.match(r'^\s*\[', value):
            out = [str_to_float(x)
                   for x in re.sub(r'[\[\]\s+]', r'', value).split(';')]
        else:
            out = [str_to_float(value)]
    else:
        out = [np.float64(value)]
    return np.array(out, dtype=np.float64)


def int32_array(value):
    ''' Convert value to array of int32s '''
    if isinstance(value, six.string_types):
        if re.match(r'^\s*\[', value):
            out = [str_to_int32(x)
                   for x in re.sub(r'[\[\]\s+]', r'', value).split(';')]
        else:
            out = [str_to_int32(value)]
    else:
        out = [np.int32(value)]
    return np.array(out, dtype=np.int32)


def int64_array(value):
    ''' Convert value to array of int32s '''
    if isinstance(value, six.string_types):
        if re.match(r'^\s*\[', value):
            out = [str_to_int64(x)
                   for x in re.sub(r'[\[\]\s+]', r'', value).split(';')]
        else:
            out = [str_to_int64(value)]
    else:
        out = [np.int64(value)]
    return np.array(out, dtype=np.int64)


ESP2PY_MAP = {
    'date': lambda x: EPOCH + datetime.timedelta(seconds=int(x)),
    'stamp': lambda x: EPOCH + datetime.timedelta(microseconds=int(x)),
    'double': str_to_float,
    'int64': lambda x: np.int64(decimal.Decimal(x)),
    'int32': lambda x: np.int32(decimal.Decimal(x)),
    'money': decimal.Decimal,
    'blob': base64.b64decode,
    'string': lambda x: hasattr(x, 'decode') and x.decode('utf-8') or x,
    'array(dbl)': double_array,
    'array(double)': double_array,
    'array(i32)': int32_array,
    'array(int32)': int32_array,
    'array(i64)': int64_array,
    'array(int64)': int64_array,
}

ESP2DF_TYPEMAP = {
    'date': datetime.datetime.now(),
    'stamp': datetime.datetime.now(),
    'double': np.float64(0),
    'int64': np.int64(0),
    'int32': np.int32(0),
    'money': decimal.Decimal(0),
    'blob': b'bytes',
    'string': u'string',
    'array(dbl)': pd.Series(dtype=np.float64),
    'array(double)': pd.Series(dtype=np.float64),
    'array(i32)': pd.Series(dtype=np.int32),
    'array(int32)': pd.Series(dtype=np.int32),
    'array(i64)': pd.Series(dtype=np.int64),
    'array(int64)': pd.Series(dtype=np.int64),
}


def get_dataframe(obj):
    '''
    Get an empty DataFrame that represents the Window schema

    Parameters
    ----------
    schema : Schema, optional
        The schema to use instead of the given window's schema

    Returns
    -------
    :class:`pandas.DataFrame`

    '''
    try:
        BaseWindow, Schema    # noqa: F821
    except:
        from ..schema import Schema
        from ..windows import BaseWindow

    if isinstance(obj, Schema):
        schema = obj
    elif isinstance(obj, BaseWindow) and obj.schema.fields:
        schema = obj.schema
    else:
        schema = get_schema(obj, obj.fullname)

    columns = []
    row = []
    index = []
    int32s = []
    int32_arrays = []
    for field in schema.fields.values():
        columns.append(field.name)
        row.append(ESP2DF_TYPEMAP[field.type])
        if field.type == 'int32':
            int32s.append(field.name)
        elif field.type in ['array(int32)', 'array(i32)']:
            int32s.append(field.name)
        if field.key:
            index.append(field.name)

    out = pd.DataFrame(columns=columns, data=[row])

    for name in int32s:
        out[name] = out[name].astype('int32')
    for name in int32_arrays:
        out[name] = out[name].apply(lambda x: x.astype('int32'),
                                    convert_dtype=False)

    if index:
        out = out.set_index(index)

    return out.iloc[0:0]


def get_schema(obj, window):
    ''' Retrieve the schema for the specified window '''
    try:
        get_window_class    # noqa: F821
    except:
        from ..windows import get_window_class

    if isinstance(window, six.string_types):
        path = window.replace('.', '/')
    else:
        if getattr(window, 'schema') and window.schema.fields:
            return window.schema.copy(deep=True)
        path = window.fullname.replace('.', '/')

    res = obj._get(urllib.parse.urljoin(obj.base_url, 'windows/%s' % path),
                   params=dict(schema='true'))

    for item in res.findall('./*'):
        try:
            wcls = get_window_class(item.tag)
        except KeyError:
            raise TypeError('Unknown window type: %s' % item.tag)
        return wcls.from_xml(item, session=obj.session).schema


def get_events(obj, data, format='xml', separator=None, single=False, server_info=None):
    '''
    Convert events to DataFrames

    Parameters
    ----------
    obj : ESPObject
        The calling object.  If this is a Schema, that schema is used
        for the events.  If it is a Window, the schema for tha window
        is used for the events.
    data : xml-string or ElementTree.Element
        The events to process
    format : string, optional
        The format of the events
    separator : string, optional
        The separator between each 'properties' events
    single : bool, optional
        Only return a single DataFrame rather than a dictionary.
        If there is more than one DataFrame, raise an exception.
    server_info : dict, optional
        Information about the server, for version-specific behaviors

    Returns
    -------
    dict of :class:`pandas.DataFrame`
        If single == False
    :class:`pandas.DataFrame`
        If single == True

    '''
    try:
        BaseWindow, Schema    # noqa: F821
    except:
        from ..schema import Schema
        from ..windows import BaseWindow

    server_info = server_info or {}

    if get_option('debug.events'):
        sys.stderr.write('%s\n' % data)

    if format.lower() == 'csv':
        return get_csv_events(obj, data)

    if format.lower() == 'json':
        return get_json_events(obj, data)

    if format.lower() == 'properties':
        try:
            return get_properties_events(obj, data, separator)
        except:
            import traceback
            traceback.print_exc()
            raise

    if isinstance(data, six.string_types):
        data = ET.fromstring(data)

    windows = dict()
    from . import xml
    for event in data.findall('./event'):

        wname = event.attrib.get('window', '')

        if wname not in windows:

            current = windows[wname] = dict(transformers={}, columns=[],
                                            index=[], events=[], dtypes=[])

            if isinstance(obj, Schema):
                schema = obj
            elif isinstance(obj, BaseWindow) and obj.fullname == wname.replace('/', '.') and obj.schema.fields:
                schema = obj.schema
            elif not wname:
                if isinstance(obj, BaseWindow):
                    schema = get_schema(obj, obj.fullname)
                else:
                    raise ValueError('Could not determine window schema')
            else:
                schema = get_schema(obj, wname)

            for field in schema.fields.values():
                current['transformers'][field.name] = ESP2PY_MAP.get(field.type,
                                                                     lambda x: x)
                current['columns'].append(field.name)
                current['dtypes'].append(field.type)
                if field.key:
                    current['index'].append(field.name)

        else:
            current = windows[wname]

        row = dict()
        for item in event.findall('./*'):
            row[item.tag] = current['transformers'].get(item.tag, lambda x: x)(item.text)
        current['events'].append(row)

    out = dict()
    for wname, window in windows.items():
        orig_wname = wname
        wname = wname.replace('/', '.')
        out[wname] = pd.DataFrame(window['events'])
        columns = [x for x in window['columns'] if x in out[wname].columns]
        out[wname] = out[wname][columns]
        for colname, dtype in zip(windows[orig_wname]['columns'],
                                  windows[orig_wname]['dtypes']):
            if dtype == 'int32':
                out[wname][colname] = out[wname][colname].astype('int32', copy=False)
        if window['index']:
            index = [x for x in window['index'] if x in out[wname].columns]
            if index:
                out[wname] = out[wname].set_index(index)

    if single:
        if len(out) == 1:
            return list(out.values())[0]
        elif not out:
            return get_dataframe(obj)
        raise ValueError('Output contains more than one value: %s' % out)

    return out


def get_csv_events(obj, data):
    '''
    Convert CSV events to DataFrames

    Parameters
    ----------
    obj : ESPObject
        The calling object.  If this is a Schema, that schema is used
        for the events.  If it is a Window, the schema for tha window
        is used for the events.
    data : csv-string
        The events to process

    Returns
    -------
    :class:`pandas.DataFrame`

    '''
    try:
        BaseWindow, Schema    # noqa: F821
    except:
        from ..schema import Schema
        from ..windows import BaseWindow

    if isinstance(obj, Schema):
        schema = obj
    elif isinstance(obj, BaseWindow):
        if obj.schema.fields:
            schema = obj.schema
        else:
            schema = get_schema(obj, obj)
    else:
        raise ValueError('Can not obtain window schema from given object')

    transformers = []
    columns = []
    index = []
    dtypes = []
    for fname, field in schema.fields.items():
        transformers.append(ESP2PY_MAP.get(field.type, lambda x: x))
        columns.append(fname)
        dtypes.append(field.type)
        if field.key:
            index.append(fname)

    rows = []
    for row in csv.reader(data.rstrip().split('\n')):
        row = list(row)[2:]
        for i, item in enumerate(row):
            row[i] = transformers[i](item)
        rows.append(row)

    out = pd.DataFrame(data=rows, columns=columns)
    out = out.set_index(index)
    for colname, dtype in zip(columns, dtypes):
        if dtype == 'int32':
            out[colname] = out[colname].astype('int32', copy=False)
    return out


def get_json_events(obj, data):
    '''
    Convert JSON events to DataFrames

    Parameters
    ----------
    obj : ESPObject
        The calling object.  If this is a Schema, that schema is used
        for the events.  If it is a Window, the schema for tha window
        is used for the events.
    data : json-string
        The events to process

    Returns
    -------
    :class:`pandas.DataFrame`

    '''
    try:
        BaseWindow, Schema    # noqa: F821
    except:
        from ..schema import Schema
        from ..windows import BaseWindow

    if isinstance(obj, Schema):
        schema = obj
    elif isinstance(obj, BaseWindow):
        if obj.schema.fields:
            schema = obj.schema
        else:
            schema = get_schema(obj, obj)
    else:
        raise ValueError('Can not obtain window schema from given object')

    transformers = {}
    columns = []
    index = []
    dtypes = []
    for fname, field in schema.fields.items():
        transformers[fname] = ESP2PY_MAP.get(field.type, lambda x: x)
        columns.append(fname)
        dtypes.append(field.type)
        if field.key:
            index.append(fname)

    rows = []
    for event in json.loads(data)['events']:
        event = event['event']
        row = []
        for col in columns:
            row.append(transformers[col](event[col]))
        rows.append(row)

    out = pd.DataFrame(data=rows, columns=columns)
    out = out.set_index(index)
    for colname, dtype in zip(columns, dtypes):
        if dtype == 'int32':
            out[colname] = out[colname].astype('int32', copy=False)
    return out


def get_properties_events(obj, data, separator=None):
    '''
    Convert properties events to DataFrames

    Parameters
    ----------
    obj : ESPObject
        The calling object.  If this is a Schema, that schema is used
        for the events.  If it is a Window, the schema for tha window
        is used for the events.
    data : json-string
        The events to process

    Returns
    -------
    :class:`pandas.DataFrame`

    '''
    try:
        BaseWindow, Schema    # noqa: F821
    except:
        from ..schema import Schema
        from ..windows import BaseWindow

    if separator is None:
        separator = '\n\n'

    if isinstance(obj, Schema):
        schema = obj
    elif isinstance(obj, BaseWindow):
        if obj.schema.fields:
            schema = obj.schema
        else:
            schema = get_schema(obj, obj)
    else:
        raise ValueError('Can not obtain window schema from given object: %s' % obj)

    transformers = {}
    columns = []
    index = []
    for fname, field in schema.fields.items():
        transformers[fname] = ESP2PY_MAP.get(field.type, lambda x: x)
        columns.append(fname)
        if field.key:
            index.append(fname)

    rows = []
    for event in [x for x in data.split(separator) if x.strip()]:
        row = []
        for i, col in enumerate(x for x in event.split('\n') if x.strip()):
            if i == 0 and col.startswith('opcode='):
                continue
            col, value = col.split('=', 1)
            row.append(transformers[col](value))
        rows.append(row)

    out = pd.DataFrame(data=rows, columns=columns)
    out = out.set_index(index)
    return out
