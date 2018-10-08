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

''' ESP Plotting Utilities '''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
import collections
import copy
import csv
import datetime
import io
import json
import numpy as np
import os
import pandas as pd
import re
import requests
import six
import threading
import time
import uuid
import warnings
import weakref
import xml.etree.ElementTree as ET
from six.moves import urllib
from .base import ESPObject
from .config import CONCAT_OPTIONS
from .schema import Schema
from .utils import xml
from .utils.rest import get_params
from .utils.data import get_project_data, gen_name
from .utils.events import get_events

#
# The streaming_* plot methods create a chart automatically so they
# must accept parameters for both the chart and for the plotting methods.
# This is the list of chart parameters that get filtered out.
#
CHART_PARAMS = set(re.split(r'\s+', '''
    title x_axis_label x_axis_location x_axis_type y_axis_label
    y_axis_location y_axis_type x_range y_range plot_width plot_height
    responsive palette
'''.strip()))


def listify(data):
    ''' Make sure data is a list '''
    if data is None:
        return []
    if isinstance(data, (set, list, tuple)):
        return list(data)
    return [data]


def camelize(name):
    ''' Convert underscore delimited name to camel-case '''
    return re.sub(r'[_-](\w)', lambda m: m.group(1).upper(), name)


def split_chart_params(**kwargs):
    '''
    Split the keyword arguments into chart and plot method params

    Parameters
    ----------
    **kwargs : keyword arguments
        The parameters to split into categories

    Returns
    -------
    (chart-params-dict, plot-method-params-dict)

    '''
    chart_params = {}
    mthd_params = {}
    for key, value in kwargs.items():
        if key in CHART_PARAMS:
            chart_params[key] = value
        else:
            mthd_params[key] = value
    return chart_params, mthd_params


def highlight_image(pil_image, detections, line_width=3, font=None):
    from PIL import ImageDraw

    row = detections.iloc[-1]
    n_objects = row['_nObjects_']
    if n_objects == 0:
        return pil_image

    draw = ImageDraw.Draw(pil_image)

    for i in range(0, int(n_objects)):
        obj = row['_Object%s_' % i]
        prob = row['_P_Object%s_' % i]
        x = int(row['_Object%s_x' % i])
        y = int(row['_Object%s_y' % i])
        width = int(row['_Object%s_width' % i])
        height = int(row['_Object%s_height' % i])
        value = int(prob * 255)

        points = (
            (x, y),
            (x+width, y),
            (x+width, y+height),
            (x, y+height),
            (x, y)
        )

        color = (value, 0, 0, 128)

        draw.line(points, fill=color, width=line_width)
        for point in points:
            draw.ellipse((point[0] - int(line_width / 2),
                          point[1] - int(line_width / 2),
                          point[0] + int(line_width / 2),
                          point[1] + int(line_width / 2)),
                          outline=color, fill=color)

        #draw.rectangle([(x, y), (x+width, y+height)],
        #               outline=(value, 0, 0, 192),
        #               fill=None)
        #font = ImageFont.truetype('/usr/share/fonts/dejavu/DejaVuSans.ttf', 16)
        draw.text((x, y), obj, fill=(255, 255, 255, 255), font=font)
        #cv2.rectangle(img_np, (x, y), (x+width,y+height), (value,0, 0), 2)
        #cv2.putText(img_np, obj, (x,y), cv2.FONT_HERSHEY_SIMPLEX, 1, (255,255,255), 2)

    return pil_image


# Python 2 / 3 compatibility
try:
    long
except:
    long = int


class ESPDataEncoder(json.JSONEncoder):
    '''
    Custom JSON encoder for ESP data

    '''

    def default(self, obj):
        '''
        Convert objects unrecognized by the default encoder

        Parameters
        ----------
        obj : any
           Arbitrary object to convert

        Returns
        -------
        any
           Python object that JSON encoder will recognize

        '''
        if isinstance(obj, (float, np.float64)):
            return float(obj)
        if isinstance(obj, (long, np.int64)):
            return long(obj)
        if isinstance(obj, (int, np.int32)):
            return int(obj)
        return json.JSONEncoder.default(self, obj)


def dump_json(*args, **kwargs):
    '''
    Encode JSON using ESP data types

    '''
    kwargs['cls'] = ESPDataEncoder
    return json.dumps(*args, **kwargs)


class StreamingChart(object):
    '''
    Streaming Chart

    Parameters
    ----------
    data : Window or DataFrame or callable
        The object to get the data from
    title : string, optional
        The title for the chart
    x_axis_label : string, optional
        The label for the x-axis
    x_axis_location : string, optional
        The location of the x-axis: 'top', 'bottom'
    x_axis_type : string, optional
        The type of x-axis: 'linear', 'category', 'logarithmic'
    y_axis_label : string, optional
        The label for the y-axis
    y_axis_location : string, optional
        The location of the y-axis: 'left', 'right'
    y_axis_type : string, optional
        The type of y-axis: 'linear', 'category', 'logarithmic'
    x_range : tuple, optional
        The range and step size of the x-axis in the form (min, max, step)
    y_range : tuple, optional
        The range and step size of the y-axis in the form (min, max, step)
    plot_width : int, optional
        The width of the plot
    plot_height : int, optional
        The height of the plot
    responsive : bool, optional
        Should the dimensions of the plot adjust to fit?
    palette : callable, optional
        Callable object to return a specified number of colors
    steps : int, optional
        The maximum number of calls to update the data in the chart
    interval : int, optional
        The time between data calls in milliseconds
    max_data : int, optional
        The maximum number of data points to store in each data set
    var_generator : callable, optional
        Callable object used to create new transient data columns for
        use in chart parameters

    Returns
    -------
    :class:`StreamingChart`

    '''

    def __init__(self, data, title=None,
                 x_axis_label=None, x_axis_location=None, x_axis_type=None,
                 y_axis_label=None, y_axis_location=None, y_axis_type=None,
                 x_range=None, y_range=None, plot_width=900, plot_height=400,
                 responsive=True, palette=None,
                 steps=1e5, interval=1000, max_data=100, var_generator=None):
        self.max_data = max(int(max_data), 1)
        self.steps = max(int(steps), 0)
        self.interval = max(int(interval), 10)
        self.plot_width = max(plot_width, 10)
        self.plot_height = max(plot_height, 10)

        if palette is None:
            def default_palette(num):
                return ['#1f78b4', '#ff7f00', '#33a02c', '#6a3d9a', '#e31a1c',
                        '#a6cee3', '#cab2d6', '#b2df8a', '#cab2d6', '#fb9a99',
                        '#ffff99', '#b15928'][:num]
            self.palette = default_palette
        else:
            self.palette = palette

        self._data_callback = self._make_callback(data, max_data=self.max_data)
        self._var_generator = var_generator

        self.type = 'line'

        self.data = dict(
            labels=[],
            datasets=[],
        )

        self.options = dict(
            responsive=responsive,

            title=dict(
                display=title and True or False,
                text=title or ''
            ),

            legend=dict(
                display=True,
                position='right',
                labels=dict(
                    usePointStyle=True
                )
            ),

        )

        if [x for x in [x_range, y_range,
                        x_axis_label, y_axis_label,
                        x_axis_type, y_axis_type] if x is not None]:
            scales = dict(xAxes=[{}], yAxes=[{}])

            self.options['scales'] = scales

            if x_axis_type is not None:
                scales['xAxes'][0]['type'] = x_axis_type
            if y_axis_type is not None:
                scales['yAxes'][0]['type'] = y_axis_type

            if x_axis_label is not None:
                scales['xAxes'][0].setdefault('scaleLabel', {})['labelString'] = \
                    x_axis_label
                scales['xAxes'][0].setdefault('scaleLabel', {})['display'] = True
            if y_axis_label is not None:
                scales['yAxes'][0].setdefault('scaleLabel', {})['labelString'] = \
                    y_axis_label
                scales['yAxes'][0].setdefault('scaleLabel', {})['display'] = True

            self._set_range(x_range, 'x')
            self._set_range(y_range, 'y')

    def _set_range(self, limits, axis, index=0):
        '''
        Set range limits

        Parameters
        ----------
        limits : list or tuple
            The min, max, and step-size of the axis
        axis : string
            Which axis to apply range to: 'x' or 'y'
        index : int, optional
            Which axis index to apply range to (if using multiple axes)

        '''
        limits = listify(limits)
        if limits is not None:
            limits += [None, None, None]
            ticks = self.options['scales']['%sAxes' % axis][index].setdefault('ticks', {})
            if limits[0] is not None:
                ticks['min'] = limits[0]
            if limits[1] is not None:
                ticks['max'] = limits[1]
            if limits[2] is not None:
                ticks['stepSize'] = limits[2]

    def _make_callback(self, data, max_data=100):
        '''
        Create a data callback function for the given object

        Parameters
        ----------
        data : Window or DataFrame-like or callable
            The object to retrieve data from
        max_data : int, optional
            The maximum number of rows of data to retrieve

        Returns
        -------
        function

        '''
        from .windows import BaseWindow
        from .utils.events import get_dataframe

        if isinstance(data, BaseWindow):

            lock = threading.RLock()
            empty_df = get_dataframe(data)
            state = dict(df=empty_df, reset=False, updated=False)

            def on_event(sock, event):
                with lock:
                    if state['reset']:
                        state['df'] = event.tail(self.max_data)
                    else:
                        state['df'] = pd.concat([state['df'], event],
                                                **CONCAT_OPTIONS).tail(self.max_data)
                    state['updated'] = True

            sub = data.create_subscriber(mode='streaming',
                                         interval=self.interval,
                                         pagesize=self.max_data,
                                         on_event=on_event)

            def data_callback(initial=False, max_data=max_data, terminate=False):
                if initial:
                    sub.start()
                if terminate:
                    sub.stop()
                    return
                if state['updated']:
                    with lock:
                        out = state['df'].tail(max_data)
                        state['reset'] = True
                        state['updated'] = False
                        return out
                return empty_df

        elif hasattr(data, 'tail'):
            def data_callback(initial=False, max_data=max_data, terminate=False):
                return data.tail(max_data)

        else:
            def data_callback(initial=False, max_data=max_data, terminate=False):
                return data(initial=initial, max_data=max_data,
                            terminate=terminate).tail(max_data)

        return data_callback

    def _get_data(self, data, initial=False, terminate=False):
        '''
        Retrieve new data points

        Parameters
        ----------
        data : dict
            The data definition
        initial : bool, optional
            Is this the first time the callback is being executed?
        terminate : bool, optional
            Should allocated resources be cleaned up?

        Returns
        -------
        ([labels], [displayed-labels], [datasets-dicts])

        '''
        df = self._data_callback(initial=initial, terminate=terminate)

        if terminate:
            return [], []

        if self._var_generator:
            extra = self._var_generator(df)
        else:
            extra = {}

        df = df.reset_index()
        labels = []
        displayed_labels = None
        datasets = []
        num_obs = 0

        for i, dataset in enumerate(data['datasets']):
            key = dataset['_key']
            x = dataset['_x']
            y = dataset['_y']
            x_labels = dataset.get('_displayed_labels', None)
            radius = dataset['_radius']
            ctype = dataset['type']
            use_labels = None

            if initial or re.match(r'^\s*var\s*\([^\)]+\)\s*$', dataset.get('label', '')):
                out = copy.deepcopy(dataset)
            else:
                out = dict(_key=key)

            if ctype in ['scatter', 'area', 'line']:
                out['data'] = self._get_xy_data(df, x, y, gen_vars=extra)
                use_labels = out['data']
                num_obs = len(out['data'])

            elif ctype in ['bubble']:
                out['data'] = self._get_xyz_data(df, x, y, radius, gen_vars=extra)
                use_labels = out['data']
                num_obs = len(out['data'])

            else:
                labels, displayed_labels, obs = \
                    self._get_labeled_data(df, x, y,
                                           displayed_labels=x_labels, gen_vars=extra)
                out['data'] = obs
                num_obs = len(out['data'])

            self._set_dynamic_vars(dataset, out, df, extra)

            datasets.append(out)

        if not labels:
            if use_labels:
                labels = [item['x'] for item in use_labels]
            else:
                labels = [None] * num_obs

        return self._normalize_data(labels, displayed_labels, datasets, initial=initial)

    def _alias_colors(self, dataset, color):

        def autoify(value):
            if isinstance(value, (tuple, list)):
                return ['auto(%s)' % x for x in value]
            return 'auto(%s)' % value

        if not dataset.get('borderColor') and not dataset.get('backgroundColor'):
            dataset['borderColor'] = autoify(color)
            dataset['backgroundColor'] = autoify(color)

        elif dataset.get('borderColor') and not dataset.get('backgroundColor'):
            dataset['backgroundColor'] = autoify(dataset['borderColor'])

        elif dataset.get('backgroundColor') and not dataset.get('borderColor'):
            if isinstance(dataset['backgroundColor'], six.string_types):
                if dataset['backgroundColor'].startswith('url'):
                    dataset['borderColor'] = autoify(color)
                else:
                    dataset['borderColor'] = autoify(dataset['backgroundColor'])

    def _normalize_data(self, labels, displayed_labels, datasets, initial=False):
        out = []
        palette = self.palette(len(datasets))

        for i, dataset in enumerate(datasets):

            if 'label' not in dataset or not isinstance(dataset['label'], list):
                if initial:
                    self._alias_colors(dataset, palette[i])
                out.append(dataset)
                continue

            finished = set()

            for i, label in enumerate(dataset['label']):
                if label in finished:
                    continue
                else:
                    finished.add(label)

                ds = copy.deepcopy(dataset)

                self._apply_mask(label, ds['label'], ds['data'])

                if isinstance(ds.get('borderColor'), list):
                    ds['borderColor'] = ds['borderColor'][i]
                if isinstance(ds.get('backgroundColor'), list):
                    ds['backgroundColor'] = ds['backgroundColor'][i]
                if isinstance(ds.get('pointStyle'), list):
                    ds['pointStyle'] = ds['pointStyle'][i]
                if ds.get('pointStyle'):
                    ds['pointStyle'] = camelize(ds['pointStyle'])

                ds['label'] = label
                ds['_key'] = '%s-%s' % (ds['_key'], ds['label'])

                self._alias_colors(ds, palette[i % len(palette)])

                out.append(ds)

        return labels, displayed_labels, out

    def _apply_mask(self, label, labels, value, repl=None):
        for i, item in enumerate(labels):
            if item != label:
                value[i] = repl

    def _set_dynamic_vars(self, dataset, out, *data):
        '''
        Set the values of attributes set by variables

        Parameters
        ----------
        dataset : dict
            The input chart dataset configuration
        out : dict
            The output chart dataset configuration
        *data : one-or-more DataFrame-or-dicts
            The data sources for any variables

        '''
        for name in ['label', 'borderColor', 'backgroundColor', 'pointStyle']:
            varname = self._extract_varname(dataset.get(name, ''))
            if varname is not None:
                values = self._get_values(varname, *data)
                if values is not None:
                    if name == 'pointStyle':
                        out[name] = [camelize(x) for x in values]
                    else:
                        out[name] = values

    def _extract_varname(self, value):
        '''
        Extract variable name from parameter value

        Parameter values that are indicated by variables must have the
        form ``var(name)``.

        Parameters
        ----------
        value : string
            The string to look for a variable name

        Returns
        -------
        string
            The name of the variable, if one is found
        None
            If no variable name is found

        '''
        if isinstance(value, six.string_types):
            m = re.match(r'^\s*var\s*\(\s*([^\)]+)\s*\)\s*$', value)
            if m:
                return m.group(1)

    def _get_values(self, name, *data):
        '''
        Retrieve the list of values for the given data name

        Parameters
        ----------
        name : string
            The name of the variable to look for
        *data : one-or-more DataFrames-or-dicts
            The data sets to look in

        Returns
        -------
        list-of-strings
            If the name was found in one of the data sets
        None
            If the name was not found in any of the data sets

        '''
        for item in data:
            if name in item:
                return ['%s' % value for value in item[name]]

    def _add_dataset(self, params, x, y, radius=None):
        '''
        Add a data set to the chart configuration

        Parameters
        ----------
        params : dict
            The chart parameters
        x : string
            The independent variable name
        y : string
            The dependent variable name
        radius : string, optional
            The variable name of the variable that indicates the radius
            of the points in a bubble chart

        '''
        if not self.data['datasets'] or params['type'] in ['pie', 'bar', 'horizontalBar']:
            self.type = params['type']
        params['_x'] = x
        params['_y'] = y
        params['_radius'] = radius
        params['_key'] = 'ds%02d' % len(self.data['datasets'])
        params['data'] = []
        self.data['datasets'].append(params)

    def _get_xy_data(self, data, x, y, gen_vars=None):
        '''
        Return the requested data in [{'x': ..., 'y': ...}] form

        Parameters
        ----------
        data : DataFrame
            The DataFram to retrieve data from
        x : string
            The variable for the x-axis
        y : string
            The variable for the y-axis

        Returns
        -------
        list-of-dicts

        '''
        gen_vars = gen_vars or {}
        out = {}
        out_cols = [x, y]
        if x in gen_vars:
            out[x] = gen_vars[x]
            out_cols[0] = None
        if y in gen_vars:
            out[y] = gen_vars[y]
            out_cols[1] = None
        out_cols = [col for col in out_cols if col is not None]
        if out_cols:
            out.update(data.loc[:, out_cols].to_dict(orient='list'))
        return [dict(x=x, y=y) for x, y in zip(out[x], out[y])]

    def _get_xyz_data(self, data, x, y, r, gen_vars=None):
        '''
        Return the requested data in [{'x': ..., 'y': ..., 'r': ...}] form

        Parameters
        ----------
        data : DataFrame
            The DataFram to retrieve data from
        x : string
            The variable for the x-axis
        y : string
            The variable for the y-axis
        r : string
            The variable for the radius of the points

        Returns
        -------
        list-of-dicts

        '''
        gen_vars = gen_vars or {}
        out = {}
        out_cols = [x, y, r]
        if x in gen_vars:
            out[x] = gen_vars[x]
            out_cols[0] = None
        if y in gen_vars:
            out[y] = gen_vars[y]
            out_cols[1] = None
        if r in gen_vars:
            out[r] = gen_vars[r]
            out_cols[2] = None
        out_cols = [col for col in out_cols if col is not None]
        if out_cols:
            out.update(data.loc[:, out_cols].to_dict(orient='list'))
        return [dict(x=x, y=y, r=r) for x, y, r in zip(out[x], out[y], out[r])]

    def _get_labeled_data(self, data, x, y, displayed_labels=None, gen_vars=None):
        '''
        Return the requested data as a tuple of lables and data points

        Parameters
        ----------
        data : DataFrame
            The DataFrame to retrieve data from
        x : string
            The variable for the x-axis
        y : string
            The variable for the y-axis
        displayed_labels : string
            The variable containing x-axis labels

        Returns
        -------
        ([labels], [displayed-labels], [data-points])

        '''
        gen_vars = gen_vars or {}
        out = {}
        out_cols = [x, displayed_labels, y]
        if x in gen_vars:
            out[x] = gen_vars[x]
            out_cols[0] = None
        if displayed_labels in gen_vars:
            out[displayed_labels] = gen_vars[displayed_labels]
            out_cols[1] = None
        if y in gen_vars:
            out[y] = gen_vars[y]
            out_cols[2] = None
        out_cols = [col for col in out_cols if col is not None]
        if out_cols:
            out.update(data.loc[:, out_cols].to_dict(orient='list'))
        if displayed_labels:
            return out[x], out[displayed_labels], out[y]
        return out[x], None, out[y]

    def pie(self, x, y, line_color=None, line_width=1,
            fill_color=None, rotation=None, circumference=None):
        '''
        Create a pie chart

        Parameters
        ----------
        x : string
            The slice categories
        y : string
            The value of the slice
        line_color : string, optional
            CSS color of the arc borders
        line_width : string, optional
            The width of the arc borders
        fill_color : string, optional
            CSS fill color for the arcs
        rotation : float, optional
            The angle, in radians, to start drawing arcs from
        circumference : float, optional
            The angle, in radians, to sweep the arcs

        '''
        cutout_percentage = 0
        return self.donut(**{k: v for k, v in locals().items() if k != 'self'})

    def donut(self, x, y, line_color=None, line_width=1,
              fill_color=None, cutout_percentage=None, rotation=None,
              circumference=None, color=None):
        '''
        Create a donut chart

        Parameters
        ----------
        x : string
            The slice categories
        y : string
            The value of the slice
        line_color : string, optional
            CSS color of the arc borders
        line_width : string, optional
            The width of the arc borders
        fill_color : string, optional
            CSS fill color for the arcs
        cutout_percentage : int, optional
            The percentage of the center of the chart that is empty
        rotation : float, optional
            The angle, in radians, to start drawing arcs from
        circumference : float, optional
            The angle, in radians, to sweep the arcs

        '''
        donut_param_map = dict(line_color='borderColor',
                               line_width='borderWidth', color='*borderColor',
                               fill_color='backgroundColor',
                               cutout_percentage='cutoutPercentage',
                               rotation='rotation', circumference='circumference')

        params = {donut_param_map[k]: v for k, v in locals().items()
                  if k in donut_param_map and v is not None}

        if '*borderColor' in params and 'borderColor' not in params:
            params['borderColor'] = params.pop('*borderColor')

        if params.get('borderWidth', 0) == 0:
            params['borderWidth'] = 0.001

        if params.get('borderColor') is None:
            params['borderColor'] = self.palette(12)

        params['type'] = 'doughnut'

        # These affect all donut charts on this axis
        if cutout_percentage is not None:
            self.options['cutoutPercentage'] = cutout_percentage
        if rotation is not None:
            self.options['rotation'] = rotation
        if circumference is not None:
            self.options['circumference'] = circumference

        self._add_dataset(params, x, y)

    doughnut = donut

    def area(self, x, y, label=None, line_color=None,
             line_width=None, line_dash=None, line_dash_offset=None,
             line_cap_style=None, line_join_style=None, line_tension=None,
             color=None, fill_color=None, fill_mode='origin',
             point_radius=0, point_style=None, display=True,
             span_gaps=False, stacked=False):
        '''
        Create an area chart

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string
            The y-axis variable name
        label : string, optional
            The label for the data set (used in the legend)
        line_color : string, optional
            CSS color value for the stroke color
        line_width : int or float, optional
            The stroke thickness
        line_dash : list-of-ints or list-of-floats, optional
            The line dash pattern
        line_dash_offset : int or float, optional
            The offset to start the line dash pattern
        line_cap_style : string, optional
            The method of drawing line endpoints: 'butt', 'round', 'square'
        line_join_style : string, optional
            The method of drawing line intersections: 'bevel', 'round', 'miter'
        line_tension : int or float, optional
            Bezier curve tension of the line
        color : string, optional
            Alias of line_color
        fill_color : string, optional
            The fill color under the line
        fill_mode : string or int or bool, optional
            Determines the method of filling.
                * int - fill to the dataset at the given index
                * numeric string (e.g., '-1', '-2', '+1') - fill to
                  relative dataset index
                * string - fill to 'start', 'end', 'origin'
                * bool - True means fill to origin, False means no fill
        point_radius : int or float, optional
            The radius of the data points
        display : bool, optional
            Should the line itself be displayed?
        span_gaps : bool, optional
            Should missing values be spanned?

        '''
        return self.line(**{k: v for k, v in locals().items() if k != 'self'})

    def line(self, x, y, label=None, line_color=None,
             line_width=None, line_dash=None, line_dash_offset=None,
             line_cap_style=None, line_join_style=None, line_tension=None,
             color=None, fill_color=None, fill_mode=False,
             point_radius=0, point_style=None, display=True,
             span_gaps=False, stacked=False):
        '''
        Create a line chart

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string
            The y-axis variable name
        label : string, optional
            The label for the data set (used in the legend)
        line_color : string, optional
            CSS color value for the stroke color
        line_width : int or float, optional
            The stroke thickness
        line_dash : list-of-ints or list-of-floats, optional
            The line dash pattern
        line_dash_offset : int or float, optional
            The offset to start the line dash pattern
        line_cap_style : string, optional
            The method of drawing line endpoints: 'butt', 'round', 'square'
        line_join_style : string, optional
            The method of drawing line intersections: 'bevel', 'round', 'miter'
        line_tension : int or float, optional
            Bezier curve tension of the line
        color : string, optional
            Alias of line_color
        fill_color : string, optional
            The fill color under the line
        fill_mode : string or int or bool, optional
            Determines the method of filling.
                * int - fill to the dataset at the given index
                * numeric string (e.g., '-1', '-2', '+1') - fill to
                  relative dataset index
                * string - fill to 'start', 'end', 'origin'
                * bool - True means fill to origin, False means no fill
        point_radius : int or float, optional
            The radius of the data points
        display : bool, optional
            Should the line itself be displayed?
        span_gaps : bool, optional
            Should missing values be spanned?

        '''

        line_param_map = dict(label='label', line_color='borderColor',
                              line_width='borderWidth', line_dash='borderDash',
                              line_dash_offset='borderDashOffset',
                              line_cap_style='borderCapStyle',
                              line_join_style='borderJoinStyle',
                              line_tension='lineTension',
                              color='*borderColor', fill_color='backgroundColor',
                              fill_mode='fill', point_radius='pointRadius',
                              point_style='pointStyle', display='showLine',
                              span_gaps='spanGaps')

        params = {line_param_map[k]: v for k, v in locals().items()
                  if k in line_param_map and v is not None}

        if '*borderColor' in params and 'borderColor' not in params:
            params['borderColor'] = params.pop('*borderColor')

        if params.get('pointStyle'):
            params['pointStyle'] = camelize(params['pointStyle'])
        if params.get('borderWidth', 1) == 0:
            params['borderWidth'] = 0.001

        params['type'] = 'line'

        if 'label' not in params:
            params['label'] = y

        if stacked:
            self.options['scales']['yAxes'][0]['stacked'] = True

        self._add_dataset(params, x, y)

    def scatter(self, x, y, label=None, color=None, fill_color=None,
                line_color=None, line_width=None, point_radius=3,
                point_style=None, display=False):
        '''
        Create a scatter chart

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string
            The y-axis variable name
        label : string, optional
            The label for the data set (used in the legend)
        color : string, optional
            CSS color value for the stroke color
        fill_color : string, optional
            The fill color for data points
        line_color : string, optional
            Alias for color
        line_width : int or float, optional
            The stroke thickness of the data points
        point_radius : int or float, optional
            The radius of the data points
        point_style : string, optional
            The point style for each data point: 'circle', 'cross', 'cross-rot',
            'dash', 'line', 'rect', 'rect-rounded', 'star', 'triangle'
        display : bool, optional
            Should the line itself be displayed?

        '''
        scatter_param_map = dict(label='label',
                                 color='*borderColor', line_color='borderColor',
                                 fill_color='backgroundColor',
                                 line_width='borderWidth',
                                 point_radius='pointRadius',
                                 point_style='pointStyle', display='showLine')

        params = {scatter_param_map[k]: v for k, v in locals().items()
                  if k in scatter_param_map and v is not None}

        if '*borderColor' in params and 'borderColor' not in params:
            params['borderColor'] = params.pop('*borderColor')

        if params.get('pointStyle'):
            params['pointStyle'] = camelize(params['pointStyle'])

        if params.get('borderWidth', 1) == 0:
            params['borderWidth'] = 0.001

        params['type'] = 'scatter'

        if 'label' not in params:
            params['label'] = y

        self._add_dataset(params, x, y)

    def bubble(self, x, y, radius, label=None, color=None, fill_color=None,
               line_color=None, line_width=None, point_style=None, display=False):
        '''
        Create a bubble chart

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string
            The y-axis variable name
        radius : string
            The variable name that contains the point radii
        label : string, optional
            The label for the data set (used in the legend)
        color : string, optional
            CSS color value for the stroke color
        fill_color : string, optional
            The fill color for data points
        line_color : string, optional
            Alias for color
        line_width : int or float, optional
            The stroke thickness of the data points
        point_radius : int or float, optional
            The radius of the data points
        point_style : string, optional
            The point style for each data point: 'circle', 'cross', 'cross-rot',
            'dash', 'line', 'rect', 'rect-rounded', 'star', 'triangle'
        display : bool, optional
            Should the line itself be displayed?

        '''

        scatter_param_map = dict(label='label', color='*borderColor',
                                 line_color='borderColor',
                                 fill_color='backgroundColor',
                                 line_width='borderWidth',
                                 point_style='pointStyle', display='showLine')

        params = {scatter_param_map[k]: v for k, v in locals().items()
                  if k in scatter_param_map and v is not None}

        if '*borderColor' in params and 'borderColor' not in params:
            params['borderColor'] = params.pop('*borderColor')

        if params.get('pointStyle'):
            params['pointStyle'] = camelize(params['pointStyle'])

        if params.get('borderWidth', 1) == 0:
            params['borderWidth'] = 0.001

        params['type'] = 'bubble'

        if 'label' not in params:
            params['label'] = y

        self._add_dataset(params, x, y, radius)

    def bar(self, x, y, label=None, displayed_labels=None,
            line_color=None, line_width=1, color=None,
            fill_color=None, border_skipped=None, bar_percentage=None,
            category_percentage=None, bar_thickness=None, max_bar_thickness=None,
            stack_id=None, orientation='vertical'):
        '''
        Create a bar chart

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string
            The y-axis variable name
        label : string, optional
            The label for the data set (used in the legend)
        displayed_labels : string
            The variable name containing labels to display in place of x values
        line_color : string, optional
            CSS color value for the stroke color
        line_width : int or float, optional
            The stroke thickness of the data points
        color : string, optional
            Alias for color
        fill_color : string, optional
            The fill color for data points
        border_skipped : string, optional
            Border to avoid drawing: 'bottom', 'top', 'left', 'right'
        bar_percentage : float, optional
            The percentage of space within the category to use
        category_percentage : float, optional
            The percentage of space to use for each category.
            ``bar_percentage`` is the percentage within this space to use
            for each bar.
        bar_thickness : int or float, optional
            The manually-set thickness of the bars
        stack_id : string, optional
            The stack identifier for the bar.  All bars using the same stack
            identifier are stacked onto each other.
        orientation : string, optional
            The orientation of the chart: 'horizontal', 'vertical'

        '''
        bar_param_map = dict(label='label', line_color='borderColor',
                             line_width='borderWidth', color='*borderColor',
                             fill_color='backgroundColor',
                             border_skipped='borderSkipped',
                             stack_id='stack', displayed_labels='_displayed_labels')

        params = {bar_param_map[k]: v for k, v in locals().items()
                  if k in bar_param_map and v is not None}

        if '*borderColor' in params and 'borderColor' not in params:
            params['borderColor'] = params.pop('*borderColor')

        horizontal = orientation.startswith('horiz')

        params['type'] = horizontal and 'horizontalBar' or 'bar'

        if params.get('borderWidth', 1) == 0:
            params['borderWidth'] = 0.001

        if 'label' not in params:
            params['label'] = y

        self._add_dataset(params, x, y)

        # These affect all bar charts on this axis
        if bar_percentage is not None:
            self.options['scales']['xAxes'][-1]['barPercentage'] = bar_percentage
            self.options['scales']['yAxes'][-1]['barPercentage'] = bar_percentage
        if category_percentage is not None:
            self.options['scales']['xAxes'][-1]['categoryPercentage'] = category_percentage
            self.options['scales']['yAxes'][-1]['categoryPercentage'] = category_percentage
        if bar_thickness is not None:
            self.options['scales']['xAxes'][-1]['barThickness'] = bar_thickness
            self.options['scales']['yAxes'][-1]['barThickness'] = bar_thickness
        if max_bar_thickness is not None:
            self.options['scales']['xAxes'][-1]['maxBarThickness'] = max_bar_thickness
            self.options['scales']['yAxes'][-1]['maxBarThickness'] = max_bar_thickness

    def hbar(self, x, y, **kwargs):
        '''
        Create a horizontal bar chart

        Parameters
        ----------
        x : string
            The x-axis variable name
        y : string
            The y-axis variable name
        label : string, optional
            The label for the data set (used in the legend)
        line_color : string, optional
            CSS color value for the stroke color
        line_width : int or float, optional
            The stroke thickness of the data points
        color : string, optional
            Alias for color
        fill_color : string, optional
            The fill color for data points
        border_skipped : string, optional
            Border to avoid drawing: 'bottom', 'top', 'left', 'right'
        bar_percentage : float, optional
            The percentage of space within the category to use
        category_percentage : float, optional
            The percentage of space to use for each category.
            ``bar_percentage`` is the percentage within this space to use
            for each bar.
        bar_thickness : int or float, optional
            The manually-set thickness of the bars
        stack_id : string, optional
            The stack identifier for the bar.  All bars using the same stack
            identifier are stacked onto each other.

        '''
        return self.bar(x, y, orientation='horizontal', **kwargs)

    def _repr_html_(self, plot_width=None, plot_height=None):
        plot_id = 'chart_%s' % str(uuid.uuid4()).replace('-', '_')
        plot_width = plot_width or self.plot_width
        plot_height = plot_height or self.plot_height
        max_data = self.max_data
        steps = self.steps

        loc_data = copy.deepcopy(self.data)
        loc_options = copy.deepcopy(self.options)

        # Use custom label generator for everything except donut/pie
        if self.type not in ['pie', 'donut']:
            loc_options['legend']['labels']['generateLabels'] = 'function(generateLabels)'

        # Set initial data
        data = {}
        data['labels'], data['displayed_labels'], data['datasets'] = \
            self._get_data(loc_data, initial=True)

        if steps <= 0:
            out = '''
                <canvas id='%(plot_id)s_canvas'
                        style='width: %(plot_width)spx;
                               height: %(plot_height)spx'></canvas>

                <script language='javascript'>
                <!--
                requirejs.config({
                    paths: {
                        Chart: ['//cdnjs.cloudflare.com/ajax/libs/Chart.js/2.7.0/Chart.min']
                    }
                });

                require(['jquery', 'Chart'], function($, Chart) {

                    var generateLabels = function( chart ) {
                        var data = chart.data;
                        var labels = [];
                        var ds_labels = [];
                        var prev_key = '';

                        if ( !Array.isArray(data.datasets) ) {
                            return labels;
                        }

                        for ( var i=0; i < data.datasets.length; i++ ) {
                            var dataset = data.datasets[i];
                            var entry = {
                                text: dataset.label,
                                fillStyle: (!Array.isArray(dataset.backgroundColor) ?
                                            dataset.backgroundColor :
                                            dataset.backgroundColor[0]),
                                hidden: !chart.isDatasetVisible(i),
                                lineCap: dataset.borderCapStyle,
                                lineDash: dataset.borderDash,
                                lineDashOffset: dataset.borderDashOffset,
                                lineJoin: dataset.borderJoinStyle,
                                lineWidth: dataset.borderWidth,
                                strokeStyle: dataset.borderColor,
                                pointStyle: dataset.pointStyle,

                                // Below is extra data used for toggling the datasets
                                datasetIndex: i
                            }

                            if ( dataset._key &&
                                 dataset._key.split('-')[0] === prev_key ) {
                                ds_labels.push(entry);
                            }
                            else {
                                ds_labels.sort(function(a, b) {
                                    if ( a.text < b.text ) return -1;
                                    if ( a.text > b.text ) return 1;
                                    return 0;
                                });
                                for ( var j=0; j < ds_labels.length; j++ ) {
                                    labels.push(ds_labels[j]);
                                }
                                prev_key = dataset._key.split('-')[0];
                                ds_labels = [entry];
                            }
                        }

                        ds_labels.sort(function(a, b) {
                            if ( a.text < b.text ) return -1;
                            if ( a.text > b.text ) return 1;
                            return 0;
                        });
                        for ( var j=0; j < ds_labels.length; j++ ) {
                            labels.push(ds_labels[j]);
                        }

                        return labels;
                    };

                    var auto_colors = function ( data ) {
                        var ctx = document.getElementById('%(plot_id)s_canvas')
                                          .getContext('2d');
                        var datasets = data.datasets;
                        var Color = Chart.helpers.color;
                        if ( !data.datasets || data.datasets.length == 0 ) {
                            return data;
                        }
                        for ( var i=0; i < datasets.length; i++ ) {
                            var ds = datasets[i];
                            if ( ds.backgroundColor ) {
                                if ( Array.isArray(ds.backgroundColor) ) {
                                    for ( var j=0; j < ds.backgroundColor.length; j++ ) {
                                        var bgcolor = ds.backgroundColor[j];
                                        if ( bgcolor.match(/^\s*auto\s*\(.+\)\s*$/) ) {
                                            bgcolor = bgcolor.replace(/^\s*auto\s*\(\s*(.+)\s*\)\s*$/, '$1');
                                            ds.backgroundColor[j] = Color(bgcolor).clearer(0.6).rgbString();
                                        }
                                        else if ( bgcolor.match(/^\s*url\s*\(.+\)\s*$/) ) {
                                            var img = new Image();
                                            img.src = bgcolor.replace(/^\s*url\s*\(\s*(.+)\s*\)\s*$/, '$1');
                                            img.onload = function () {
                                                ds.backgroundColor[j] = ctx.createPattern(img, 'repeat');
                                            };
                                        }
                                    }
                                }
                                else if ( ds.backgroundColor.match(/^\s*auto\s*\(.+\)\s*$/) ) {
                                    var bgcolor = ds.backgroundColor.replace(/^\s*auto\s*\(\s*(.+)\s*\)\s*$/, '$1');
                                    ds.backgroundColor = Color(bgcolor).clearer(0.6).rgbString();
                                }
                                else if ( ds.backgroundColor.match(/^\s*url\s*\(.+\)\s*$/) ) {
                                    var img = new Image();
                                    img.src = ds.backgroundColor.replace(/^\s*url\s*\(\s*(.+)\s*\)\s*$/, '$1');
                                    img.onload = function () {
                                        ds.backgroundColor = ctx.createPattern(img, 'repeat');
                                    };
                                }
                            }
                            if ( ds.borderColor ) {
                                if ( Array.isArray(ds.borderColor) ) {
                                    for ( var j=0; j < ds.borderColor.length; j++ ) {
                                        var fgcolor = ds.borderColor[j];
                                        if ( fgcolor.match(/^\s*auto\s*\(.+\)\s*$/) ) {
                                            fgcolor = fg.replace(/^\s*auto\s*\(\s*(.*)\s*\)\s*$/, '$1');
                                            ds.borderColor[j] = Color(fgcolor).opaquer(1).rgbString();
                                        }
                                    }
                                } else if ( ds.borderColor.match(/^\s*auto\s*\(.+\)\s*$/) ) {
                                    var fgcolor = ds.borderColor.replace(/^\s*auto\s*\(\s*(.*)\s*\)\s*$/, '$1');
                                    ds.borderColor = Color(fgcolor).opaquer(1).rgbString();
                                }
                            }
                        }
                        return data;
                    };

                    var ctx = document.getElementById('%(plot_id)s_canvas').getContext('2d');
                    var chart = new Chart(ctx, {
                        type: '%(chart_type)s',
                        data: auto_colors(%(data)s),
                        options: %(options)s
                    });
                });
                //-->
                </script>
            ''' % dict(plot_id=plot_id, plot_width=plot_width, plot_height=plot_height,
                       chart_type=self.type, data=dump_json(data),
                       options=dump_json(loc_options))

            return re.sub(r'([\'"])function\((\w+)\)\1', r'\2', out)

        # Comm setup
        state = dict(paused=False, kill=False)

        def target_func(comm, msg): # pragma: no cover
            ''' Setup comm object '''
            @comm.on_msg
            def on_msg(msg):
                ''' Handle comm messages '''
                data = msg['content']['data']
                if 'command' in data:
                    command = data['command']
                    if command == 'stop':
                        state['kill'] = True
                    elif command == 'pause':
                        state['paused'] = True
                    elif command in ['start', 'play']:
                        state['paused'] = False

            @comm.on_close
            def on_close(msg):
                ''' Close the comm '''
                state['kill'] = True

            def do_plot(_get_data, loc_data, steps, interval):
                ''' Loop and update the data as needed '''
                while steps > 0 and not state['kill']:

                    if not state['paused']:
                        labels, displayed_labels, datasets = _get_data(loc_data)
                        if datasets and datasets[0]:
                            comm.send({'labels': labels,
                                       'displayed_labels': displayed_labels,
                                       'datasets': datasets})

                    time.sleep(interval / 1000.)
                    steps -= 1

                comm.send({'command': 'stop'})

                _get_data(loc_data, terminate=True)

            def kill():
                ''' Indicate that the image drawing loop should be killed '''
                state['kill'] = True

            weakref.ref(self, kill)

            threading.Thread(target=do_plot, name=plot_id,
                             args=(self._get_data, loc_data, self.steps,
                                   self.interval)).start()

        try:
            get_ipython().kernel.comm_manager.register_target(plot_id, target_func)
        except NameError:
            warnings.warn('Streaming figures are only supported in Jupyter notebooks',
                          RuntimeWarning) 

        out = '''
            <canvas id='%(plot_id)s_canvas'
                    style='width: %(plot_width)spx; height: %(plot_height)spx'></canvas>

            <style type="text/css">
            .stream-plot-controls {
                border: 1px solid #e0e0e0;
                border-radius: 5px;
                padding: 8px;
                display: inline-block;
            }
            .stream-plot-control {
                color: #909090;
                font-size: 150%%;
                margin-right: 1em;
                margin-left: 1em;
            }
            .fa-play.stream-plot-control:not([class*='enabled']):not([class*='disabled']):hover {
                text-shadow: 0px 0px 15px #00c000;
                color: #00c000;
            }
            .fa-pause.stream-plot-control:not([class*='enabled']):not([class*='disabled']):hover {
                text-shadow: 0px 0px 15px #fdeb61;
                color: #d37a2d;
            }
            .fa-stop.stream-plot-control:not([class*='enabled']):not([class*='disabled']):hover {
                text-shadow: 0px 0px 15px #ff0000;
                color: #ff0000;
            }
            .fa-play.stream-plot-control.enabled { color: #00c000; }
            .fa-pause.stream-plot-control.enabled { color: #d37a2d; }
            .fa-stop.stream-plot-control.enabled { color: #ff0000; }
            .stream-plot-control.disabled { color: #d0d0d0; }
            </style>

            <div id="streaming-plot-%(plot_id)s"
                 style="text-align: center; padding-top:10px; width: %(plot_width)spx">
            <div class="stream-plot-controls" id="stream-plot-%(plot_id)s">
              <i class="fa fa-play enabled stream-plot-control"
                 title="Stream data into figure" aria-hidden="true"
                 style="cursor:pointer" id="%(plot_id)s-play"></i>
              <i class="fa fa-pause stream-plot-control"
                 title="Pause data updates" aria-hidden="true"
                 style="cursor:pointer" id="%(plot_id)s-pause"></i>
              <i class="fa fa-stop stream-plot-control"
                 title="Shutdown data stream" aria-hidden="true"
                 style="cursor:pointer" id="%(plot_id)s-stop"></i>
            </div>
            </div>

            <script language='javascript'>
            <!--
            requirejs.config({
                paths: {
                    Chart: ['//cdnjs.cloudflare.com/ajax/libs/Chart.js/2.7.0/Chart.min']
                }
            });

            require(['jquery', 'Chart'], function($, Chart) {

                var afterBuildTicks = function ( scale ) {
                    return;
                    scale.ticks.shift();
                    scale.ticks.pop();
                };

                var generateLabels = function( chart ) {
                    var data = chart.data;
                    var labels = [];
                    var ds_labels = [];
                    var prev_key = '';

                    if ( !Array.isArray(data.datasets) ) {
                        return labels;
                    }

                    for ( var i=0; i < data.datasets.length; i++ ) {
                        var dataset = data.datasets[i];
                        var entry = {
                            text: dataset.label,
                            fillStyle: (!Array.isArray(dataset.backgroundColor) ?
                                        dataset.backgroundColor :
                                        dataset.backgroundColor[0]),
                            hidden: !chart.isDatasetVisible(i),
                            lineCap: dataset.borderCapStyle,
                            lineDash: dataset.borderDash,
                            lineDashOffset: dataset.borderDashOffset,
                            lineJoin: dataset.borderJoinStyle,
                            lineWidth: dataset.borderWidth,
                            strokeStyle: dataset.borderColor,
                            pointStyle: dataset.pointStyle,

                            // Below is extra data used for toggling the datasets
                            datasetIndex: i
                        }

                        if ( dataset._key && dataset._key.split('-')[0] === prev_key ) {
                            ds_labels.push(entry);
                        }
                        else {
                            ds_labels.sort(function(a, b) {
                                if ( a.text < b.text ) return -1;
                                if ( a.text > b.text ) return 1;
                                return 0;
                            });
                            for ( var j=0; j < ds_labels.length; j++ ) {
                                labels.push(ds_labels[j]);
                            }
                            prev_key = dataset._key.split('-')[0];
                            ds_labels = [entry];
                        }
                    }

                    ds_labels.sort(function(a, b) {
                        if ( a.text < b.text ) return -1;
                        if ( a.text > b.text ) return 1;
                        return 0;
                    });
                    for ( var j=0; j < ds_labels.length; j++ ) {
                        labels.push(ds_labels[j]);
                    }

                    return labels;
                };

                var auto_colors = function ( data ) {
                    var ctx = document.getElementById('%(plot_id)s_canvas').getContext('2d');
                    var datasets = data.datasets;
                    var Color = Chart.helpers.color;
                    if ( !data.datasets || data.datasets.length == 0 ) {
                        return data;
                    }
                    for ( var i=0; i < datasets.length; i++ ) {
                        var ds = datasets[i];
                        if ( ds.backgroundColor ) {
                            if ( Array.isArray(ds.backgroundColor) ) {
                                for ( var j=0; j < ds.backgroundColor.length; j++ ) {
                                    var bgcolor = ds.backgroundColor[j];
                                    if ( bgcolor.match(/^\s*auto\s*\(.+\)\s*$/) ) {
                                        bgcolor = bgcolor.replace(/^\s*auto\s*\(\s*(.+)\s*\)\s*$/, '$1');
                                        ds.backgroundColor[j] = Color(bgcolor).clearer(0.6).rgbString();
                                    }
                                    else if ( bgcolor.match(/^\s*url\s*\(.+\)\s*$/) ) {
                                        var img = new Image();
                                        img.src = bgcolor.replace(/^\s*url\s*\(\s*(.+)\s*\)\s*$/, '$1');
                                        img.onload = function () {
                                            ds.backgroundColor[j] = ctx.createPattern(img, 'repeat');
                                        };
                                    }
                                }
                            }
                            else if ( ds.backgroundColor.match(/^\s*auto\s*\(.+\)\s*$/) ) {
                                var bgcolor = ds.backgroundColor.replace(/^\s*auto\s*\(\s*(.+)\s*\)\s*$/, '$1');
                                ds.backgroundColor = Color(bgcolor).clearer(0.6).rgbString();
                            }
                            else if ( ds.backgroundColor.match(/^\s*url\s*\(.+\)\s*$/) ) {
                                var img = new Image();
                                img.src = ds.backgroundColor.replace(/^\s*url\s*\(\s*(.+)\s*\)\s*$/, '$1');
                                img.onload = function () {
                                    ds.backgroundColor = ctx.createPattern(img, 'repeat');
                                };
                            }
                        }
                        if ( ds.borderColor ) {
                            if ( Array.isArray(ds.borderColor) ) {
                                for ( var j=0; j < ds.borderColor.length; j++ ) {
                                    var fgcolor = ds.borderColor[j];
                                    if ( fgcolor.match(/^\s*auto\s*\(.+\)\s*$/) ) {
                                        fgcolor = fg.replace(/^\s*auto\s*\(\s*(.*)\s*\)\s*$/, '$1');
                                        ds.borderColor[j] = Color(fgcolor).opaquer(1).rgbString();
                                    }
                                }
                            } else if ( ds.borderColor.match(/^\s*auto\s*\(.+\)\s*$/) ) {
                                var fgcolor = ds.borderColor.replace(/^\s*auto\s*\(\s*(.*)\s*\)\s*$/, '$1');
                                ds.borderColor = Color(fgcolor).opaquer(1).rgbString();
                            }
                        }
                    }
                    return data;
                };

                var max_data = %(max_data)s;
                var ctx = document.getElementById('%(plot_id)s_canvas').getContext('2d');
                var chart = new Chart(ctx, {
                    type: '%(chart_type)s',
                    data: auto_colors(%(data)s),
                    options: %(options)s
                });

                var set_display_labels = function (value, index, values) {
                    if ( chart.data.displayed_labels && chart.data.displayed_labels[index] ) {
                        return chart.data.displayed_labels[index];
                    }
                    return value;
                };

                if ( chart.options.scales && chart.options.scales.xAxes ) {
                    chart.options.scales.xAxes[0].ticks.callback = set_display_labels;
                }

/*
                var dynamic_x_axis = false;
                if ( chart.config.type === 'line' || chart.config.type === 'area' ||
                     chart.config.type === 'scatter' || chart.config.type === 'bubble' ) {
                    if ( chart.options.scales.xAxes[0].ticks.min == null &&
                         chart.options.scales.xAxes[0].ticks.max == null ) {
                        dynamic_x_axis = true;
                    }
                }

                if ( dynamic_x_axis && chart.options.scales && chart.options.scales.xAxes ) {
                    chart.options.scales.xAxes[0].afterBuildTicks = afterBuildTicks;
                }
*/

                var rotate_linear = function (labels, values, new_values) {
                    Array.prototype.push.apply(values, new_values);
                    while ( values.length > max_data ) {
                        values.shift();
                    }
                    while ( values.length < labels.length ) {
                        values.unshift(null);
                    }
                }

                var rotate_category = function (labels, new_labels, values, new_values) {
                    var idx;

                    if ( !new_labels || new_labels.length == 0 ) return;
                    if ( !new_values || new_values.length == 0 ) return;

                    for ( var i=0; i < new_labels.length; i++ ) {
                        idx = labels.indexOf(new_labels[i]);
                        if ( idx === -1 ) {
                            labels.push(new_labels[i]);
                            values.push(new_values[i]);
                        } else {
                            values[idx] = new_values[i];
                        }
                    }

                    while ( values.length > max_data ) {
                        values.shift();
                    }

                    while ( values.length < labels.length ) {
                        values.unshift(null);
                    }
                }

                var comm = Jupyter.notebook.kernel.comm_manager.new_comm('%(plot_id)s')

                comm.on_msg(function(msg) {
                    var data = auto_colors(msg.content.data);
                    var key = null;
                    var old_datasets = {};
                    var new_datasets = {};
                    var updated_datasets = {};

                    if ( data.labels && data.datasets &&
                         data.labels.length > 0 && data.datasets.length > 0 ) {

                        // Categorize updatable and new datasets
                        for ( var i=0; i < chart.data.datasets.length; i++ ) {
                            old_datasets[chart.data.datasets[i]._key] = chart.data.datasets[i];
                        }
                        for ( var i=0; i < data.datasets.length; i++ ) {
                            if ( old_datasets[data.datasets[i]._key] == null ) {
                                new_datasets[data.datasets[i]._key] = data.datasets[i];
                            } else {
                                updated_datasets[data.datasets[i]._key] = data.datasets[i];
                            }
                        }

                        if ( chart.config.type == 'doughnut' ||
                             chart.config.type == 'pie' ||
                             chart.config.type == 'bar' ||
                             chart.config.type == 'horizontalBar' ) {

                            // Update existing datasets
                            for ( key in updated_datasets ) {
                                rotate_category( chart.data.labels,
                                                   data.labels,
                                                   old_datasets[key].data,
                                                   updated_datasets[key].data );
                            }

                            // Add new datasets
                            for ( key in new_datasets ) {
                                chart.data.datasets.push(new_datasets[key]);
                                rotate_category( chart.data.labels,
                                                   data.labels,
                                                   new_datasets[key].data,
                                                   new_datasets[key].data );
                            }

                            chart.data.displayed_labels = data.displayed_labels;

                        } else {

                            // Rotate labels
                            Array.prototype.push.apply(chart.data.labels, data.labels);
                            while ( chart.data.labels.length > max_data ) {
                                chart.data.labels.shift();
                            }

                            // Update existing datasets
                            for ( key in updated_datasets ) {
                                rotate_linear( chart.data.labels,
                                               old_datasets[key].data,
                                               updated_datasets[key].data );
                            }

                            // Add new datasets
                            for ( key in new_datasets ) {
                                chart.data.datasets.push(new_datasets[key]);
                                rotate_linear( chart.data.labels,
                                               new_datasets[key].data,
                                               [] );
                            }

                            // Shift stale datasets
                            var nulls = [];
                            for ( key in old_datasets ) {
                                if ( updated_datasets[key] == null ) {
                                    if ( nulls.length == 0 ) {
                                        for ( var i=0; i < data.labels.length; i++ ) {
                                            nulls.push(null);
                                        }
                                    }
                                    rotate_linear( chart.data.labels,
                                                   old_datasets[key].data,
                                                   nulls );
                                }
                            }

                            // Reset x-axis limits
/*
                            if ( dynamic_x_axis && chart.options.scales && chart.options.scales.xAxes ) {
                                chart.options.scales.xAxes[0].ticks.min = chart.data.labels[Math.max(Math.min(chart.data.labels.length-1, 2), 0)];
                                chart.options.scales.xAxes[0].ticks.max = chart.data.labels[Math.max(chart.data.labels.length-1, 0)];
                            }
*/
                        }

                        if ( chart.config.type == 'line' ) {
                            chart.update(0);
                        } else {
                            chart.update(%(interval)s * 2);
                        }
                    }

                    if ( data.command ) {
                        if ( data.command == 'stop' ) {
                            $('#%(plot_id)s-play, #%(plot_id)s-pause, #%(plot_id)s-stop')
                                .removeClass('enabled')
                                .addClass('disabled')
                                .css('cursor', 'default');
                            $('#streaming-plot-%(plot_id)s').hide().remove();
                        }
                    }

                    // If plot doesn't exist anymore, stop data
                    if ( $('#streaming-plot-%(plot_id)s').length == 0 ) {
                        comm.send({'command': 'stop'});
                    }
                });

                comm.on_close(function() {
                    comm.send({'command': 'stop'});
                });

                // Send message when plot is removed
                $.event.special.destroyed = {
                    remove: function(o) {
                      if (o.handler) {
                        o.handler()
                      }
                    }
                }
                $('#streaming-plot-%(plot_id)s').bind('destroyed', function() {
                    comm.send({'command': 'stop'});
                });

                var blink = function () {
                  $('#%(plot_id)s-pause.enabled').fadeOut(900).fadeIn(900, blink);
                }

                $('#%(plot_id)s-play').click(function() {
                  if ( $('#%(plot_id)s-play').hasClass('disabled') ) return;
                  if ( $('#%(plot_id)s-play').hasClass('enabled') ) return;
                  $('#%(plot_id)s-pause').removeClass('enabled');
                  $('#%(plot_id)s-play').addClass('enabled');
                  comm.send({'command': 'play'});
                });

                $('#%(plot_id)s-pause').click(function() {
                  if ( $('#%(plot_id)s-pause').hasClass('disabled') ) return;
                  if ( $('#%(plot_id)s-pause').hasClass('enabled') ) {
                    $('#%(plot_id)s-play').click();
                    return;
                  }
                  $('#%(plot_id)s-play').removeClass('enabled');
                  $('#%(plot_id)s-pause').addClass('enabled');
                  comm.send({'command': 'pause'});
                  blink();
                });

                $('#%(plot_id)s-stop').click(function() {
                  if ( $('#%(plot_id)s-stop').hasClass('disabled') ) return;
                    $('#%(plot_id)s-play, #%(plot_id)s-pause, #%(plot_id)s-stop')
                    .removeClass('enabled')
                    .addClass('disabled')
                    .css('cursor', 'default');
                  comm.send({'command': 'stop'});
                  $('#streaming-plot-%(plot_id)s').hide().remove();
                });

            });
            //-->
            </script>
        ''' % dict(plot_id=plot_id, steps=steps, interval=self.interval,
                   plot_width=plot_width, plot_height=plot_height,
                   chart_type=self.type, data=dump_json(data),
                   options=dump_json(self.options), max_data=max_data)

        return re.sub(r'([\'"])function\((\w+)\)\1', r'\2', out)


class StreamingImages(object):
    '''
    Streaming Images

    Attributes
    ----------
    data_callback : callable
        The function to call for data updates
    interval : int, optional
        The length of each step in milliseconds
    steps : int, optional
        The maximum number of steps to iterate

    Parameters
    ----------
    data_callback : callable
        The function to call for data updates
    image_key : string, optional
        The key for the image data in the callback result.  If a key value
        is not set, but there is only one key in the data, the data at that
        key will be used.
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
    plot_width : int, optional
        The width of the plot in pixels
    plot_height : int, optional
        The height of the plot in pixels
    annotations : Window, optional
        Window containing image highlighting information
    transformers : function or list-of-functions, optional
        Functions to apply to each image prior to displaying

    Returns
    -------
    :class:`StreamingImages`

    '''

    def __init__(self, data_callback, image_key=None, steps=1e5, interval=1000,
                 size=None, plot_width=900, plot_height=400, annotations=None,
                 transformers=None, **kwargs):
        self.data_callback = data_callback
        self._image_key = image_key
        self.plot_width = plot_width
        self.plot_height = plot_height
        self.size = size
        self.steps = steps
        self.interval = interval
        self.annotations = annotations
        self.transformers = listify(transformers)

    def _repr_html_(self, plot_height=None, plot_width=None):
        plot_id = 'image_comm_%s' % str(uuid.uuid4()).replace('-', '_')

        state = dict(paused=False, kill=False, image_key=self._image_key)

        def target_func(comm, msg): # pragma: no cover
            ''' Setup comm object '''
            from PIL import Image

            @comm.on_msg
            def on_msg(msg):
                ''' Handle comm messages '''
                data = msg['content']['data']
                if 'command' in data:
                    command = data['command']
                    if command == 'stop':
                        state['kill'] = True
                    elif command == 'pause':
                        state['paused'] = True
                    elif command in ['start', 'play']:
                        state['paused'] = False

            @comm.on_close
            def on_close(msg):
                ''' Close the comm '''
                state['kill'] = True

            def do_plot(data_callback, steps, interval, annotations, transformers):
                ''' Loop and update the image as needed '''
                from .windows import BaseWindow

                previous = None
                initial = True

                while steps > 0 and not state['kill']:

                    if not state['paused']:
                        data = data_callback(initial=initial, max_data=1)
                        initial = False
                        if state['image_key'] is None and len(data.keys()) == 1:
                            state['image_key'] = list(data.keys())[0]
                        if state['image_key'] in data:
                            data = data[state['image_key']]
                            if len(data):
                                modified = False
                                data = data[-1]
                                if Image.isImageType(data):
                                    img = data
                                else:
                                    img = Image.open(io.BytesIO(data))
                                img_format = img.format

                                if transformers is not None and transformers:
                                    for trans in transformers:
                                        img = trans(img) 
                                        modified = True

                                if annotations is None:
                                    pass
                                elif isinstance(annotations, BaseWindow):
                                    if ('_nObjects_' in annotations.columns and
                                        '_Object0_' in annotations.columns):
                                        img = highlight_image(img, annotations)
                                        modified = True

                                if modified:
                                    data = io.BytesIO()
                                    img.save(data, format=img_format)
                                    data = data.getvalue()

                                data = b'data:image/' + img_format.lower().encode('utf-8') + \
                                       b';base64,' + base64.b64encode(data)
                                if previous is None or previous != data:
                                    random = id(object())
                                    comm.send({'url': data,
                                               'id': random,
                                               'format': img_format,
                                               'length': len(data),
                                               'size': list(img.size)})
                                    previous = data

                    time.sleep(interval / 1000.)
                    steps -= 1

                comm.send({'command': 'stop'})

                data_callback(terminate=True)

            def kill():
                ''' Indicate that the image drawing loop should be killed '''
                state['kill'] = True

            weakref.ref(self, kill)

            threading.Thread(target=do_plot, name=plot_id,
                             args=(self.data_callback,
                                   self.steps, self.interval,
                                   self.annotations, self.transformers)).start()

        try:
            get_ipython().kernel.comm_manager.register_target(plot_id, target_func)
        except NameError:
            warnings.warn('Streaming images are only supported in Jupyter notebooks',
                          RuntimeWarning) 

        styles = []
        styles.append('width: %spx' % (plot_width or self.plot_width))
        styles.append('height: %spx' % (plot_height or self.plot_height))
        styles.append('text-align: right')
        styles.append('background-position: center')
        styles.append('background-repeat: no-repeat')
        if self.size is None:
            pass
        elif isinstance(self.size, six.string_types):
            styles.append('background-size: %s' % self.size)
        elif isinstance(self.size, (int, float)):
            styles.append('background-size: %dpx %dpx' % (self.size, self.size))
        elif isinstance(self.size, (list, tuple)):
            styles.append('background-size: %dpx %dpx' % (self.size[0], self.size[1]))
        styles = '; '.join(styles)

        out = '''
            <div style='position: relative; top: 0; left: 0;
                        height: %(plot_height)spx; width: %(plot_width)spx'>
                <div id='%(plot_id)s_output_bg' style='%(styles)s;
                         position: absolute; top: 0; left: 0'></div>
                <div id='%(plot_id)s_output' style='%(styles)s;
                         position: absolute; top: 0; left: 0'></div>
            </div>

            <style type="text/css">
            #%(plot_id)s_output {
                color: black;
                text-shadow: #fff 0px 0px 1px;
                -webkit-font-smoothing: antialiased;
            }
            .stream-plot-controls {
                border: 1px solid #e0e0e0;
                border-radius: 5px;
                padding: 8px;
                display: inline-block;
            }
            .stream-plot-control {
                color: #909090;
                font-size: 150%%;
                margin-right: 1em;
                margin-left: 1em;
            }
            .fa-play.stream-plot-control:not([class*='enabled']):not([class*='disabled']):hover {
                text-shadow: 0px 0px 15px #00c000;
                color: #00c000;
            }
            .fa-pause.stream-plot-control:not([class*='enabled']):not([class*='disabled']):hover {
                text-shadow: 0px 0px 15px #fdeb61;
                color: #d37a2d;
            }
            .fa-stop.stream-plot-control:not([class*='enabled']):not([class*='disabled']):hover {
                text-shadow: 0px 0px 15px #ff0000;
                color: #ff0000;
            }
            .fa-play.stream-plot-control.enabled { color: #00c000; }
            .fa-pause.stream-plot-control.enabled { color: #d37a2d; }
            .fa-stop.stream-plot-control.enabled { color: #ff0000; }
            .stream-plot-control.disabled { color: #d0d0d0; }
            .stream-image-label { background-color: rgba(255, 255, 255, 0.5) }
            </style>

            <div id="streaming-plot-%(plot_id)s"
                 style="text-align: center; padding-top:10px; width: %(plot_width)spx">
            <div class="stream-plot-controls" id="stream-plot-%(plot_id)s">
              <i class="fa fa-play enabled stream-plot-control"
                 title="Stream data into figure" aria-hidden="true"
                 style="cursor:pointer" id="%(plot_id)s-play"></i>
              <i class="fa fa-pause stream-plot-control"
                 title="Pause data updates" aria-hidden="true"
                 style="cursor:pointer" id="%(plot_id)s-pause"></i>
              <i class="fa fa-stop stream-plot-control"
                 title="Shutdown data stream" aria-hidden="true"
                 style="cursor:pointer" id="%(plot_id)s-stop"></i>
            </div>
            </div>

            <script language='javascript'>
            <!--
            require(['jquery'], function($) {
                var %(plot_id)s = Jupyter.notebook.kernel.comm_manager.new_comm('%(plot_id)s')

                %(plot_id)s.on_msg(function(msg) {
                    var data = msg.content.data;
                    if ( data.url ) {
                        if ( !data.url.startsWith('data:') ) {
                            data.url = atob(data.url);
                        }
                        var preload_img = new Image();
                        preload_img.onload = function () {
                            $('#%(plot_id)s_output_bg').css('background-image',
                                                            'url(' + data.url + ')');
                            setTimeout(function () {
                                $('#%(plot_id)s_output').css('background-image',
                                                             'url(' + data.url + ')')
                                                             .html('<span class="stream-image-label">' +
                                                                   data.format +
                                                                   ' (' + data.size[0] + 'px, ' +
                                                                          data.size[1] + 'px)' +
                                                                   '</span>');
                            }, 15);
                        }
                        preload_img.src = data.url;
                    }

                    if ( data.command ) {
                        if ( data.command == 'stop' ) {
                            $('#%(plot_id)s-play, #%(plot_id)s-pause, #%(plot_id)s-stop')
                                .removeClass('enabled')
                                .addClass('disabled')
                                .css('cursor', 'default');
                            $('#streaming-plot-%(plot_id)s').hide().remove();
                        }
                    }

                    // If plot doesn't exist anymore, stop data
                    if ( $('#streaming-plot-%(plot_id)s').length == 0 ) {
                        %(plot_id)s.send({'command': 'stop'});
                    }
                });

                %(plot_id)s.on_close(function() {
                    %(plot_id)s.send({'command': 'stop'});
                });

                // Send message when plot is removed
                $.event.special.destroyed = {
                    remove: function(o) {
                      if (o.handler) {
                        o.handler()
                      }
                    }
                }
                $('#streaming-plot-%(plot_id)s').bind('destroyed', function() {
                    %(plot_id)s.send({'command': 'stop'});
                });

                var blink = function () {
                  $('#%(plot_id)s-pause.enabled').fadeOut(900).fadeIn(900, blink);
                }

                $('#%(plot_id)s-play').click(function() {
                  if ( $('#%(plot_id)s-play').hasClass('disabled') ) return;
                  if ( $('#%(plot_id)s-play').hasClass('enabled') ) return;
                  $('#%(plot_id)s-pause').removeClass('enabled');
                  $('#%(plot_id)s-play').addClass('enabled');
                  %(plot_id)s.send({'command': 'play'});
                });

                $('#%(plot_id)s-pause').click(function() {
                  if ( $('#%(plot_id)s-pause').hasClass('disabled') ) return;
                  if ( $('#%(plot_id)s-pause').hasClass('enabled') ) {
                    $('#%(plot_id)s-play').click();
                    return;
                  }
                  $('#%(plot_id)s-play').removeClass('enabled');
                  $('#%(plot_id)s-pause').addClass('enabled');
                  %(plot_id)s.send({'command': 'pause'});
                  blink();
                });

                $('#%(plot_id)s-stop').click(function() {
                  if ( $('#%(plot_id)s-stop').hasClass('disabled') ) return;
                    $('#%(plot_id)s-play, #%(plot_id)s-pause, #%(plot_id)s-stop')
                    .removeClass('enabled')
                    .addClass('disabled')
                    .css('cursor', 'default');
                  %(plot_id)s.send({'command': 'stop'});
                  $('#streaming-plot-%(plot_id)s').hide().remove();
                });

            });
            //-->
            </script>
        ''' % dict(plot_id=plot_id, styles=styles,
                   plot_width=plot_width or self.plot_width,
                   plot_height=plot_height or self.plot_height)

        return out


class ChartLayout(object):
    '''
    Create a layout of charts / images

    The layout of chart and image components is specified in the positional
    arguments of the constructor.  Each positional argument becomes a row
    in the layout.  If the argument is a tuple, each item in the tuple becomes
    a cell in that row.

    Parameters
    ----------
    *layout : one-or-more tuple or StreamingImage or StreamingChart
        Components of the layout
    width : int, optional
        The pixel width of the layout
    height : int, optional
        The pixel height of the layout

    Examples
    --------
    The following code will create a layout of two line charts on the first
    row, a streaming image in the second row, and two streaming scatter
    plots on the third row.

    >>> layout = ChartLayout(
    ...              (win.streaming_line('x', 'y'),
    ...               win.streaming_line('x', 'z')),
    ...              win.streaming_images('img'),
    ...              (win2.streaming_scatter('a', 'b'),
    ...               win2.streaming_scatter('a', 'c'))
    ...          )
    >>> layout

    Returns
    -------
    :class:`ChartLayout`

    '''

    def __init__(self, *layout, **kwargs):
        layout = list(layout)
        for i, item in enumerate(layout):
            if not isinstance(item, (list, tuple)):
                layout[i] = [layout[i]]
        self.layout = layout
        self.width = int(kwargs.get('width', 900))
        self.height = int(kwargs.get('height', 300))

    def _repr_html_(self):
        layout_id = 'chart_layout_%s' % str(uuid.uuid4()).replace('-', '_')

        out = []

        out.append('<div id="%s">' % layout_id)

        out.append('''
            <style>
            #%s .stream-plot-controls { display: none }
            </style>
        ''' % layout_id)

        for row in self.layout:
            out.append(('<table style="width: %dpx; height: %dpx; '
                        'border-style: none">') % (self.width, self.height))
            out.append('<tr style="border-style: none">')
            for cell in row:
                cellwidth = int(self.width / len(row))
                cellheight = self.height
                out.append('<td style="width:%dpx; border-style: none">%s</td>' %
                           (cellwidth, cell._repr_html_(plot_width=cellwidth,
                                                        plot_height=cellheight)))
            out.append('</tr>')
            out.append('</table>')

        out.append('</div>')

        out.append('''
            <div id="streaming-layout-%(layout_id)s"
                 style="text-align: center; padding-top:10px; width: %(layout_width)spx">
                <div class="stream-plot-controls" id="stream-layout-%(layout_id)s">
                  <i class="fa fa-play enabled stream-plot-control"
                     title="Stream data into figure" aria-hidden="true"
                     style="cursor:pointer" id="%(layout_id)s-play"></i>
                  <i class="fa fa-pause stream-plot-control"
                     title="Pause data updates" aria-hidden="true"
                     style="cursor:pointer" id="%(layout_id)s-pause"></i>
                  <i class="fa fa-stop stream-plot-control"
                     title="Shutdown data stream" aria-hidden="true"
                     style="cursor:pointer" id="%(layout_id)s-stop"></i>
                </div>
            </div>

            <script>
            <!--
            require(['jquery'], function($) {
                var blink = function () {
                  $('#%(layout_id)s-pause.enabled').fadeOut(900).fadeIn(900, blink);
                }

                $('#%(layout_id)s-play').click(function() {
                    if ( $('#%(layout_id)s-play').hasClass('disabled') ) return;
                    if ( $('#%(layout_id)s-play').hasClass('enabled') ) return;
                    $('#%(layout_id)s-pause').removeClass('enabled');
                    $('#%(layout_id)s-play').addClass('enabled');
                    $('#%(layout_id)s .stream-plot-control[id$="-play"]').click();
                });

                $('#%(layout_id)s-pause').click(function() {
                    if ( $('#%(layout_id)s-pause').hasClass('disabled') ) return;
                    if ( $('#%(layout_id)s-pause').hasClass('enabled') ) {
                        $('#%(layout_id)s-play').click();
                        return;
                    }
                    $('#%(layout_id)s-play').removeClass('enabled');
                    $('#%(layout_id)s-pause').addClass('enabled');
                    $('#%(layout_id)s .stream-plot-control[id$="-pause"]').click();
                    blink();
                });

                $('#%(layout_id)s-stop').click(function() {
                    if ( $('#%(layout_id)s-stop').hasClass('disabled') ) return;
                        $('#%(layout_id)s-play, #%(layout_id)s-pause, #%(layout_id)s-stop')
                        .removeClass('enabled')
                        .addClass('disabled')
                        .css('cursor', 'default');
                    $('#%(layout_id)s .stream-plot-control[id$="-stop"]').click();
                    $('#streaming-layout-%(layout_id)s').hide().remove();
                });
            });
            //-->
            </script>
        ''' % dict(layout_id=layout_id, layout_width=self.width, layout_height=self.height))

        return '\n'.join(out)
