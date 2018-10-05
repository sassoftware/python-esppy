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

''' Window Transformers '''

from __future__ import print_function, division, absolute_import, unicode_literals

import io
import numpy as np
import pandas as pd
import re
import six
from PIL import Image


def _bgr2rgb(pil_image):
    return Image.fromarray(np.asarray(pil_image)[:,:,::-1])


def bgr2rgb(data, columns=None):
    '''
    Convert BGR images to RGB

    Parameters
    ----------
    data : PIL.Image or DataFrame
        The image data
    columns : string or list-of-strings, optional
        If `data` is a DataFrame, this is the list of columns that 
        contain image data.

    Returns
    -------
    :class:`PIL.Image`
        If `data` is a :class:`PIL.Image` 
    :class:`pandas.DataFrame`
        If `data` is a :class:`pandas.DataFrame`

    '''
    if hasattr(data, 'columns'):
        if len(data):
            if not columns:
                columns = list(data.columns)
            elif isinstance(columns, six.string_types):
                columns = [columns]
            for col in columns:
                if Image.isImageType(data[col].iloc[0]):
                    data[col] = data[col].apply(_bgr2rgb)
        return data

    elif Image.isImageType(data):
        return _bgr2rgb(data)

    return data


def rgb2bgr(data, columns=None):
    '''
    Convert RGB images to BGR

    Parameters
    ----------
    data : PIL.Image or DataFrame
        The image data
    columns : string or list-of-strings, optional
        If `data` is a DataFrame, this is the list of columns that
        contain image data.

    Returns
    -------
    :class:`PIL.Image`
        If `data` is a :class:`PIL.Image`
    :class:`pandas.DataFrame`
        If `data` is a :class:`pandas.DataFrame`

    '''
    return bgr2rgb(data, columns=columns)


def _bytes2image(data):
    return Image.open(io.BytesIO(data))


def bytes2image(data, columns=None):
    '''
    Convert bytes to PIL.Image objects

    Parameters
    ----------
    data : PIL.Image or DataFrame
        The image data
    columns : string or list-of-strings, optional
        If `data` is a DataFrame, this is the list of columns that
        contain image data.

    Returns
    -------
    :class:`PIL.Image`
        If `data` is a :class:`PIL.Image`
    :class:`pandas.DataFrame`
        If `data` is a :class:`pandas.DataFrame`

    '''
    if hasattr(data, 'columns'):
        if len(data):
            if not columns:
                columns = list(data.columns)
            elif isinstance(columns, six.string_types):
                columns = [columns]
            for col in columns:
                if isinstance(data[col].iloc[0], bytes): 
                    data[col] = data[col].apply(_bytes2image)
        return data

    elif isinstance(data, six.binary_type):
        return _bytes2image(data)

    return data
