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

'''
Notebook utiilties for ESP

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import functools
import re
import six
from ..config import get_option


def _scale_value(match, scale=None):
    ''' Scale dimension '''
    if scale is None:
        scale = get_option('display.image_scale')
    value = match.group(2)
    dim = float(re.search(r'^[+-]?(\d*\.?\d*)', value).group(1)) * scale
    unit = re.search(r'([A-Za-z]+)$', value).group(1)
    return '%s%s%s%s' % (match.group(1), dim, unit, match.group(3))


def _scale_attrs(match, scale=None):
    ''' Scale width / height attributes '''
    if scale is None:
        scale = get_option('display.image_scale')
    attrs = match.group(2)
    attrs = re.sub('(width=[\'"])([^\'"]+)([\'"])',
                   functools.partial(_scale_value, scale=scale), attrs)
    attrs = re.sub('(height=[\'"])([^\'"]+)([\'"])',
                   functools.partial(_scale_value, scale=scale), attrs)
    return '%s%s%s' % (match.group(1), attrs, match.group(3))


def scale_svg(data, scale=None):
    ''' Scale width and height of SVG according to configuration '''
    if scale is None:
        scale = get_option('display.image_scale')
    return re.sub(r'(<svg\s+)([^>]+)(>)',
                  functools.partial(_scale_attrs, scale=scale), data)
