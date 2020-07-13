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

''' Data Loading Utilities '''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
import datetime
import decimal
import numpy as np
import os
import re
import io
import xml.etree.ElementTree as ET
import six
from six.moves import urllib
from . import xml
from .rest import get_params
from ..base import ESPObject

EPOCH = datetime.datetime(1970, 1, 1)


def gen_name(prefix='', suffix=''):
    '''
    Generate simple "random" name

    Parameters
    ----------
    prefix : string, optional
        Prefix of the name
    suffix : string, optional
        Suffix of the name

    Returns
    -------
    string

    '''
    out = ''
    numbers = '0123456789'
    letters = 'abcdefghijklmnopqrstuvwxyz'
    chars = numbers + letters
    value = id(eval('{}'))

    value, i = divmod(value, len(letters))
    out = letters[i] + out

    while value:
        value, i = divmod(value, len(chars))
        out = chars[i] + out

    return '%s%s%s' % (prefix, ''.join(reversed(out)), suffix)


def get_project_data(project):
    '''
    Retrieve project data using various methods

    This function attempts to retrieve the XML project definiton
    using various methods.  If ``project`` is a file-like object, a file
    path or a URL, the contents will be returned.  If it is XML, it will
    be returned as-is.  If it is a URL, it will

    Parameters
    ----------
    project : file-like or string or ElementTree.Element
        The data itself or a path to it

    Returns
    -------
    XML string

    '''
    if isinstance(project, ESPObject):
        data = project.to_xml()
    elif hasattr(project, 'read'):
        data = project.read()
    elif isinstance(project, six.string_types):
        if re.match(r'^\s*<', project):
            data = project
        elif os.path.isfile(project):
            data = io.open(project, mode="r",encoding="utf-8").read()
        else:
            data = urllib.request.urlopen(project).read().decode('utf-8')
    elif isinstance(project, ET.Element):
        data = xml.to_xml(project)
    else:
        raise TypeError('Unknown type for project: %s' % project)
    return data


def get_server_info(obj):
    '''
    Retrieve information about the server

    Parameters
    ----------
    obj : ESPObject
        The object that contains the server session

    Returns
    -------
    dict

    '''
    res = obj._get(urllib.parse.urljoin(obj.base_url, 'server'),
                   params=get_params(config=True))
    out = {}

    for key, value in res.attrib.items():
        out[key] = value

    for item in res.findall('./property'):
        out[item.attrib['name']] = item.text

    for key, value in out.items():
        if isinstance(value, six.string_types):
            if value == 'true':
                value = True
            elif value == 'false':
                value = False
            elif re.match(r'^\d+$', value):
                value = int(value)
        out[key] = value

    return out
