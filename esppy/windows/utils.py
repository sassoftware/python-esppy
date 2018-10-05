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

from __future__ import print_function, division, absolute_import, unicode_literals

import six
from six.moves import urllib
from ..exceptions import ESPError
from ..utils import xml
from ..utils.xml import ensure_element


def verify_window(window, session=None):
    '''
    Verify that the window exists on the server

    Parameters
    ----------
    window : Window or string
        The window object or URL to check for
    session : requests.Session, optional
        The session to use to check for the window.  This is only used
        if the first parameter is a string.

    Returns
    -------
    boolean

    '''
    from .base import BaseWindow
    try:
        if isinstance(window, BaseWindow):
            window._get(urllib.parse.urljoin(window.base_url,
                                             'windows/%s/%s/%s' %
                                             (window.project,
                                              window.contquery,
                                              window.name)))
            return True
        elif isinstance(window, six.string_types):
            window = window.replace('.', '/')
            out = session.get(urllib.parse.urljoin(session.base_url,
                                                   'windows/%s' % window))
            if out.status_code < 300:
                return True
    except ESPError as exc:
        print(exc)
        pass
    return False


def get_args(kwargs):
    '''
    Return keyword arguments while removing `self`

    Parameters
    ----------
    kwargs : dict
        The keyword arguments dictionary

    Returns
    -------
    dict

    '''
    kwargs = dict(kwargs)
    kwargs.pop('self', None)
    return kwargs


def listify(data):
    '''
    Ensure data is a list

    Parameters
    ----------
    data : any
        The data to put into a list

    Returns
    -------
    list

    '''
    if data is None:
        return data
    if isinstance(data, (set, list, tuple)):
        return list(data)
    return [data]


def connectors_to_end(elem):
    ''' Move connectors to the end of element '''
    connectors = elem.find('./connectors')
    if connectors is not None:
        elem.remove(connectors)
        elem.append(connectors)
