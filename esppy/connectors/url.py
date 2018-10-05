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

''' ESP URL Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class URLPublisher(Connector):
    '''
    Publish URL events

    Parameters
    ----------
    configUrl : string
        Specifies the URL for the connector configuration.
    interval : string, optional
        Specifies the interval at which the requests are sent.
        The default is '10 seconds'. 
    maxevents : int, optional
        Specifies the maximum number of events to publish.
    properties : string, optional
        Specifies the properties that can be used in the
        configuration. The properties are entered as a
        semicolon-delimited list of name-value pairs.

    Returns
    -------
    :class:`URLPublisher`

    '''
    connector_key = dict(cls='url', type='publish')

    property_defs = dict(
        configUrl=prop('configUrl', dtype='string', required=True),
        interval=prop('interval', dtype='string'),
        maxevents=prop('maxevents', dtype='int'),
        properties=prop('properties', dtype='string')
    )

    def __init__(self, configUrl, name=None, is_active=None,
                 interval=None, maxevents=None, properties=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'url', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['configUrl'],
                                         delete='type')
        return cls(req[0], name=name, is_active=is_active, **properties)
