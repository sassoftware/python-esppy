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

''' ESP Adapter Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class AdapterSubscriber(Connector):
    '''
    Subscribe using an adapter

    Parameters
    ----------
    command : string
        Specifies the command and options that are used to run the adapter
        from the command line. All required adapter parameters must be specified
        when defining the command parameter.
    url : string, optional
        Specifies the URL for the adapter connection. If this parameter is not
        included, a default URL is generated.

    Returns
    -------
    :class:`AdapterSubscriber`

    '''
    connector_key = dict(cls='adapter', type='subscribe')

    property_defs = dict(
        command=prop('command', dtype='string', required=True),
        url=prop('url', dtype='string'),
    )

    def __init__(self, command, url=None, name=None, is_active=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'adapter', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['command'],
                                         delete='type')
        return cls(req[0], name=name, is_active=is_active, **properties)


class AdapterPublisher(Connector):
    '''
    Publish using an adapter

    Parameters
    ----------
    command : string
        Specifies the command and options that are used to run the adapter
        from the command line. All required adapter parameters must be specified
        when defining the command parameter.
    url : string, optional
        Specifies the URL for the adapter connection. If this parameter is not
        included, a default URL is generated.

    Returns
    -------
    :class:`AdapterPublisher`

    '''
    connector_key = dict(cls='adapter', type='publish')

    property_defs = dict(
        command=prop('command', dtype='string', required=True),
        url=prop('url', dtype='string'),
    )

    def __init__(self, command, url=None, name=None, is_active=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'adapter', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['command'],
                                         delete='type')
        return cls(req[0], name=name, is_active=is_active, **properties)
