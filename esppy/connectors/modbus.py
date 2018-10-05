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

''' ESP Modbus Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class ModbusSubscriber(Connector):
    '''
    Subscribe to Modbus events

    Parameters
    ----------
    modbus : string
        The modbus server location. The default port is 502. If your
        server is running on a different port, use host:port notation.
    snapshot : boolean, optional
        If true a snapshot is pulled from the window on startup.

    Returns
    -------
    :class:`ModbusSubscriber`

    '''
    connector_key = dict(cls='modbus', type='subscribe')

    property_defs = dict(
        modbus=prop('modbus', dtype='string', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True)
    )

    def __init__(self, modbus, name=None, is_active=None, snapshot=False):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'modbus', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['modbus'],
                                         delete='type')
        return cls(req[0], name=name, is_active=is_active, **properties)


class ModbusPublisher(Connector):
    '''
    Publish Modbus events

    Parameters
    ----------
    modbus : string
        The modbus server location. The default port is 502. If your
        server is running on a different port, use host:port notation.
    interval : int
        The interval at which the object value data will be pulled
        from the Modbus server (defaults to 10 seconds)

    Returns
    -------
    :class:`ModbusPublisher`

    '''
    connector_key = dict(cls='modbus', type='publish')

    property_defs = dict(
        modbus=prop('modbus', dtype='string', required=True),
        interval=prop('interval', dtype='int')
    )

    def __init__(self, modbus, name=None, is_active=None, interval=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'modbus', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['modbus'],
                                         delete='type')
        return cls(req[0], name=name, is_active=is_active, **properties)
