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

''' ESP OPC-UA Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class OPCUASubscriber(Connector):
    '''
    Subscribe to OPC-UA operations

    Parameters
    ----------
    opcuaendpoint : string, optional
        Specifies the OPC-UA server endpoint (only the portion
        following opc.tcp://).
    opcuanamespaceuri : string, optional
        Specifies the OPC-UA server namespace URI. The default is the
        namespace at index=0.
    opcuausername : string, optional
        Specifies the OPC-UA user name. The default is none.
    opcuapassword : string, optional
        Specifies the OPC-UA password. The default is none.
    opcuanodeids : string, optional
        Specifies a comma-separated list of Node IDs to map to ESP window
        schema fields, in the form <identifier type>_<identifier>. The list
        size must be equal to the number of fields in the subscribed window
        schema. Window field names are in Node ID form by default.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file to
        parse for configuration parameters. Specify the value
        as [configfilesection].
    snapshot : boolean, optional
        Specifies whether to send snapshot data

    Returns
    -------
    :class:`OPCUASubscriber`

    '''
    connector_key = dict(cls='opcua', type='subscribe')

    property_defs = dict(
        opcuaendpoint=prop('opcuaendpoint', dtype='string'),
        opcuanamespaceuri=prop('opcuanamespaceuri', dtype='string'),
        opcuausername=prop('opcuausername', dtype='string'),
        opcuapassword=prop('opcuapassword', dtype='string'),
        opcuanodeids=prop('opcuanodeids', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
    )

    def __init__(self, opcuaendpoint=None, name=None, is_active=None,
                 opcuanamespaceuri=None, opcuausername=None,
                 opcuapassword=None, opcuanodeids=None,
                 configfilesection=None, snapshot=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'opcua', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        properties = map_properties(cls, properties, delete='type')
        return cls(name=name, is_active=is_active, **properties)


class OPCUAPublisher(Connector):
    '''
    Publish OPC-UA operations

    Parameters
    ----------
    opcuaendpoint : string, optional
        Specifies the OPC-UA server endpoint (only the portion
        following opc.tcp://).
    opcuanamespaceuri : string, optional
        Specifies the OPC-UA server namespace URI. The default is the
        namespace at index=0.
    opcuausername : string, optional
        Specifies the OPC-UA user name. The default is none.
    opcuapassword : string, optional
        Specifies the OPC-UA password. The default is none.
    opcuanodeids : string, optional
        Specifies a comma-separated list of Node IDs to map to ESP window
        schema fields, in the form <identifier type>_<identifier>. The list
        size must be equal to the number of fields in the subscribed window
        schema. Window field names are in Node ID form by default.
    publishinterval : int, optional
        Specifies an interval in seconds when current values of all nodes
        in the Source window schema are published. The default is to publish
        when one or more values changes.
    transactional : string, optional
        Sets the event block type to transactional. The default value is normal.
    blocksize : int, optional
        Specifies the number of events to include in a published event
        block. The default value is 1.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file to
        parse for configuration parameters. Specify the value
        as [configfilesection].
    publishwithupsert : boolean, optional
        Builds events with opcode=Upsert instead of Insert.
    maxevents : int, optional
        Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`OPCUAPublisher`

    '''
    connector_key = dict(cls='opcua', type='publish')

    property_defs = dict(
        opcuaendpoint=prop('opcuaendpoint', dtype='string'),
        opcuanamespaceuri=prop('opcuanamespaceuri', dtype='string'),
        opcuausername=prop('opcuausername', dtype='string'),
        opcuapassword=prop('opcuapassword', dtype='string'),
        opcuanodeids=prop('opcuanodeids', dtype='string'),
        publishinterval=prop('publishinterval', dtype='int'),
        transactional=prop('transactional', dtype='string'),
        blocksize=prop('blocksize', dtype='int'),
        configfilesection=prop('configfilesection', dtype='string'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, opcuaendpoint=None, name=None, is_active=None,
                 opcuanamespaceuri=None, opcuausername=None,
                 opcuapassword=None, opcuanodeids=None,
                 publishinterval=None, transactional=None,
                 blocksize=None, configfilesection=None,
                 publishwithupsert=None, maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'opcua', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        properties = map_properties(cls, properties, delete='type')
        return cls(name=name, is_active=is_active, **properties)
