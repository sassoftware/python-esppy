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

''' ESP Solace Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class SolaceSubscriber(Connector):
    '''
    Subscribe to Solace events

    Parameters
    ----------
    solhostport : string
        Specifies the appliance to connect to, in the form 'host:port'
    solvpn : string
        Specifies the appliance message VPN to assign the client to
        which the session connects.
    soltopic : string
        Specifies the Solace destination topic to which to publish
    urlhostport : string
        Specifies the host:port field in the metadata topic subscribed
        to on start-up to field metadata requests.
    numbufferedmsgs : int
        Specifies the maximum number of messages buffered by a standby
        subscriber connector.
    snapshot : boolean, optional
        Specifies whether to send snapshot data
    collapse : string, optional
        Enables conversion of UPDATE_BLOCK events to make subscriber
        output publishable. The default value is disabled.
    hotfailover : boolean, optional
        Enables hot failover mode.
    buspersistence : string, optional
        Sets the Solace message delivery mode to Guaranteed Messaging.
        The default value is Direct Messaging.
    rmretdel : boolean, optional
        Specifies to remove all delete events from event blocks
        received by a subscriber that were introduced by a window
        retention policy.
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol
        Buffers message definition used to convert event blocks to protobuf
        messages. When you specify this parameter, you must also specify
        the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the
        .proto file that you specified with the protofile parameter.
        Event blocks are converted into this message.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file
        to parse for configuration parameters. Specify the value
        as [configfilesection].
    json : boolean, optional
        Enables transport of event blocks encoded as JSON messages
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields in
        CSV events. The default behavior is these fields are interpreted as
        an integer number of seconds (ESP_DATETIME) or microseconds
        (ESP_TIMESTAMP) since epoch.
    solpasswordencrypted : boolean, optional
        Specifies that solpassword is encrypted

    Returns
    -------
    :class:`SolaceSubscriber`

    '''
    connector_key = dict(cls='sol', type='subscribe')

    property_defs = dict(
        solhostport=prop('solhostport', dtype='string', required=True),
        soluserid=prop('soluserid', dtype='string', required=True),
        solpassword=prop('solpassword', dtype='string', required=True),
        solvpn=prop('solvpn', dtype='string', required=True),
        soltopic=prop('soltopic', dtype='string', required=True),
        urlhostport=prop('urlhostport', dtype='string', required=True),
        numbufferedmsgs=prop('numbufferedmsgs', dtype='int', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        collapse=prop('collapse', dtype='string'),
        hotfailover=prop('hotfailover', dtype='boolean'),
        buspersistence=prop('buspersistence', dtype='string'), 
        rmretdel=prop('rmretdel', dtype='boolean'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        json=prop('json', dtype='boolean'),
        dateformat=prop('dateformat', dtype='string'),
        solpasswordencrypted=prop('solpasswordencrypted', dtype='boolean')
    )

    def __init__(self, solhostport=None, soluserid=None, solpassword=None,
                 solvpn=None, soltopic=None, urlhostport=None,
                 name=None, is_active=None,
                 numbufferedmsgs=None, snapshot=None, collapse=None,
                 hotfailover=None, buspersistence=None, rmretdel=None,
                 protofile=None, protomsg=None, configfilesection=None,
                 json=None, dateformat=None, solpasswordencrypted=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'sol', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['solhostport',
                                                   'soluserid',
                                                   'solpassword',
                                                   'solvpn', 'soltopic',
                                                   'urlhostport',
                                                   'numbufferedmsgs'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4], req[5],
                   name=name, is_active=is_active, **properties)


class SolacePublisher(Connector):
    '''
    Publish events to Solace

    Parameters
    ----------
    solhostport : string
        Specifies the appliance to connect to, in the form “host:port”
    soluserid : string
        Specifies the user name required to authenticate the connector’s
        session with the appliance.
    solpassword : string
        Specifies the password associated with soluserid
    solvpn : string
        Specifies the appliance message VPN to assign the client to which
        the session connects.
    soltopic : string
        Specifies the Solace topic to which to subscribe
    urlhostport : string
        Specifies the host:port field in the metadata topic subscribed
        to on start-up to field metadata requests.
    buspersistence : boolean, optional
        Creates the Guaranteed message flow to bind to the topic endpoint
        provisioned on the appliance that the published Guaranteed messages
        are delivered and spooled to
    buspersistencequeue : string, optional
        Specifies the name of the queue to which the Guaranteed message
        flow binds.
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol Buffers
        message definition used to convert event blocks to protobuf
        messages. When you specify this parameter, you must also specify
        the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the
        .proto file that you specified with the protofile parameter.
        Event blocks are converted into this message.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file
        to parse for configuration parameters. Specify the value
        as [configfilesection].
    json : boolean, optional
        Enables transport of event blocks encoded as JSON messages
    publishwithupsert : boolean, optional
        Specifies to build with opcode = Upsert instead of opcode = Insert
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields
        in CSV events. The default behavior is these fields are
        interpreted as an integer number of seconds (ESP_DATETIME) or
        microseconds (ESP_TIMESTAMP) since epoch.
    solpasswordencrypted : boolean, optional
        Specifies that solpassword is encrypted
    getmsgfromdestattr : boolean, optional
        Specifies to extract the payload from the destination attribute
        instead of the message body.
    transactional : string, optional
        When getmsgfromdestattr is enabled, sets the event block type
        to transactional. The default value is normal.
    blocksize : int, optional
        When getmsgfromdestattr is enabled, specifies the number of
        events to include in a published event block. The default
        value is 1.
    maxevents : int, optional
        Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`SolacePublisher`

    '''
    connector_key = dict(cls='sol', type='publish')

    property_defs = dict(
        solhostport=prop('solhostport', dtype='string', required=True),
        soluserid=prop('soluserid', dtype='string', required=True),
        solpassword=prop('solpassword', dtype='string', required=True),
        solvpn=prop('solvpn', dtype='string', required=True),
        soltopic=prop('soltopic', dtype='string', required=True),
        urlhostport=prop('urlhostport', dtype='string', required=True),
        buspersistence=prop('buspersistence', dtype='boolean'),
        buspersistencequeue=prop('buspersistencequeue', dtype='string'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        json=prop('json', dtype='boolean'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        dateformat=prop('dateformat', dtype='string'),
        solpasswordencrypted=prop('solpasswordencrypted', dtype='boolean'),
        getmsgfromdestattr=prop('getmsgfromdestattr', dtype='boolean'),
        transactional=prop('transactional', dtype='string'),
        blocksize=prop('blocksize', dtype='int'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, solhostport=None, soluserid=None,
                 solpassword=None, solvpn=None, soltopic=None,
                 urlhostport=None, name=None, is_active=None,
                 buspersistence=None, buspersistencequeue=None,
                 protofile=None, protomsg=None, configfilesection=None,
                 json=None, publishwithupsert=None, dateformat=None,
                 solpasswordencrypted=None, getmsgfromdestattr=None,
                 transactional=None, blocksize=None, maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'sol', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['solhostport',
                                                   'soluserid',
                                                   'solpassword',
                                                   'solvpn',
                                                   'soltopic',
                                                   'urlhostport'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4], req[5],
                   name=name, is_active=is_active, **properties)
