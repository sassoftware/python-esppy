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

''' ESP Tibco Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class TibcoSubscriber(Connector):
    '''
    Subscribe to Tibco Rendezvous (RV) events

    Parameters
    ----------
    tibrvsubject : string
        Specifies the Tibco RV subject name
    tibrvtype : string
        Specifies binary, CSV, JSON, or the name of a string field in
        the subscribed window schema.
    snapshot : boolean, optional
        Specifies whether to send snapshot data
    collapse : string, optional
        Enables conversion of UPDATE_BLOCK events to make subscriber
        output publishable. The default value is disabled.
    tibrvservice : string, optional
        Specifies the Rendezvous service used by the Tibco RV transport
        created by the connector. The default service name is “rendezvous”.
    tibrvnetwork : string, optional
        Specifies the network interface used by the Tibco RV transport
        created by the connector. The default network depends on the
        type of daemon used by the connector.
    tibrvdaemon : string, optional
        Specifies the Rendezvous daemon used by the connector. The
        default is the default socket created by the local daemon.
    rmretdel : boolean, optional
        Specifies to remove all delete events from event blocks received
        by a subscriber that were introduced by a window retention policy.
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields in
        CSV events. The default behavior is these fields are interpreted
        as an integer number of seconds (ESP_DATETIME) or microseconds
        (ESP_TIMESTAMP) since epoch.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file
        to parse for configuration parameters. Specify the value
        as [configfilesection].
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol Buffers
        message definition. This definition is used to convert event
        blocks to protobuf messages. When you specify this parameter,
        you must also specify the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the
        .proto file that you specified with the protofile parameter.
        Event blocks are converted into this message.
    csvmsgperevent : int, optional
        For CSV, specifies to send one message per event. The default is
        one message per transactional event block or else one message
        per event.
    csvmsgpereventblock : int, optional
        For CSV, specifies to send one message per event block. The
        default is one message per transactional event block or else one
        message per event.

    Returns
    -------
    :class:`TibcoSubscriber`

    '''
    connector_key = dict(cls='tibrv', type='subscribe')

    property_defs = dict(
        tibrvsubject=prop('tibrvsubject', dtype='string', required=True),
        tibrvtype=prop('tibrvtype', dtype='string', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        collapse=prop('collapse', dtype='string'),
        tibrvservice=prop('tibrvservice', dtype='string'),
        tibrvnetwork=prop('tibrvnetwork', dtype='string'),
        tibrvdaemon=prop('tibrvdaemon', dtype='string'),
        rmretdel=prop('rmretdel', dtype='boolean'),
        dateformat=prop('dateformat', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        csvmsgperevent=prop('csvmsgperevent', dtype='int'),
        csvmsgpereventblock=prop('csvmsgpereventblock', dtype='int')
    )

    def __init__(self, tibrvsubject=None, tibrvtype=None,
                 name=None, is_active=None,
                 snapshot=None, collapse=None, tibrvservice=None,
                 tibrvnetwork=None, tibrvdaemon=None, rmretdel=None,
                 dateformat=None, configfilesection=None,
                 protofile=None, protomsg=None, csvmsgperevent=None,
                 csvmsgpereventblock=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'tibrv', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['tibrvsubject',
                                                   'tibrvtype'],
                                         delete='type')
        return cls(req[0], req[1], name=name, is_active=is_active, **properties)


class TibcoPublisher(Connector):
    '''
    Subscribe to Tibco Rendezvous (RV) events

    Parameters
    ----------
    tibrvsubject : string
        Specifies the Tibco RV subject name
    tibrvtype : string
        Specifies binary, CSV, JSON, or opaquestring. For opaquestring,
        the Source window schema is assumed to be "index:int64,message:string".
    blocksize : int, optional
        Specifies the number of events to include in a published
        event block. The default value is 1.
    transactional : string, optional
        Sets the event block type to transactional. The default value
        is normal.
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields
        in CSV events. The default behavior is these fields are
        interpreted as an integer number of seconds (ESP_DATETIME)
        or microseconds (ESP_TIMESTAMP) since epoch.
    tibrvservice : string, optional
        Specifies the Rendezvous service used by the Tibco RV
        transport created by the connector. The default service
        name is “rendezvous”.
    tibrvnetwork : string, optional
        Specifies the network interface used by the Tibco RV transport
        created by the connector. The default network depends
        on the type of daemon used by the connector.
    tibrvdaemon : string, optional
        Specifies the Rendezvous daemon used by the connector. The
        default is the default socket created by the local daemon.
    configfilesection : string, optional
        Specifies the name of the section in the connector config
        file to parse for configuration parameters. Specify the
        value as [configfilesection].
    ignorecsvparseerrors : boolean, optional
        Specifies that when a field in an input CSV event cannot be
        parsed, the event is dropped, an error is logged, and
        publishing continues.
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol
        Buffers message definition. This definition is used to
        convert event blocks to protobuf messages. When you specify
        this parameter, you must also specify the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in
        the .proto file that you specified with the protofile
        parameter. Event blocks are converted into this message.
    csvfielddelimiter : string, optional
        Specifies the character delimiter for field data in input
        CSV events. The default delimiter is the , character.
    noautogenfield : boolean, optional
        Specifies that input events are missing the key field that
        is autogenerated by the source window.
    publishwithupsert : boolean, optional
        Specifies to build events with opcode = Upsert instead of Insert.
    addcsvopcode : boolean, optioanl
        Prepends an opcode and comma to input CSV events. The
        opcode is Insert unless publishwithupsert is enabled.
    addcsvflags : string, optional
        Specifies the event type to insert into input CSV events
        (with a comma). Valid values are "normal" and "partialupdate".
    maxevents : int, optional
        Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`TibcoPublisher`

    '''
    connector_key = dict(cls='tibrv', type='publish')

    property_defs = dict(
        tibrvsubject=prop('tibrvsubject', dtype='string', required=True),
        tibrvtype=prop('tibrvtype', dtype='string', required=True),
        blocksize=prop('blocksize', dtype='int'),
        transactional=prop('transactional', dtype='string'),
        dateformat=prop('dateformat', dtype='string'),
        tibrvservice=prop('tibrvservice', dtype='string'),
        tibrvnetwork=prop('tibrvnetwork', dtype='string'),
        tibrvdaemon=prop('tibrvdaemon', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        ignorecsvparseerrors=prop('ignorecsvparseerrors', dtype='boolean'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        csvfielddelimiter=prop('csvfielddelimiter', dtype='string'),
        noautogenfield=prop('noautogenfield', dtype='boolean'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        addcsvopcode=prop('addcsvopcode', dtype='boolean'),
        addcsvflags=prop('addcsvflags', dtype='string'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, tibrvsubject=None, tibrvtype=None,
                 name=None, is_active=None,
                 blocksize=None, transactional=None, dateformat=None,
                 tibrvservice=None, tibrvnetwork=None, tibrvdaemon=None,
                 configfilesection=None, ignorecsvparseerrors=None,
                 protofile=None, protomsg=None, csvfielddelimiter=None,
                 noautogenfield=None, publishwithupsert=None,
                 addcsvopcode=None, addcsvflags=None, maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'tibrv', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['tibrvsubject',
                                                   'tibrvtype'],
                                         delete='type')
        return cls(req[0], req[1], name=name, is_active=is_active, **properties)
