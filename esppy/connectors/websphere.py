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

''' ESP IBM WebSphene MQ Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class WebSphereMQSubscriber(Connector):
    '''
    Subscribe to IBM WebSphere MQ events

    Parameters
    ----------
    mqtype : string
        Specifies binary, CSV, JSON, XML, or the name of a string
        field in the subscribed window schema.
    snapshot : boolean, optional
        Specifies whether to send snapshot data.
    mqtopic : string, optional
        Specifies the MQ topic name. Required if mqqueue is
        not configured.
    mqqueue : string, optional
        Specifies the MQ queue name. Required if mqtopic is
        not configured.
    collapse : string, optional
        Enables conversion of UPDATE_BLOCK events to make subscriber
        output publishable. The default value is disabled.
    queuemanager : string, optional
        Specifies the MQ queue manager.
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP
        fields in CSV events. The default behavior is these fields
        are interpreted as an integer number of seconds
        (ESP_DATETIME) or microseconds (ESP_TIMESTAMP) since epoch.
    rmretdel : boolean, optional
        Specifies to remove all delete events from event blocks
        received by a subscriber that were introduced by a
        window retention policy.
    configfilesection : string, optional
        Specifies the name of the section in the connector config
        file to parse for configuration parameters. Specifies
        the value as [configfilesection].
    protofile : string, optional
        Specifies the .proto file that contains the Google
        Protocol Buffers message definition. This definition is
        used to convert event blocks to protobuf messages. When
        you specify this parameter, you must also specify the
        protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message
        in the .proto file that you specified with the protofile
        parameter. Event blocks are converted into this message.
    usecorrelid : boolean, optional
        Copies the value of the correlid field in the event to
        the MQ message correlation ID.
    csvmsgperevent : int, optional
        For CSV, specifies to send one message per event. The
        default is one message per transactional event block or
        else one message per event.
    csvmsgpereventblock : int, optional
        For CSV, specifies to send one message per event block.
        The default is one message per transactional event block
        or else one message per event.

    Returns
    -------
    :class:`WebSphereMQSubscriber`

    '''
    connector_key = dict(cls='mq', type='subscribe')

    property_defs = dict(
        mqtype=prop('mqtype', dtype='string', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        mqtopic=prop('mqtopic', dtype='string'),
        mqqueue=prop('mqqueue', dtype='string'),
        collapse=prop('collapse', dtype='string'),
        queuemanager=prop('queuemanager', dtype='string'),
        dateformat=prop('dateformat', dtype='string'),
        rmretdel=prop('rmretdel', dtype='boolean'),
        configfilesection=prop('configfilesection', dtype='string'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        usecorrelid=prop('usecorrelid', dtype='boolean'),
        csvmsgperevent=prop('csvmsgperevent', dtype='int'),
        csvmsgpereventblock=prop('csvmsgpereventblock', dtype='int')
    )

    def __init__(self, mqtype=None, name=None, is_active=None, snapshot=None,
                 mqtopic=None, mqqueue=None, collapse=None,
                 queuemanager=None, dateformat=None, rmretdel=None,
                 configfilesection=None, protofile=None,
                 protomsg=None, usecorrelid=None, csvmsgperevent=None,
                 csvmsgpereventblock=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'mq', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['mqtype'],
                                         delete='type')
        return cls(req[0], name=name, is_active=is_active, **properties)


class WebSphereMQPublisher(Connector):
    '''
    Publish IBM WebSphere MQ events

    Parameters
    ----------
    mqtype : string
        Specifies binary, CSV, JSON, XML, or opaquestring. For
        opaquestring, the Source window schema is assumed to
        be "index:int64,message:string".
    mqtopic : string, optional
        Specifies the MQ topic name. Required if mqqueue is
        not configured.
    mqqueue : string, optional
        Specifies the MQ queue name. Required if mqtopic is
        not configured.
    mqsubname : string, optional
        Specifies the MQ subscription name. Required if
        mqtopic is configured.
    blocksize : int, optional
        Specifies the number of events to include in a
        published event block. The default value is 1.
    transactional : string, optional
        Sets the event block type to transactional. The
        default value is normal.
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP
        fields in CSV events. The default behavior is these
        fields are interpreted as an integer number of 
        seconds (ESP_DATETIME) or microseconds (ESP_TIMESTAMP)
        since epoch.
    queuemanager : string, optional
        Specifies the MQ queue manager.
    configfilesection : string, optional
        Specifies the name of the section in the connector config
        file to parse for configuration parameters. Specifies
        the value as [configfilesection].
    ignorecsvparseerrors : boolean, optional
        Specifies that when a field in an input CSV event cannot
        be parsed, the event is dropped, an error is logged,
        and publishing continues.
    protofile : string, optional
        Specifies the .proto file that contains the Google
        Protocol Buffers message definition. This definition
        is used to convert event blocks to protobuf messages.
        When you specify this parameter, you must also specify
        the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message
        in the .proto file that you specified with the protofile
        parameter. Event blocks are converted into this message.
    csvfielddelimiter : string, optional
        Specifies the character delimiter for field data in
        input CSV events. The default delimiter is the , character.
    noautogenfield : boolean, optional
        Specifies that input events are missing the key field
        that is autogenerated by the source window.
    publishwithupsert : boolean, optional
        Builds events with opcode=Upsert instead of Insert.
    addcsvopcode : boolean, optional
        Prepends an opcode and comma to input CSV events. The
        opcode is Insert unless publishwithupsert is enabled.
    addcsvflags : string, optional
        Specifies the event type to insert into input CSV
        events (with a comma). Valid values are "normal"
        and "partialupdate".
    usecorrelid : boolean, optional
        Copies the value of the MQ message correlation ID into
        the correlid field in every Event Stream Processing
        event.
    ignoremqmdformat : string, optional
        Specifies to ignore the value of the Message Descriptor
        Format parameter, and assume the message format is
        compatible with the mqtype parameter setting.
    maxevents : int, optional
        Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`WebSphereMQPublisher`

    '''
    connector_key = dict(cls='mq', type='publish')

    property_defs = dict(
        mqtype=prop('mqtype', dtype='string', required=True),
        mqtopic=prop('mqtopic', dtype='string'),
        mqqueue=prop('mqqueue', dtype='string'),
        mqsubname=prop('mqsubname', dtype='string'),
        blocksize=prop('blocksize', dtype='int'),
        transactional=prop('transactional', dtype='string'),
        dateformat=prop('dateformat', dtype='string'),
        queuemanager=prop('queuemanager', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        ignorecsvparseerrors=prop('ignorecsvparseerrors', dtype='boolean'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        csvfielddelimiter=prop('csvfielddelimiter', dtype='string'),
        noautogenfield=prop('noautogenfield', dtype='boolean'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        addcsvopcode=prop('addcsvopcode', dtype='boolean'),
        addcsvflags=prop('addcsvflags', dtype='string'),
        usecorrelid=prop('usecorrelid', dtype='boolean'),
        ignoremqmdformat=prop('ignoremqmdformat', dtype='boolean'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, mqtype=None, name=None, is_active=None,
                 mqtopic=None, mqqueue=None, mqsubname=None,
                 blocksize=None, transactional=None, dateformat=None,
                 queuemanager=None, configfilesection=None,
                 ignorecsvparseerrors=None, protofile=None,
                 protomsg=None, csvfielddelimiter=None,
                 noautogenfield=None, publishwithupsert=None,
                 addcsvopcode=None, addcsvflags=None,
                 usecorrelid=None, ignoremqmdformat=None,
                 maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'mq', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['mqtype'],
                                         delete='type')
        return cls(req[0], name=name, is_active=is_active, **properties)
