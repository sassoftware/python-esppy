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

''' ESP Kafka Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class KafkaSubscriber(Connector):
    '''
    Subscribe to events from a Kafka broker

    Parameters
    ----------
    kafkahostport : string
        Specifies one or more Kafka brokers in the following form:
        'host:port,host:port,...'.
    kafkatopic : string
        Specifies the Kafka topic
    urlhostport : string
        Specifies the host:port field in the metadata topic that is
        subscribed to on start-up.
    kafkapartition : string, optional
        Specifies the Kafka partition
    kafkatype : string, optional
        Specifies binary, CSV, JSON, or the name of a string field in
        the subscribed window schema.
    numbufferedmsgs : int, optional
        Specifies the maximum number of messages buffered by a
        standby subscriber connector.
    name : string, optional
        The name of the connector object
    snapshot : bool, optional
        Specifies whether to send snapshot data
    collapse : bool, optional
        Enables conversion of UPDATE_BLOCK events to make subscriber
        output publishable.
    rmretdel : bool, optional
        Specifies to remove all delete events from event blocks received
        by a subscriber that were introduced by a window retention policy.
    hotfailover : bool, optional
        Enables hot failover mode
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields
        in CSV events.
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol
        Buffers message definition used to convert event blocks to
        protobuf messages.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the
        .proto file that you specified with the protofile parameter.
    csvincludeschema : string, optional
        When kafkatype=CSV, specifies when to prepend output CSV data
        with the window's serialized schema.
        Valid values: 'never', 'once', and 'pereventblock'
    useclientmsgid : bool, optional
        Uses the client-generated message ID instead of the engine-generated
        message ID when performing a failover operation and extracting a
        message ID from an event block.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file
        to parse for configuration parameters.
    zookeeperhostport : string, optional
        Specifies the Zookeeper server in the form 'host:port'
    kafkaglobalconfig : string, optional
        Specifies a semicolon-separated list of 'key=value' strings to
        configure librdkafka global configuration values.
    kafkatopicconfig : string, optional
        Specifies a semicolon-separated list of 'key=value' strings to
        configure librdkafka topic configuration values.
    csvmsgperevent : bool, optional
        For CSV, specifies to send one message per event
    csvmsgperevent_block : bool, optional
        For CSV, specifies to send one message per event block

    Returns
    -------
    :class:`KafkaSubscriber`

    '''
    connector_key = dict(cls='kafka', type='subscribe')

    property_defs = dict(
        kafkahostport=prop('kafkahostport', dtype='string', required=True,
                           valid_values=re.compile(r'(\w[\w\-\.]*:\d+\s*,?\s*)+')),
        kafkatopic=prop('kafkatopic', dtype='string', required=True),
        kafkapartition=prop('kafkapartition', dtype='string', required=True, default=0),
        kafkatype=prop('kafkatype', dtype='string', required=True, default='csv'),
        urlhostport=prop('urlhostport', dtype='string', required=True),
        numbufferedmsgs=prop('numbufferedmsgs', dtype='int', required=True,
                             default=10000, valid_expr='value >= 0'),
        snapshot=prop('snapshot', dtype='bool', required=True, default=False),
        collapse=prop('collapse', dtype='bool'),
        rmretdel=prop('rmretdel', dtype='bool'),
        hotfailover=prop('hotfailover', dtype='bool'),
        dateformat=prop('dateformat', dtype='string'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        csvincludeschema=prop('csvincludeschema', dtype='string',
                              valid_values=['never', 'once', 'pereventblock']),
        useclientmsgid=prop('useclientmsgid', dtype='bool'),
        configfilesection=prop('configfilesection', dtype='string'),
        zookeeperhostport=prop('zookeeperhostport', dtype='string',
                               valid_values=re.compile(r'\w[\w\-\.]*:\d+')),
        kafkaglobalconfig=prop('kafkaglobalconfig', dtype='string'),
        kafkatopicconfig=prop('kafkatopicconfig', dtype='string'),
        csvmsgperevent=prop('csvmsgperevent', dtype='bool'),
        csvmsgperevent_block=prop('csvmsgpereventblock', dtype='bool'),
    )

    def __init__(self, kafkahostport=None, kafkatopic=None, urlhostport=None,
                 kafkapartition=None, kafkatype=None, numbufferedmsgs=None,
                 name=None, is_active=None, snapshot=None, collapse=None,
                 rmretdel=None, hotfailover=None, dateformat=None,
                 protofile=None, protomsg=None,
                 csvincludeschema=None, useclientmsgid=None,
                 configfilesection=None, zookeeperhostport=None,
                 kafkaglobalconfig=None, kafkatopicconfig=None,
                 csvmsgperevent=None, csvmsgpereventblock=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'kafka', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['kafkahostport',
                                                   'kafkatopic',
                                                   'urlhostport'],
                                         delete='type')
        return cls(req[0], req[1], req[2], name=name, is_active=is_active, **properties)


class KafkaPublisher(Connector):
    '''
    Publish events to a Kafka broker

    Parameters
    ----------
    kafkahostport : string
        Specifies one or more Kafka brokers in the following form:
        'host:port,host:port,...'.
    kafkatopic : string
        Specifies the Kafka topic
    urlhostport : string
        Specifies the host:port field in the metadata topic that is
        subscribed to on start-up.
    kafkapartition : string, optional
        Specifies the Kafka partition
    kafkatype : string, optional
        Specifies binary, CSV, JSON, or the name of a string field in
        the subscribed window schema.
    name : string, optional
        The name of the connector object
    transactional : string, optional
        When kafkatype = CSV, sets the event block type to transactional.
        The default value is normal.
    blocksize : int, optional
        When kafkatype = CSV, specifies the number of events to include in
        a published event block. The default value is 1.
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields
        in CSV events.
    ignorecsvparseerrors : boolean, optional
        Specifies that when a field in an input CSV event cannot be parsed,
        the event is dropped, an error is logged, and publishing continues.
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol Buffers
        message definition used to convert event blocks to protobuf messages.
        When you specify this parameter, you must also specify the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the .proto
        file that you specified with the protofile parameter. Event blocks
        are converted into this message.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file
        to parse for configuration parameters.
    csvfielddelimiter : string, optional
        Specifies the character delimiter for field data in input CSV
        events. The default delimiter is the ',' character.
    noautogenfield : boolean, optional
        Specifies that input events are missing the key field that is
        autogenerated by the source window.
    publishwithupsert : boolean, optional
        Specifies to build events with opcode=Upsert instead of opcode=Insert.
    kafkainitialoffset : string or int, optional
        Specifies the offset from which to begin consuming messages from the
        Kafka topic and partition. Valid values are "smallest", "largest",
        or an integer. The default value is "smallest".
    addcsvopcode : boolean, optional
        Prepends an opcode and comma to write CSV events. The opcode is
        Insert unless publish_with_upsert is enabled.
    addcsvflags : string, optional
        Specifies the event type to insert into input CSV events (with a comma).
        Valid values are "normal" and "partialupdate".
    kafkaglobalconfig : string, optional
        Specifies a semicolon-separated list of "key=value" strings to configure
        librdkafka global configuration values.
    kafkatopicconfig : string, optional
        Specifies a semicolon-separated list of "key=value" strings to configure
        librdkafka topic configuration values.
    useclientmsgid : boolean, optional
        If the Source window has been restored from a persist to disk, ignore
        received binary event blocks that contain a message ID less than the
        greatest message ID in the restored window.
    maxevents : int, optional
         Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`KafkaPublisher`

    '''
    connector_key = dict(cls='kafka', type='publish')

    property_defs = dict(
        kafkahostport=prop('kafkahostport', dtype='string', required=True,
                           valid_values=re.compile(r'(.+?:.+?\s*,?\s*)+')),
        kafkatopic=prop('kafkatopic', dtype='string', required=True),
        kafkapartition=prop('kafkapartition', dtype='string', required=True),
        kafkatype=prop('kafkatype', dtype='string', required=True),
        urlhostport=prop('urlhostport', dtype='string', required=True),
        name=prop('name', dtype='string'),
        transactional=prop('transactional', dtype='string'),
        blocksize=prop('blocksize', dtype='int'),
        dateformat=prop('dateformat', dtype='string'),
        ignorecsvparseerrors=prop('ignorecsvparseerrors', dtype='boolean'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        csvfielddelimiter=prop('csvfielddelimiter', dtype='string'),
        noautogenfield=prop('noautogenfield', dtype='boolean'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        kafkainitialoffset=prop('kafkainitialoffset', dtype=('string', 'int')),
        addcsvopcode=prop('addcsvopcode', dtype='boolean'),
        addcsvflags=prop('addcsvflags', dtype='boolean'),
        kafkaglobalconfig=prop('kafkaglobalconfig', dtype='string'),
        kafkatopicconfig=prop('kafkatopicconfig', dtype='string'),
        useclientmsgid=prop('useclientmsgid', dtype='boolean'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, kafkahostport=None, kafkatopic=None, urlhostport=None,
                 kafkapartition=None, kafkatype=None, name=None, is_active=None,
                 transactional=None, blocksize=None, dateformat=None,
                 ignorecsvparseerrors=None, protofile=None, protomsg=None,
                 configfilesection=None, csvfielddelimiter=None,
                 noautogenfield=None, publishwithupsert=None,
                 kafkainitialoffset=None, addcsvopcode=None,
                 addcsvflags=None, kafkaglobalconfig=None,
                 kafkatopicconfig=None, useclientmsgid=None,
                 maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'kafka', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['kafkahostport',
                                                   'kafkatopic',
                                                   'urlhostport'],
                                         delete='type')
        return cls(req[0], req[1], req[2], name=name, is_active=is_active, **properties)
