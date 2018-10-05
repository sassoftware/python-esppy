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

''' ESP Rabbit MQ Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class RabbitMQSubscriber(Connector):
    '''
    Subscribe to Rabbit MQ events

    Parameters
    ----------
    rmqhost : string
        Specifies the Rabbit MQ server host name
    mqport : string
        Specifies the Rabbit MQ server port
    rmquserid : string
        Specifies the user name required to authenticate the connector's
        session with the Rabbit MQ server.    
    rmqpassword : string
        Specifies the password associated with rmquserid
    rmqexchange : string
        Specifies the Rabbit MQ exchange created by the connector,
        if nonexistent.
    rmqtopic : string
        Specifies the Rabbit MQ routing key to which messages are published.
    rmqtype : string
        Specifies binary, CSV, JSON, or the name of a string field in
        the subscribed window schema.
    urlhostport : string
        Specifies the host:port field in the metadata topic subscribed to
        on start-up to field metadata requests.
    numbufferedmsgs : int
        Specifies the maximum number of messages buffered by a standby
        subscriber connector.
    snapshot : boolean, optional
        Specifies whether to send snapshot data
    collapse : string, optional
        Enables conversion of UPDATE_BLOCK events to make subscriber
        output publishable. The default value is disabled.
    rmretdel : boolean, optional
        Specifies to remove all delete events from event blocks received
        by a subscriber that were introduced by a window retention policy.
    hotfailover : boolean, optional
        Enables hot failover mode
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields in
        CSV events. The default behavior is these fields are interpreted
        as an integer number of seconds (ESP_DATETIME) or microseconds
        (ESP_TIMESTAMP) since epoch.
    buspersistence : boolean, optional
        Specify to send messages using persistent delivery mode
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol Buffers
        message definition used to convert event blocks to protobuf messages.
        When you specify this parameter, you must also specify the
        protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in
        the .proto file that you specified with the protofile parameter.
        Event blocks are converted into this message.
    csvincludeschema : string, optional
        When rmqtype=CSV, specifies when to prepend output CSV data with
        the window's serialized schema. Valid values are never, once, and
        pereventblock. The default value is never.
    useclientmsgid : boolean, optional
        When performing a failover operation and extracting a message ID
        from an event block, use the client-generated message ID instead
        of the engine-generated message ID.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file
        to parse for configuration parameters. Specify the value
        as [configfilesection].
    rmqpasswordencrypted : boolean, optional
        Specifies that rmqpassword is encrypted
    rmqvhost : string, optional
        Specifies the Rabbit MQ vhost. The default is "/".
    csvmsgperevent : int, optional
        For CSV, specifies to send one message per event. The default is
        one message per transactional event block or else one message per event.
    csvmsgpereventblock : int, optional
        For CSV, specifies to send one message per event block. The default
        is one message per transactional event block or else one message
        per event.
    rmqcontenttype : string, optional
        Specifies the value of the content_type parameter in messages
        sent to RabbitMQ.
    rmqheaders : string, optional
        A comma separated list of key value optional headers in messages
        sent to RabbitMQ. The default value is no headers.
    rmqssl : boolean, optional
        Specifies to enable SSL encryption on the connection to the
        Rabbit MQ server.
    rmqsslcacert : string, optional
        When rmqssl is enabled, specifies the full path of the SSL CA
        certificate .pem file.
    rmqsslkey : string, optional
        When rmqssl is enabled, specifies the full path of the SSL key
        .pem file.
    rmqsslcert string, optional
        When rmqssl is enabled, specifies the full path of the SSL
        certificate .pem file.

    Returns
    -------
    :class:`RabbitMQSubscriber`

    '''
    connector_key = dict(cls='rmq', type='subscribe')

    property_defs = dict(
        rmquserid=prop('rmquserid', dtype='string', required=True),
        rmqpassword=prop('rmqpassword', dtype='string', required=True),
        rmqhost=prop('rmqhost', dtype='string', required=True),
        rmqport=prop('rmqport', dtype='int', required=True),
        rmqexchange=prop('rmqexchange', dtype='string', required=True),
        rmqtopic=prop('rmqtopic', dtype='string', required=True),
        rmqtype=prop('rmqtype', dtype='string', required=True),
        urlhostport=prop('urlhostport', dtype='string', required=True),
        numbufferedmsgs=prop('numbufferedmsgs', dtype='int', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        collapse=prop('collapse', dtype='string'),
        rmretdel=prop('rmretdel', dtype='boolean'),
        hotfailover=prop('hotfailover', dtype='boolean'),
        dateformat=prop('dateformat', dtype='string'),
        buspersistence=prop('buspersistence', dtype='boolean'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        csvincludeschema=prop('csvincludeschema', dtype='string'),
        useclientmsgid=prop('useclientmsgid', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        rmqpasswordencrypted=prop('rmqpasswordencrypted', dtype='boolean'),
        rmqvhost=prop('rmqvhost', dtype='string'),
        csvmsgperevent=prop('csvmsgperevent', dtype='int'),
        csvmsgpereventblock=prop('csvmsgpereventblock', dtype='int'),
        rmqcontenttype=prop('rmqcontenttype', dtype='string'),
        rmqheaders=prop('rmqheaders', dtype='string'),
        rmqssl=prop('rmqssl', dtype='boolean'),
        rmqsslcacert=prop('rmqsslcacert', dtype='string'),
        rmqsslkey=prop('rmqsslkey', dtype='string'),
        rmqsslcert=prop('rmqsslcert', dtype='string')
    )

    def __init__(self, rmqhost=None, rmqport=None, rmquserid=None,
                 rmqpassword=None, rmqexchange=None,
                 rmqtopic=None, rmqtype=None, urlhostport=None, numbufferedmsgs=None,
                 snapshot=None, name=None, is_active=None, collapse=None,
                 rmretdel=None, hotfailover=None, dateformat=None,
                 buspersistence=None, protofile=None, protomsg=None,
                 csvincludeschema=None, useclientmsgid=None,
                 configfilesection=None, rmqpasswordencrypted=None,
                 rmqvhost=None, csvmsgperevent=None, csvmsgpereventblock=None,
                 rmqcontenttype=None, rmqheaders=None, rmqssl=None,
                 rmqsslcacert=None, rmqsslkey=None, rmqsslcert=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'rmq', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['rmqhost', 'rmqport', 'rmquserid',
                                                   'rmqpassword', 'rmqexchange',
                                                   'rmqtopic', 'rmqtype',
                                                   'urlhostport'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4], req[5], req[6],
                   req[7], name=name, is_active=is_active, **properties)


class RabbitMQPublisher(Connector):
    '''
    Publish Rabbit MQ events

    Parameters
    ----------
    rmqhost : string
        Specifies the Rabbit MQ server host name
    mqport : string
        Specifies the Rabbit MQ server port
    rmquserid : string
        Specifies the user name required to authenticate the connector’s
        session with the Rabbit MQ server.
    rmqpassword : string
        Specifies the password associated with rmquserid
    rmqexchange : string
        Specifies the Rabbit MQ exchange created by the connector,
        if nonexistent.
    rmqtopic : string
        Specifies the Rabbit MQ routing key to which messages are published.
    rmqtype : string
        Specifies binary, CSV, JSON, or the name of a string field in
        the subscribed window schema.
    urlhostport : string
        Specifies the host:port field in the metadata topic subscribed to
        on start-up to field metadata requests.
    transactional : string, optional
        When rmqtype=CSV, sets the event block type to transactional.
        The default value is normal.
    blocksize : int, optional
        When rmqtype=CSV, specifies the number of events to include in a
        published event block. The default value is 1.
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields in
        CSV events. The default behavior is these fields are interpreted
        as an integer number of seconds (ESP_DATETIME) or microseconds
        (ESP_TIMESTAMP) since epoch.
    buspersistence : string, optional
        Controls both auto-delete and durable
    buspersistencequeue : string, optional
        Specifies the queue name used by a persistent publisher
    ignorecsvparseerrors : boolean, optional
        Specifies that when a field in an input CSV event cannot be
        parsed, the event is dropped, an error is logged, and
        publishing continues.
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol
        Buffers message definition used to convert event blocks to
        protobuf messages. When you specify this parameter, you must
        also specify the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the
        .proto file that you specified with the protofile parameter.
        Event blocks are converted into this message.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file
        to parse for configuration parameters. Specify the value
        as [configfilesection].
    csvfielddelimiter : string, optional
        Specifies the character delimiter for field data in input CSV
        events. The default delimiter is the , character.
    noautogenfield : boolean, optional
        Specifies that input events are missing the key field that is
        autogenerated by the source window.
    ackwindow : int, optional
        Specifies the time period (in seconds) to leave messages that
        are received from Rabbit MQ unacknowledged. 
    acktimer : int, optional
        Specifies the time interval (in seconds) for how often to
        check whether to send acknowledgments that are triggered by
        the ackwindow parameter. Must be configured if ackwindow is
        configured.
    publishwithupsert : boolean, optional
        Builds events with opcode=Upsert instead of Insert
    rmqpasswordencrypted : boolean, optional
        Specifies that rmqpassword is encrypted
    addcsvopcode : boolean, optional
        Prepends an opcode and comma to input CSV events. The opcode
        is Insert unless publishwithupsert is enabled.
    addcsvflags : string, optional
        Specifies the event type to insert into input CSV events
        (with a comma). Valid values are "normal" and "partialupdate".
    rmqvhost : string, optional
        Specifies the Rabbit MQ vhost. The default is "/"
    useclientmsgid : string, optional
        If the Source window has been restored from a persist to disk,
        ignores received binary event blocks that contain a message ID
        less than the greatest message ID in the restored window.
    rmqssl : boolean, optional
        Specifies to enable SSL encryption on the connection to the
        Rabbit MQ server.
    rmqsslcacert : string, optional
        When rmqssl is enabled, specifies the full path of the SSL CA
        certificate .pem file.
    rmqsslkey : string, optional
        When rmqssl is enabled, specifies the full path of the SSL key
        .pem file.
    rmqsslcert string, optional
        When rmqssl is enabled, specifies the full path of the SSL
        certificate .pem file.
    maxevents : int, optional
         Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`RabbitMQPublisher`

    '''
    connector_key = dict(cls='rmq', type='publish')

    property_defs = dict(
        rmquserid=prop('rmquserid', dtype='string', required=True),
        rmqpassword=prop('rmqpassword', dtype='string', required=True),
        rmqhost=prop('rmqhost', dtype='string', required=True),
        rmqport=prop('rmqport', dtype='int', required=True),
        rmqexchange=prop('rmqexchange', dtype='string', required=True),
        rmqtopic=prop('rmqtopic', dtype='string', required=True),
        rmqtype=prop('rmqtype', dtype='string', required=True),
        urlhostport=prop('urlhostport', dtype='string', required=True),
        transactional=prop('transactional', dtype='string'),
        blocksize=prop('blocksize', dtype='int'),
        dateformat=prop('dateformat', dtype='string'),
        buspersistence=prop('buspersistence', dtype='string'),
        buspersistencequeue=prop('buspersistencequeue', dtype='string'),
        ignorecsvparseerrors=prop('ignorecsvparseerrors', dtype='boolean'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        csvfielddelimiter=prop('csvfielddelimiter', dtype='string'),
        noautogenfield=prop('noautogenfield', dtype='boolean'),
        ackwindow=prop('ackwindow', dtype='int'),
        acktimer=prop('acktimer', dtype='int'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        rmqpasswordencrypted=prop('rmqpasswordencrypted', dtype='boolean'),
        addcsvopcode=prop('addcsvopcode', dtype='boolean'),
        addcsvflags=prop('addcsvflags', dtype='string'),
        rmqvhost=prop('rmqvhost', dtype='string'),
        useclientmsgid=prop('useclientmsgid', dtype='boolean'),
        rmqssl=prop('rmqssl', dtype='boolean'),
        rmqsslcacert=prop('rmqsslcacert', dtype='string'),
        rmqsslkey=prop('rmqsslkey', dtype='string'),
        rmqsslcert=prop('rmqsslcert', dtype='string'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, rmqhost=None, rmqport=None, rmquserid=None,
                 rmqpassword=None, rmqexchange=None,
                 rmqtopic=None, rmqtype=None, urlhostport=None,
                 name=None, is_active=None,
                 transactional=None, blocksize=None, dateformat=None,
                 buspersistence=None, buspersistencequeue=None,
                 ignorecsvparseerrors=None, protofile=None, protomsg=None,
                 configfilesection=None, csvfielddelimiter=None,
                 noautogenfield=None, ackwindow=None, acktimer=None,
                 publishwithupsert=None, rmqpasswordencrypted=None,
                 addcsvopcode=None, addcsvflags=None, rmqvhost=None,
                 useclientmsgid=None, rmqssl=None, rmqsslcacert=None,
                 rmqsslkey=None, rmqsslcert=None, maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'rmq', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['rmqhost', 'rmqport',
                                                   'rmquserid', 'rmqpassword',
                                                   'rmqexchange', 'rmqtopic',
                                                   'rmqtype', 'urlhostport'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4], req[5], req[6],
                   req[7], name=name, is_active=is_active, **properties)
