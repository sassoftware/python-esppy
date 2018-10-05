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

''' ESP Tervela Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class TervelaSubscriber(Connector):
    '''
    Subscribe to Tervela Data Fabric events

    Parameters
    ----------
    tvaprimarytmx : string
        Specifies the host name or IP address of the primary TMX
    tvauserid : string
        Specifies a user name defined in the Tervela TPM.
        Publish-topic entitlement rights must be associated with
        this user name.
    tvapassword : string
        Specifies the password associated with tvauserid
    tvatopic : string
        Specifies the topic name for the topic to which to subscribed.
        This topic must be configured on the TPM for the GD service and
        tvauserid must be assigned the Guaranteed Delivery subscribe
        rights for this Topic in the TPM.
    tvaclientname : string
        Specifies the client name associated with the Tervela
        Guaranteed Delivery context.
    tvamaxoutstand : int
        Specifies the maximum number of unacknowledged messages that
        can be published to the Tervela fabric (effectively the size of
        the publication cache). Should be twice the expected transmit rate.
    numbufferedmsgs : int
        Specifies the maximum number of messages buffered by a standby
        subscriber connector. 
    urlhostport : string
        Specifies the “host/port” string sent in the metadata message
        published by the connector on topic SAS.META.tvaclientname when
        it starts.
    snapshot : boolean, optional
        Specifies whether to send snapshot data
    collapse : string, optional
        Enables conversion of UPDATE_BLOCK events to make subscriber
        output publishable. The default value is disabled.
    hotfailover : boolean, optional
        Enables hot failover mode
    tvasecondarytmx : string, optional
        Specifies the host name or IP address of the secondary TMX.
        Required if logging in to a fault-tolerant pair.
    tvalogfile : string, optional
        Causes the connector to log to the specified file instead of to
        syslog (on Linux or Solaris) or Tervela.log (on Windows)
    tvapubbwlimit : int, optional
        Specifies the maximum bandwidth, in Mbps, of data published to
        the fabric. The default value is 100 Mbps.
    tvapubrate : int, optional
        Specifies the rate at which data messages are published to the
        fabric, in Kbps. The default value is 30,000 messages per second.
    tvapubmsgexp : int, optional
        Specifies the maximum amount of time, in seconds, that published
        messages are kept in the cache in the Tervela API.
    rmretdel : boolean, optional
        Specifies to remove all delete events from event blocks received
        by a subscriber that were introduced by a window retention policy.
    configfilesection : string, optional
        Specifies the name of the section in the connector config file to
        parse for configuration parameters. Specify the value
        as [configfilesection].
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol Buffers
        message definition. This definition is used to convert event blocks
        to protobuf messages. When you specify this parameter, you must
        also specify the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the
        .proto file that you specified with the protofile parameter. Event
        blocks are converted into this message.
    json : boolean, optional
        Enables transport of event blocks encoded as JSON messages
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields in
        CSV events. The default behavior is these fields are interpreted
        as an integer number of seconds (ESP_DATETIME) or microseconds
        (ESP_TIMESTAMP) since epoch.
    tvapasswordencrypted : boolean, optional
        Specifies that tvapassword is encrypted

    Returns
    -------
    :class:`TervelaSubscriber`

    '''
    connector_key = dict(cls='tervela', type='subscribe')

    property_defs = dict(
        tvaprimarytmx=prop('tvaprimarytmx', dtype='string', required=True),
        tvauserid=prop('tvauserid', dtype='string', required=True),
        tvapassword=prop('tvapassword', dtype='string', required=True),
        tvatopic=prop('tvatopic', dtype='string', required=True),
        tvaclientname=prop('tvaclientname', dtype='string', required=True),
        tvamaxoutstand=prop('tvamaxoutstand', dtype='int', required=True),
        numbufferedmsgs=prop('numbufferedmsgs', dtype='int', required=True),
        urlhostport=prop('urlhostport', dtype='string', required=True),
        snapshot=prop('snapshot', dtype='string', required=True, default=False),
        collapse=prop('collapse', dtype='string'),
        hotfailover=prop('hotfailover', dtype='boolean'),
        tvasecondarytmx=prop('tvasecondarytmx', dtype='string'),
        tvalogfile=prop('tvalogfile', dtype='string'),
        tvapubbwlimit=prop('tvapubbwlimit', dtype='int'),
        tvapubrate=prop('tvapubrate', dtype='int'),
        tvapubmsgexp=prop('tvapubmsgexp', dtype='int'),
        rmretdel=prop('rmretdel', dtype='boolean'),
        configfilesection=prop('configfilesection', dtype='string'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        json=prop('json', dtype='boolean'),
        dateformat=prop('dateformat', dtype='string'),
        tvapasswordencrypted=prop('tvapasswordencrypted', dtype='boolean')
    )

    def __init__(self, tvaprimarytmx=None, tvauserid=None, tvapassword=None,
                 tvatopic=None, tvaclientname=None, tvamaxoutstand=None,
                 numbufferedmsgs=None, urlhostport=None,
                 name=None, is_active=None, snapshot=None,
                 collapse=None, hotfailover=None, tvasecondarytmx=None,
                 tvalogfile=None, tvapubbwlimit=None, tvapubrate=None,
                 tvapubmsgexp=None, rmretdel=None, configfilesection=None,
                 protofile=None, protomsg=None, json=None,
                 dateformat=None, tvapasswordencrypted=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'tervela', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['tvaprimarytmx',
                                                   'tvauserid',
                                                   'tvapassword',
                                                   'tvatopic',
                                                   'tvaclientname',
                                                   'tvamaxoutstand',
                                                   'numbufferedmsgs',
                                                   'urlhostport'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4], req[5], req[6],
                   req[7], name=name, is_active=is_active, **properties)


class TervelaPublisher(Connector):
    '''
    Subscribe to Tervela Data Fabric events

    Parameters
    ----------
    tvaprimarytmx : string
        Specifies the host name or IP address of the primary TMX
    tvauserid : string
        Specifies a user name defined in the Tervela TPM. Subscribe-topic
        entitlement rights must be associated with this user name.
    tvapassword : string
        Specifies the password associated with tvauserid
    tvatopic : string
        Specifies the topic name for the topic to which to publish. This
        topic must be configured on the TPM for the GD service.
    tvaclientname : string
        Specifies the client name associated with the Tervela Guaranteed
        Delivery context. Must be unique among all instances of
        Tervela connectors.
    tvasubname : string
        Specifies the name assigned to the Guaranteed Delivery subscription
        being created. The combination of this name and tvaclientname
        are used by the fabric to replay the last subscription state
    urlhostport : string
        Specifies the “host:port” string sent in the metadata message
        published by the connector on topic SAS.META.tvaclientname when
        it starts.
    tvasecondarytmx : string, optional
        Specifies the host name or IP address of the secondary TMX.
        Required when logging in to a fault-tolerant pair.
    tvalogfile : string, optional
        Causes the connector to log to the specified file instead of to
        syslog (on Linux or Solaris) or Tervela.log (on Windows)
    configfilesection : string, optional
        Specifies the name of the section in the connector config file
        to parse for configuration parameters. Specify the value
        as [configfilesection].
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol
        Buffers message definition. This definition is used to convert
        event blocks to protobuf messages. When you specify this
        parameter, you must also specify the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the
        .proto file that you specified with the protofile parameter.
        Event blocks are converted into this message.
    json : boolean, optional
        Enables transport of event blocks encoded as JSON messages.
    publishwithupsert : boolean, optional
        Specifies to build events with opcode = Upsert instead of
        opcode = Insert.
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields
        in CSV events. The default behavior is these fields are
        interpreted as an integer number of seconds (ESP_DATETIME)
        or microseconds (ESP_TIMESTAMP) since epoch.
    tvapasswordencrypted : boolean, optional
        Specifies that tvapassword is encrypted
    maxevents : int, optional
         Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`TervelaPublisher`

    '''
    connector_key = dict(cls='tva', type='publish')

    property_defs = dict(
        tvaprimarytmx=prop('tvaprimarytmx', dtype='string', required=True),
        tvauserid=prop('tvauserid', dtype='string', required=True),
        tvapassword=prop('tvapassword', dtype='string', required=True),
        tvatopic=prop('tvatopic', dtype='string', required=True),
        tvaclientname=prop('tvaclientname', dtype='string', required=True),
        tvasubname=prop('tvasubname', dtype='string', required=True),
        urlhostport=prop('urlhostport', dtype='string', required=True),
        tvasecondarytmx=prop('tvasecondarytmx', dtype='string'),
        tvalogfile=prop('tvalogfile', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        json=prop('json', dtype='boolean'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        dateformat=prop('dateformat', dtype='string'),
        tvapasswordencrypted=prop('tvapasswordencrypted', dtype='boolean'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, tvaprimarytmx=None, tvauserid=None,
                 tvapassword=None, tvatopic=None,
                 tvaclientname=None, tvasubname=None, urlhostport=None,
                 name=None, is_active=None,
                 tvasecondarytmx=None, tvalogfile=None,
                 configfilesection=None, protofile=None, protomsg=None,
                 json=None, publishwithupsert=None, dateformat=None,
                 tvapasswordencrypted=None, maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'tva', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['tvaprimarytmx',
                                                   'tvauserid',
                                                   'tvapassword',
                                                   'tvatopic',
                                                   'tvaclientname',
                                                   'tvasubname',
                                                   'urlhostport'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4], req[5], req[6],
                   name=name, is_active=is_active, **properties)
