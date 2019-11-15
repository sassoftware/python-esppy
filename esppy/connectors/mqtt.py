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

''' ESP MQTT Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class MQTTSubscriber(Connector):
    '''
    Subscribe to MQ Telemetry Transport (MQTT)

    Parameters
    ----------
    mqtthost : string
        Specifies the MQTT server host name
    mqttclientid : string
        Specifies the string to use as the MQTT Client ID. If empty, a random
        client ID is generated. If NULL, mqttdonotcleansession must be false.
        Must be unique among all clients connected to the MQTT server.
    mqtttopic : string
        Specifies the string to use as an MQTT topic to publish events to.
    mqttqos : string
        Specifies the requested Quality of Service. Values can be 0, 1 or 2.
    mqttmsgtype : string
        Specifies binary, CSV, JSON, or the name of a string field in the
        subscribed window schema.
    snapshot : boolean, optional
        Specifies whether to send snapshot data
    mqttuserid : string, optional
        Specifies the user name required to authenticate the connector's
        session with the MQTT server. 
    mqttpassword : string, optional
        Specifies the password associated with mqttuserid
    mqttport : int, optional
        Specifies the MQTT server port. Default is 1883.
    mqttretainmsg : boolean, optional
        Sets to true to make the published message retained in the MQTT
        Server. Default is false.
    mqttdonotcleansession : boolean, optional
        Instructs the MQTT Server to keep all messages and subscriptions on
        disconnect, instead of keeping them. Default is false.
    mqttkeepaliveinterval : int, optional
        Specifies the number of seconds after which the broker should send a
        PING message to the client if no other messages have been exchanged
        in that time. Default is 10.
    mqttmsgmaxdeliveryattempts : int, optional
        Specifies the number of times the connector tries to resend the
        message in case of failure. Default is 20.
    mqttmsgdelaydeliveryattempts : int, optional
        Specifies the delay in milliseconds between delivery attempts
        specified with mqttmsgmaxdeliveryattempts. Default is 500.
    mqttmsgwaitbeforeretry : int, optional
        Specifies the number of seconds to wait before retrying to send
        messages to the MQTT broker. This applies to publish messages
        with QoS > 0. Default is 20.
    mqttmaxinflightmsg : int, optional
        Specifies the number of QoS 1 and 2 messages that can be simultaneously
        in flight. Default is 20.
    collapse : string, optional
        Enables conversion of UPDATE_BLOCK events to make subscriber output
        publishable. The default value is disabled.
    rmretdel : boolean, optional
        Specifies to remove all delete events from event blocks received by
        a subscriber that were introduced by a window retention policy.
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields in
        CSV events. The default behavior is these fields are interpreted as
        an integer number of seconds (ESP_DATETIME) or microseconds
        (ESP_TIMESTAMP) since epoch.
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol Buffers
        message definition used to convert event blocks to protobuf messages.
        When you specify this parameter, you must also specify the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the .proto
        file that you specified with the protofile parameter. Event blocks
        are converted into this message.
    mqttssl : boolean, optional
        Specifies to use SSL/TLS to connect to the MQTT broker. Default is
        false. In order to use SSL/TLS, the ESP encryption overlay
        must be installed.
    mqttsslcafile : string, optional
        If mqttssl=true, specifies the path to a file containing the PEM encoded
        trusted CA certificate files. Either mqttsslcafile or mqttsslcapath
        must be specified.
    mqttsslcapath : string, optional
        If mqttssl=true, specifies the path to a directory containing the
        PEM encoded trusted CA certificate files. See mosquitto.conf for
        more details about configuring this directory. Either mqttsslcafile
        or mqttsslcapath must be specified.
    mqttsslcertfile : string, optional
        If mqttssl=true, specifies the path to a file containing the PEM
        encoded certificate file for this client. Both mqttsslcertfile and
        mqttsslkeyfile must be provided if one of them is.
    mqttsslkeyfile : string, optional
        If mqttssl=true, specifies the path to a file containing the PEM
        encoded private key for this client. Both mqttsslcertfile and
        mqttsslkeyfile must be provided if one of them is.
    mqttsslpassword : boolean, optional
        If mqttssl=true, and if key file is encrypted, specifies the
        password for decryption.
    csvincludeschema : string, optional
        Specifies "never", "once", or "pereventblock". The default value is
        "never". When mqttmsgtype = CSV, prepend output CSV with the window's
        serialized schema.
    configfilesection : string, optional
        Specifies the name of the section in the config file to parse for
        configuration parameters. Specify the value as [configfilesection].
    mqttpasswordencrypted : boolean, optional
        Specifies that mqttpassword is encrypted
    addcsvopcode : boolean, optional
        Prepends an opcode and comma to input CSV events. The opcode is Insert
        unless publishwithupsert is enabled.
    addcsvflags : string, optional
        Specifies the event type to insert into input CSV events (with a comma).
        Valid values are "normal" and "partialupdate".
    csvmsgperevent : int, optional
        For CSV, specifies to send one message per event. The default is one
        message per transactional event block or else one message per event.
    csvmsgpereventblock : int, optional
        For CSV, specifies to send one message per event block. The default
        is one message per transactional event block or else one message per event.

    Returns
    -------
    :class:`MQTTSubscriber`

    '''
    connector_key = dict(cls='mqtt', type='subscribe')

    property_defs = dict(
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        mqtthost=prop('mqtthost', dtype='string', required=True),
        mqttclientid=prop('mqttclientid', dtype='string', required=True),
        mqtttopic=prop('mqtttopic', dtype='string', required=True),
        mqttqos=prop('mqttqos', dtype='string', required=True),
        mqttmsgtype=prop('mqttmsgtype', dtype='string', required=True),
        mqttuserid=prop('mqttuserid', dtype='string'),
        mqttpassword=prop('mqttpassword', dtype='string'),
        mqttport=prop('mqttport', dtype='int'),
        mqttretainmsg=prop('mqttretainmsg', dtype='boolean'),
        mqttdonotcleansession=prop('mqttdonotcleansession', dtype='boolean'),
        mqttkeepaliveinterval=prop('mqttkeepaliveinterval', dtype='int'),
        mqttmsgmaxdeliveryattempts=prop('mqttmsgmaxdeliveryattempts', dtype='int'),
        mqttmsgdelaydeliveryattempts=prop('mqttmsgdelaydeliveryattempts', dtype='int'),
        mqttmsgwaitbeforeretry=prop('mqttmsgwaitbeforeretry', dtype='int'),
        mqttmaxinflightmsg=prop('mqttmaxinflightmsg', dtype='int'),
        collapse=prop('collapse', dtype='string'),
        rmretdel=prop('rmretdel', dtype='boolean'),
        dateformat=prop('dateformat', dtype='string'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        mqttssl=prop('mqttssl', dtype='boolean'),
        mqttsslcafile=prop('mqttsslcafile', dtype='string'),
        mqttsslcapath=prop('mqttsslcapath', dtype='string'),
        mqttsslcertfile=prop('mqttsslcertfile', dtype='string'),
        mqttsslkeyfile=prop('mqttsslkeyfile', dtype='string'),
        mqttsslpassword=prop('mqttsslpassword', dtype='string'),
        csvincludeschema=prop('csvincludeschema', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        mqttpasswordencrypted=prop('mqttpasswordencrypted', dtype='boolean'),
        addcsvopcode=prop('addcsvopcode', dtype='boolean'),
        addcsvflags=prop('addcsvflags', dtype='string'),
        csvmsgperevent=prop('csvmsgperevent', dtype='int'),
        csvmsgpereventblock=prop('csvmsgpereventblock', dtype='int') 
    )

    def __init__(self, mqtthost=None, mqttclientid=None, mqtttopic=None,
                 mqttqos=None, mqttmsgtype=None,
                 name=None, is_active=None, snapshot=None,
                 mqttuserid=None, mqttpassword=None, mqttport=None,
                 mqttretainmsg=None, mqttdonotcleansession=None,
                 mqttkeepaliveinterval=None, mqttmsgmaxdeliveryattempts=None,
                 mqttmsgdelaydeliveryattempts=None, mqttmsgwaitbeforeretry=None,
                 mqttmaxinflightmsg=None, collapse=None, rmretdel=None,
                 dateformat=None, protofile=None, protomsg=None, mqttssl=None,
                 mqttsslcafile=None, mqttsslcapath=None, mqttsslcertfile=None,
                 mqttsslkeyfile=None, mqttsslpassword=None, csvincludeschema=None,
                 configfilesection=None, mqttpasswordencrypted=None,
                 addcsvopcode=None, addcsvflags=None, csvmsgperevent=None,
                 csvmsgpereventblock=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'mqtt', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['mqtthost', 'mqttclientid',
                                                   'mqtttopic', 'mqttqos',
                                                   'mqttmsgtype'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4],
                   name=name, is_active=is_active, **properties)


class MQTTPublisher(Connector):
    '''
    Subscribe to MQ Telemetry Transport (MQTT)

    Parameters
    ----------
    mqtthost : string
        Specifies the MQTT server host name
    mqttclientid : string
        Specifies the string to use as the MQTT Client ID. If NULL, a random
        client ID is generated. If empty, mqttdonotcleansession must be
        false. Must be unique among all clients connected to the MQTT server.
    mqtttopic : string
        Specifies the string to use as an MQTT subscription topic pattern
    mqttqos : string
        Specifies the requested Quality of Service. Values can be 0, 1 or 2. 
    mqttmsgtype : string
        Specifies binary, CSV, JSON, or opaquestring
    mqttuserid : string, optional
        Specifies the user name required to authenticate the connector’s
        session with the MQTT server.
    mqttpassword : string, optional
        Specifies the password associated with mqttuserid
    mqttport : int, optional
        Specifies the MQTT server port. Default is 1883.
    mqttacceptretainedmsg : boolean, optional
        Sets to true to accept to receive retained message. Default is false.
    mqttcleansession : boolean, optional
        Set to true to instruct the MQTT Server to clean all messages and
        subscriptions on disconnect, false to instruct it to keep them.
        Default is true.
    mqttkeepaliveinterval : int, optional
        Specifies the number of seconds after which the broker should send
        a PING message to the client if no other messages have been
        exchanged in that time. Default is 10.
    publishwithupsert : boolean, optional
        Builds events with opcode=Upsert instead of Insert
    transactional : string, optional
        When mqttmsgtype=CSV, sets the event block type to transactional.
        The default value is normal.
    blocksize : int, optional
        When mqttmsgtype=CSV, specifies the number of events to include in
        a published event block. The default value is 1.
    ignorecsvparseerrors : boolean, optional
        Specifies that when a field in an input CSV event cannot be parsed,
        the event is dropped, an error is logged, and publishing continues.
    csvfielddelimiter : string, optional
        Specifies the character delimiter for field data in input CSV events.
        The default delimiter is the , character.
    noautogenfield : boolean, optional
        Specifies that input events are missing the key field that is
        automatically generated by the Source window.
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields in
        CSV events. The default behavior is these fields are interpreted as
        an integer number of seconds (ESP_DATETIME) or microseconds
        (ESP_TIMESTAMP) since epoch.
    protofile : string, optional
        Specifies the .proto file that contains the Google Protocol Buffers
        message definition used to convert event blocks to protobuf messages.
        When you specify this parameter, you must also specify the protomsg parameter.
    protomsg : string, optional
        Specifies the name of a Google Protocol Buffers message in the .proto
        file that you specified with the protofile parameter. Event blocks
        are converted into this message.
    mqttssl : boolean, optional
        Specifies to use SSL/TLS to connect to the MQTT broker. Default is
        false. In order to use SSL/TLS, the ESP encryption overlay must
        be installed.
    mqttsslcafile : string, optional
        If mqttssl=true, specifies the path to a file containing the PEM
        encoded trusted CA certificate files. Either mqttsslcafile or
        mqttsslcapath must be specified.
    mqttsslcapath : string, optional
        If mqttssl=true, specifies the path to a directory containing the
        PEM encoded trusted CA certificate files. See mosquitto.conf for
        more details about configuring this directory. Either mqttsslcafile
        or mqttsslcapath must be specified.
    mqttsslcertfile : string, optional
        If mqttssl=true, specifies the path to a file containing the PEM
        encoded certificate file for this client. Both mqttsslcertfile and
        mqttsslkeyfile must be provided if one of them is.
    mqttsslkeyfile : string, optional
        If mqttssl=true, specifies the path to a file containing the PEM
        encoded private key for this client. Both mqttsslcertfile and
        mqttsslkeyfile must be provided if one of them is.
    mqttsslpassword : string, optional
        If mqttssl=true, and if key file is encrypted, specifies the
        password for decryption.
    addcsvopcode : boolean, optional
        Prepends an opcode and comma to input CSV events. The opcode is Insert
        unless publishwithupsert is enabled.
    addcsvflags : string, optional
        Specifies the event type to insert into input CSV events (with a comma).
        Valid values are "normal" and "partialupdate".
    configfilesection : string, optional
        Specifies the name of the section in the config file to parse for
        configuration parameters. Specify the value as [configfilesection].
    mqttpasswordencrypted : boolean, optional
        Specifies that mqttpassword is encrypted
    maxevents : int, optional
         Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`MQTTPublisher`

    '''
    connector_key = dict(cls='mqtt', type='publish')

    property_defs = dict(
        mqtthost=prop('mqtthost', dtype='string', required=True),
        mqttclientid=prop('mqttclientid', dtype='string', required=True),
        mqtttopic=prop('mqtttopic', dtype='string', required=True),
        mqttqos=prop('mqttqos', dtype='string', required=True),
        mqttmsgtype=prop('mqttmsgtype', dtype='string', required=True),
        mqttuserid=prop('mqttuserid', dtype='string'),
        mqttpassword=prop('mqttpassword', dtype='string'),
        mqttport=prop('mqttport', dtype='int'),
        mqttacceptretainedmsg=prop('mqttacceptretainedmsg', dtype='boolean'),
        mqttcleansession=prop('mqttcleansession', dtype='boolean'),
        mqttkeepaliveinterval=prop('mqttkeepaliveinterval', dtype='int'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        transactional=prop('transactional', dtype='string'),
        blocksize=prop('blocksize', dtype='int'),
        ignorecsvparseerrors=prop('ignorecsvparseerrors', dtype='boolean'),
        csvfielddelimiter=prop('csvfielddelimiter', dtype='string'),
        noautogenfield=prop('noautogenfield', dtype='boolean'),
        dateformat=prop('dateformat', dtype='string'),
        protofile=prop('protofile', dtype='string'),
        protomsg=prop('protomsg', dtype='string'),
        mqttssl=prop('mqttssl', dtype='boolean'),
        mqttsslcafile=prop('mqttsslcafile', dtype='string'),
        mqttsslcapath=prop('mqttsslcapath', dtype='string'),
        mqttsslcertfile=prop('mqttsslcertfile', dtype='string'),
        mqttsslkeyfile=prop('mqttsslkeyfile', dtype='string'),
        mqttsslpassword=prop('mqttsslpassword', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        addcsvopcode=prop('addcsvopcode', dtype='boolean'),
        addcsvflags=prop('addcsvflags', dtype='string'),
        mqttpasswordencrypted=prop('mqttpasswordencrypted', dtype='boolean'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, mqtthost=None, mqttclientid=None, mqtttopic=None,
                 mqttqos=None, mqttmsgtype=None, name=None, is_active=None,
                 mqttuserid=None, mqttpassword=None, mqttport=None,
                 mqttretainmsg=None, mqttcleansession=None,
                 mqttkeepaliveinterval=None, publishwithupsert=None,
                 transactional=None, blocksize=None, ignorecsvparseerrors=None,
                 csvfielddelimiter=None, noautogenfield=None,
                 dateformat=None, protofile=None, protomsg=None,
                 mqttssl=None, mqttsslcafile=None, mqttsslcapath=None,
                 mqttsslcertfile=None, mqttsslkeyfile=None,
                 mqttsslpassword=None, configfilesection=None,
                 addcsvopcode=None, addcsvflags=None,
                 mqttpasswordencrypted=None, maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'mqtt', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['mqtthost', 'mqttclientid',
                                                   'mqtttopic', 'mqttqos',
                                                   'mqttmsgtype'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4],
                   name=name, is_active=is_active, **properties)
