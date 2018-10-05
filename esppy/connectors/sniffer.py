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

''' ESP Sniffer Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class SnifferPublisher(Connector):
    '''
    Publish local area network packet events

    Parameters
    ----------
    interface : string
        Specifies the name of the network interface on the local
        machine from which to capture packets.
    protocol : string
        Specifies the port number associated with the protocol type
        of packets to be captured. You can specify this as a
        comma-separated list of port numbers.
    packetfields : string
        Specifies the packet fields to be extracted from a captured
        packet and included in the published event.
    transactional : string, optional
        Sets the event block type to transactional. The default
        value is normal.
    blocksize : int, optional
        Specifies the number of events to include in a published
        event block. The default value is 1.
    addtimestamp : boolean, optional
        Specifies to append an ESP_TIMESTAMP field to each published event.
    configfilesection : string, optional
        Specifies the name of the section in the configuration file
        to parse for configuration parameters. Specify the value
        as [configfilesection].
    vendorid : string, optional
        Specifies the vendor-Id field to match when capturing the
        Attribute-Specific field in a Vendor-Specific attribute in a
        Radius Accounting-Request packet.
    vendortype : string, optional
        Specifies the vendor-Type field to match when capturing the
        Attribute-Specific field in a Vendor-Specific attribute in a
        Radius Accounting-Request packet.
    indexfieldname : string, optional
        Specifies the name to use instead of index for the index:int64
        field in the Source window schema.
    publishwithupsert : boolean, optional
        Specifies to build events with opcode = Upsert instead of Insert.
    pcapfilter : string, optional
        Specifies a filter expression as defined in the pcap
        documentation. Passed to the pcap driver to filter packets
        received by the connector.
    httpports : string, optional
        Specifies a comma-separated list of destination ports. All
        sniffed packets that contain a specified port are parsed for
        HTTP GET parameters. The default value is 80.
    ignorenopayloadpackets : boolean, optional
        Specifies whether to ignore packets with no payload, as
        calculated by subtracting the TCP or UDP header size from the
        packet size. The default value is FALSE.
    maxevents : int, optional
        Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`SnifferPublisher`

    '''
    connector_key = dict(cls='sniffer', type='publish')

    property_defs = dict(
        interface=prop('interface', dtype='string', required=True),
        protocol=prop('interface', dtype='string', required=True),
        packetfields=prop('packetfields', dtype='string', required=True),
        transactional=prop('transactional', dtype='string'),
        blocksize=prop('blocksize', dtype='int'),
        addtimestamp=prop('addtimestamp', dtype='boolean'),
        configfilesection=prop('configfilesection', dtype='string'),
        vendorid=prop('vendorid', dtype='string'),
        vendortype=prop('vendortype', dtype='string'),
        indexfieldname=prop('indexfieldname', dtype='string'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        pcapfilter=prop('pcapfilter', dtype='string'),
        httpports=prop('httpports', dtype='string'),
        ignorenopayloadpackets=prop('ignorenopayloadpackets', dtype='boolean'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, interface=None, protocol=None, packetfields=None, name=None,
                 is_active=None, transactional=None, blocksize=None,
                 addtimestamp=None, configfilesection=None, vendorid=None,
                 vendortype=None, indexfieldname=None, publishwithupsert=None,
                 pcapfilter=None, httpports=None, ignorenopayloadpackets=None,
                 maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'sniffer', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['interface',
                                                   'protocol',
                                                   'packetfields'],
                                         delete='type')
        return cls(req[0], req[1], req[2], name=name, is_active=is_active, **properties)
