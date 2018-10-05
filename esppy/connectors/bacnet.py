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

''' ESP Bacnet Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class BacnetPublisher(Connector):
    '''
    Publish Bacnet events

    Parameters
    ----------
    bacnetbbmdaddress : string
        Specifies the IP address of the BBMD
    bacnetbbmdport : int
        Specifies the port of the BBMD
    bacnetconfigfile : string
        Specifies the JSON configuration file containing
        Bacnet device and object
    bacnetipport : int, optional
        Specifies the local port used by the connector.
        The default port number is 47808.
    blocksize : int, optional
        Specifies the number of events to include in a published
        event block. The default value is 1.
    configfilesection : string, optional
        Specifies the name of the section in the connector config
        file to parse for configuration parameters. Specify the
        value as [configfilesection].
    ignoretimeouts : string, optional
        Logs a warning and continues if an attempt to read a
        property from a Bacnet device results in a timeout.
        The default is to log an error and stop.
    publishwithupsert : boolean, optional
        Builds events with opcode=Upsert instead of Insert.
    maxevents : int, optional
        Specifies the maximum number of events to publish.
    transactional : string, optional
        Sets the event block type to transactional. The default
        value is normal.

    Returns
    -------
    :class:`BacnetPublisher`

    '''
    connector_key = dict(cls='bacnet', type='publish')

    property_defs = dict(
        bacnetbbmdaddress=prop('bacnetbbmdaddress', dtype='string', required=True),
        bacnetbbmdport=prop('bacnetbbmdport', dtype='int', required=True),
        bacnetconfigfile=prop('bacnetconfigfile', dtype='string', required=True),
        bacnetipport=prop('bacnetipport', dtype='int'),
        blocksize=prop('blocksize', dtype='int'),
        configfilesection=prop('configfilesection', dtype='string'),
        ignoretimeouts=prop('ignoretimeouts', dtype='boolean'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        maxevents=prop('maxevents', dtype='int'),
        transactional=prop('transactional', dtype='string')
    )

    def __init__(self, bacnetbbmdaddress=None, bacnetbbmdport=None,
                 bacnetconfigfile=None, name=None, is_active=None,
                 bacnetipport=None, blocksize=None, configfilesection=None,
                 ignoretimeouts=None, publishwithupsert=None,
                 maxevents=None, transactional=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'bacnet', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['bacnetbbmdaddress',
                                                   'bacnetbbmdport',
                                                   'bacnetconfigfile'],
                                         delete='type')
        return cls(req[0], req[1], req[2], name=name, is_active=is_active, **properties)
