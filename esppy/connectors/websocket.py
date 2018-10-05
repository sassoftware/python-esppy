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

''' ESP Websocket Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class WebSocketPublisher(Connector):
    '''
    Publish websocket events

    Parameters
    ----------
    url : string
        Specifies the URL for the WebSocket connection. 
    configUrl : string
        Specifies the URL for the connector configuration file.
        This configuration file contains information about the
        transformation steps required to publish events.
    contentType : string
        Specifies XML or JSON as the type of content received
        over the WebSocket connection.
    sslCertificate : string, optional
        Specifies the location of the SSL certificate to use
        when connecting to a secure server.
    sslPassphrase : string, optional
        Specifies the password for the SSL certificate.
    requestHeaders : string, optional
        Specifies a comma-separated list of request headers to
        send to the server. The list must consist of
        name-value pairs in name:value format.
    maxevents : int, optional
         Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`WebSocketPublisher`

    '''
    connector_key = dict(cls='websocket', type='publish')

    property_defs = dict(
        url=prop('url', dtype='string', required=True),
        configUrl=prop('configUrl', dtype='string', required=True),
        contentType=prop('contentType', dtype='string', required=True),
        sslCertificate=prop('sslCertificate', dtype='string'),
        sslPassphrase=prop('sslPassphrase', dtype='string'),
        requestHeaders=prop('requestHeaders', dtype='string'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, url, configUrl, contentType, name=None, is_active=None,
                 sslCertificate=None, sslPassphrase=None, requestHeaders=None,
                 maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'websocket', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['url',
                                                   'configUrl',
                                                   'contentType'],
                                         delete='type')
        return cls(req[0], req[1], req[2], name=name, is_active=is_active, **properties)
