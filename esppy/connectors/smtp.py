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

''' ESP SMTP Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class SMTPSubscriber(Connector):
    '''
    Subscribe to Simple Mail Transfer Protocol (SMTP) events

    Parameters
    ----------
    smtpserver : string
       Specifies the SMTP server host name or IP address
    sourceaddress : string
        Specifies the e-mail address to be used in the “from” field of
        the e-mail.
    destaddress : string
        Specifies the e-mail address to which to send the e-mail message
    snapshot : boolean, optional
        Specifies whether to send snapshot data
    collapse : string, optional
        Enables conversion of UPDATE_BLOCK events to make subscriber
        output publishable. The default value is disabled.
    emailperevent : boolean, optional
        Specifies true or false. The default is false. If false, each
        e-mail body contains a full event block. If true, each mail
        body contains a single event.
    rmretdel : boolean, optional
        Specifies to remove all delete events from event blocks
        received by a subscriber that were introduced by a window
        retention policy.
    configfilesection : string, optional
        Specifies the name of the section in the connoctor config file
        to parse for configuration parameters. Specify the value
        as [configfilesection].
    dateformat : string, optional
        Specifies the format of ESP_DATETIME and ESP_TIMESTAMP fields
        in CSV events. The default behavior is these fields are
        interpreted as an integer number of seconds (ESP_DATETIME)
        or microseconds (ESP_TIMESTAMP) since epoch.

    Returns
    -------
    :class:`SMTPSubscriber`

    '''
    connector_key = dict(cls='smtp', type='subscribe')

    property_defs = dict(
        smtpserver=prop('smtpserver', dtype='string', required=True),
        sourceaddress=prop('sourceaddress', dtype='string', required=True),
        destaddress=prop('destaddress', dtype='string', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        collapse=prop('collapse', dtype='string'),
        emailperevent=prop('emailperevent', dtype='boolean'),
        rmretdel=prop('rmretdel', dtype='boolean'),
        configfilesection=prop('configfilesection', dtype='string'),
        dateformat=prop('dateformat', dtype='string')
    )

    def __init__(self, smtpserver=None, sourceaddress=None, destaddress=None,
                 name=None, is_active=None, snapshot=None,
                 collapse=None, emailperevent=None,
                 rmretdel=None, configfilesection=None, dateformat=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'smtp', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['smtpserver',
                                                   'sourceaddress',
                                                   'destaddress'],
                                         delete='type')
        return cls(req[0], req[1], req[2], name=name, is_active=is_active, **properties)
