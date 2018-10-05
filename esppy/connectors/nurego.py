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

''' ESP Nurego Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class NuregoSubscriber(Connector):
    '''
    Subscribe to Nurego metering window

    Parameters
    ----------
    serviceurl : string
        Specifies the target Nurego REST service URL
    certificate : string
        Specifies the full path and filename of the client certificate that
        is used to establish the HTTPS connection to the Nurego REST service.
    username : string
        Specifies the user name to use in requests to Nurego for a new token
    password : string
        Specifies the password to use in requests to Nurego for a new token
    instanceid : string
        Specifies the instance ID to use in requests to Nurego for a new token
    certpassword : string, optional
        Specifies the password associated with the client certificate that
        is configured in certificate.
    collapse : string, optional
        Enables conversion of UPDATE_BLOCK events to make subscriber output
        publishable. The default value is disabled.
    rmretdel : boolean, optional
        Specifies to remove all delete events from event blocks received by
        a subscriber that were introduced by a window retention policy.
    configfilesection : string, optional
        Specifies the name of the section in the config file to parse for
        configuration parameters. Specify the value as [configfilesection].

    Returns
    -------
    :class:`NuregoSubscriber`

    '''
    connector_key = dict(cls='nurego', type='subscribe')

    property_defs = dict(
        serviceurl=prop('serviceurl', dtype='string', required=True),
        certificate=prop('certificate', dtype='string', required=True),
        username=prop('username', dtype='string', required=True),
        password=prop('password', dtype='string', required=True),
        instanceid=prop('instanceid', dtype='string', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        certpassword=prop('certpassword', dtype='string'),
        collapse=prop('collapse', dtype='string'),
        rmretdel=prop('rmretdel', dtype='boolean'),
        configfilesection=prop('configfilesection', dtype='string')
    )

    def __init__(self, serviceurl=None, certificate=None, username=None,
                 password=None, instanceid=None,
                 name=None, is_active=None, snapshot=None,
                 certpassword=None, collapse=None, rmretdel=None,
                 configfilesection=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'nurego', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['serviceurl', 'certificate',
                                                   'username', 'password',
                                                   'instanceid'],
                                         delete='type')
        return cls(req[0], req[1], req[2], req[3], req[4],
                   name=name, is_active=is_active, **properties)
