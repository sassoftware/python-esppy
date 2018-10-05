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

''' ESP Project Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class ProjectPublisher(Connector):
    '''
    ESP window event publisher

    Parameters
    ----------
    srcproject : string
        Specifies the name of the source project
    srccontinuousquery : string
        Specifies the name of the source continuous query
    srcwindow : string
        Specifies the name of the Source window
    maxevents : int, optional
        Specifies the maximum number of events to publish
    configfilesection : string, optional
        Specifies the name of the section in the config file to parse
        for configuration parameters. Specify the value as [configfilesection].

    Returns
    -------
    :class:`ProjectPublisher`

    '''
    connector_key = dict(cls='project', type='publish')

    property_defs = dict(
        srcproject=prop('srcproject', dtype='string', required=True),
        srccontinuousquery=prop('srccontinuousquery', dtype='string', required=True),
        srcwindow=prop('srcwindow', dtype='string', required=True),
        maxevents=prop('maxevents', dtype='int'),
        configfilesection=prop('configfilesection', dtype='string')
    )

    def __init__(self, srcproject=None, srccontinuousquery=None, srcwindow=None,
                 maxevents=None, configfilesection=None, name=None, is_active=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'project', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['srcproject',
                                                   'srccontinuousquery',
                                                   'srcwindow'],
                                         delete='type')
        return cls(req[0], req[1], req[2], name=name, is_active=is_active, **properties)
