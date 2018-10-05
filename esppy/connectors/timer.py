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

''' ESP Timer Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class TimerPublisher(Connector):
    '''
    Publish events on regular intervals

    Parameters
    ----------
    basetime : string
        Specifies the start time in the format defined by the
        timeformat parameter.
    interval : float
        Specifies the interval length in units defined by the unit
        parameter.
    unit : string
        Specifies the unit of the interval parameter. Units include
        second | minute | hour | day | week | month | year.
    label : string, optional
        Specifies the string to be written to the source window
        'label' field. The default value is the connector name.
    timeformat : string, optional
        Specifies the format of the basetime parameter. The default
        value is %Y%m-%d %H:%M:%S.
    transactional : string, optional
        Sets the event block type to transactional. The default
        value is normal.
    configfilesection : string, optional
        Specifies the name of the section in the connector config
        file to parse for configuration parameters. Specify the
        value as [configfilesection].
    publishwithupsert : boolean, optional
        Specifies to build events with opcode = Upsert instead of
        opcode = Insert.
    maxevents : int, optional
        Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`TimerPublisher`

    '''
    connector_key = dict(cls='timer', type='publish')

    property_defs = dict(
        basetime=prop('basetime', dtype='string', required=True),
        interval=prop('interval', dtype='float', required=True),
        unit=prop('unit', dtype='sctring', required=True),
        label=prop('label', dtype='string'),
        timeformat=prop('timeformat', dtype='string'),
        transactional=prop('transactional', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, basetime=None, interval=None, unit=None,
                 name=None, is_active=None, label=None,
                 timeformat=None, transactional=None,
                 configfilesection=None, publishwithupsert=None,
                 maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'timer', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['basetime',
                                                   'interval',
                                                   'unit'],
                                         delete='type')
        return cls(req[0], req[1], req[2], name=name, is_active=is_active, **properties)
