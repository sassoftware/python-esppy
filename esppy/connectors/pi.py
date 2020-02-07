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

''' ESP PI Asset Framework Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class PISubscriber(Connector):
    '''
    Subscribe to operations from a PI Asset Framework (AF) server

    Parameters
    ----------
    afelement : string
        Specifies the AF element or element template name.
        Wildcards are supported.
    iselementtemplate : boolean
        Specifies whether the afelement parameter is an element template
        name. By default, the afelement parameter specifies an element name.
    snapshot : boolean, optional
        Specifies whether to send snapshot data
    rmretdel : boolean, optional
        Removes all delete events from event blocks received by the
        subscriber that were introduced by a window retention policy.
    pisystem : string, optional
        Specifies the PI system. The default is the PI system that is
        configured in the PI AF client.
    afdatabase : string, optional
        Specifies the AF database. The default is the AF database that is
        configured in the PI AF client.
    afrootelement : string, optional
        Specifies the root element in the AF hierarchy from which to search
        for parameter afelement. The default is the top-level element
        in the AF database.
    afattribute : string, optional
        Specifies a specific attribute in the element. The default is
        all attributes in the element.
    configfilesection : string, optional
        Specifies the name of the section in the config file to parse for
        configuration parameters. Specify the value as [configfilesection].

    Returns
    -------
    :class:`PISubscriber`

    '''
    connector_key = dict(cls='pi', type='subscribe')

    property_defs = dict(
        afelement=prop('afelement', dtype='string', required=True),
        iselementtemplate=prop('iselementtemplate', dtype='boolean', required=True),
        snapshot=prop('snapshot', dtype='boolean', required=True, default=False),
        rmretdel=prop('rmretdel', dtype='boolear'),
        pisystem=prop('pisystem', dtype='string'),
        afdatabase=prop('afdatabase', dtype='string'),
        afrootelement=prop('afrootelement', dtype='string'),
        afattribute=prop('afattribute', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string')
    )

    def __init__(self, afelement=None, iselementtemplate=None, name=None, is_active=None,
                 snapshot=None, rmretdel=None, pisystem=None,
                 afdatabase=None, afrootelement=None, afattribute=None,
                 configfilesection=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'pi', name=name, type='subscribe',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['afelement',
                                                   'iselementtemplate'],
                                         delete='type')
        return cls(req[0], req[1], name=name, is_active=is_active, **properties)


class PIPublisher(Connector):
    '''
    Publish operations to a PI Asset Framework (AF) server

    Parameters
    ----------
    afelement : string
        Specifies the AF element or element template name.
        Wildcards are supported.
    iselementtemplate : boolean
        Specifies that the afelement parameter is an element template name.
        By default, the afelement parameter specifies an element name.
    blocksize : int, optional
        Specifies the number of events to include in a published event
        block. The default value is 1.
    transactional : string, optional
        Sets the event block type to transactional. The default value is normal
    pisystem : string, optional
        Specifies the PI system. The default is the PI system that is
        configured in the PI AF client.
    afdatabase : string, optional
        Specifies the AF database. The default is the AF database that is
        configured in the PI AF client.
    afrootelement : string, optional
        Specifies the root element in the AF hierarchy from which to search
        for the parameter afelement. The default is the top-level element
        in the AF database.
    afattribute : string, optional
        Specifies a specific attribute in the element. The default is all
        attributes in the element.
    archivetimestamp : boolean, optional
        Specifies that all archived values from the specified timestamp
        onwards are to be published when connecting to the PI system.
        The default is to publish only new values.
    configfilesection : string, optional
        Specifies the name of the section in the config file to parse for
        configuration parameters. Specify the value as [configfilesection].
    publishwithupsert : boolean, optional
        Builds events with opcode=Upsert instead of Insert.
    maxevents : int, optional
         Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`PIPublisher`

    '''
    connector_key = dict(cls='pi', type='publish')

    property_defs = dict(
        afelement=prop('afelement', dtype='string', required=True),
        iselementtemplate=prop('iselementtemplate', dtype='boolean', required=True),
        blocksize=prop('blocksize', dtype='int'),
        transactional=prop('transactional', dtype='string'),
        pisystem=prop('pisystem', dtype='string'),
        afdatabase=prop('afdatabase', dtype='string'),
        afrootelement=prop('afrootelement', dtype='string'),
        afattribute=prop('afattribute', dtype='string'),
        archivetimestamp=prop('archivetimestamp', dtype='boolean'),
        configfilesection=prop('configfilesection', dtype='string'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        allvaluestostrings=prop('allvaluestostrings', dtype='boolean'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, afelement=None, iselementtemplate=None, name=None, is_active=None,
                 blocksize=None, transactional=None, pisystem=None,
                 afdatabase=None, afrootelement=None, afattribute=None,
                 archivetimestamp=None, configfilesection=None,
                 publishwithupsert=None, allvaluestostrings=None, maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'pi', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        req, properties = map_properties(cls, properties,
                                         required=['afelement',
                                                   'iselementtemplate'],
                                         delete='type')
        return cls(req[0], req[1], name=name, is_active=is_active, **properties)
