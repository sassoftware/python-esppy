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

''' ESP Pylon Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class PylonPublisher(Connector):
    '''
    Publish Basler GigE camera captured frames

    Parameters
    ----------
    cameraipaddress : string, optional
         Specifies the camera IP address. The default value is the
         address of the first camera found on the local subnet.
    maxnumframes : int, optional
        Specifies the maximum number of frames to publish. The default
        value is no maximum.
    maxframerate : int, optional
        Specifies the maximum number of frames per second to publish.
        The default value is the rate at which frames are received
        from the camera.
    camerafeaturesfile : string, optional
        Specifies a Pylon Features Stream (.pfs) configuration file
        to load. The default is to use the current camera
        configuration unmodified.
    camerawidth : int, optional
        Specifies the Area-Of-Interest width. The default value is
        the value in the current camera configuration.
    cameraheight : int, optional
        Specifies the Area-Of-Interest height. The default value is
        the value in the current camera configuration.
    camerapixelformat : string, optional
        Specifies the image pixel format. The default value is the
        format in the current camera configuration.
    camerapacketsize : int, optional
        Specifies the Ethernet packet size. The default value is
        the value in the current camera configuration.
    transactional : string, optional
        Sets the event block type to transactional. The default
        value is normal.
    configfilesection : string, optional
        Specifies the name of the section in the connector config
        file to parse for configuration parameters. Specify the
        value as [configfilesection].
    publishwithupsert : boolean, optional
        Specifies to build events with opcode=Upsert instead
        of opcode=Insert
    cameraxoffset : int, optional
        Specifies the Area-Of-Interest horizontal offset. The
        default value is the value in the current camera configuration.
    camerayoffset : int, optional
        Specifies the Area-Of-Interest vertical offset. The
        default value is the value in the current camera configuration.
    maxevents : int, optional
        Specifies the maximum number of events to publish.

    Returns
    -------
    :class:`PylonPublisher`

    '''
    connector_key = dict(cls='pylon', type='publish')

    property_defs = dict(
        cameraipaddress=prop('cameraipaddress', dtype='string'),
        maxnumframes=prop('maxnumframes', dtype='int'),
        maxframerate=prop('maxframerate', dtype='int'),
        camerafeaturesfile=prop('camerafeaturesfile', dtype='string'),
        camerawidth=prop('camerawidth', dtype='int'),
        cameraheight=prop('cameraheight', dtype='int'),
        camerapixelformat=prop('camerapixelformat', dtype='string'),
        camerapacketsize=prop('camerapacketsize', dtype='int'),
        transactional=prop('transactional', dtype='string'),
        configfilesection=prop('configfilesection', dtype='string'),
        publishwithupsert=prop('publishwithupsert', dtype='boolean'),
        cameraxoffset=prop('cameraxoffset', dtype='int'),
        camerayoffset=prop('camerayoffset', dtype='int'),
        maxevents=prop('maxevents', dtype='int')
    )

    def __init__(self, name=None, is_active=None,
                 cameraipaddress=None,
                 maxnumframes=None, maxframerate=None,
                 camerafeaturesfile=None, camerawidth=None,
                 cameraheight=None, camerapixelformat=None,
                 camerapacketsize=None, transactional=None,
                 configfilesection=None, publishwithupsert=None,
                 cameraxoffset=None, camerayoffset=None,
                 maxevents=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'pylon', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        properties = map_properties(cls, properties, delete='type')
        return cls(name=name, is_active=is_active, **properties)
