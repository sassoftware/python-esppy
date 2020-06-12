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

''' ESP UVC Connector '''

from __future__ import print_function, division, absolute_import, unicode_literals

import numbers
import re
import six
from .base import Connector, prop, map_properties
from ..utils import xml
from ..utils.data import gen_name


class UVCPublisher(Connector):
    '''
    Publish photos taken by a V4L2 compatible

    Parameters
    ----------
    frame_rate : float, optional
        Specifies the frames per second that the camera streams.
        Must be a double. The default value is 15.
    format_in : string, optional
        Specifies the image format of captured photos.
        The default is jpeg. yuyv, an uncompressed image format,
        is also supported.
    format_out : string, optional
        Specifies the image format that the connector publishes.
        The default is jpeg. yuyv, an uncompressed image format,
        is supported only when format_in is yuyv.
    width : int, optional
        Specifies the height of the photo.
    height : int, optional
        Specifies the width of the photo.
    brightness : string, optional
        Specifies the brightness of the photo.
    gain : string, optional
        Specifies the gain of the photo.
    saturation : string, optional
        Specifies the saturation of the photo.
    contrast : string, optional
        Specifies the contrast of the photo.
    device : string, optional
        Specifies the device name the camera is using on the
        Linux operating system.
    blocking : boolean, optional
        Specifies whether the connector is in blocking mode.
    predelay : int, optional
        Specifies a delay time, in seconds, on starting
        the connector.
    maxevents : int, optional
        Specifies the maximum number of events to publish.
    cameraid : string, optional
        Specifies an arbitrary string that is copied into the corresponding string field in the Source window. This value can be used by the model to identify the source camera.

    Returns
    -------
    :class:`UVCPublisher`

    '''
    connector_key = dict(cls='uvc', type='publish')

    property_defs = dict(
        frame_rate=prop('frame_rate', dtype='float'),
        format_in=prop('format_in', dtype='string'),
        format_out=prop('format_out', dtype='string'),
        width=prop('width', dtype='int'),
        height=prop('height', dtype='int'),
        brightness=prop('brightness', dtype='string'),
        gain=prop('gain', dtype='string'),
        saturation=prop('saturation', dtype='string'),
        contrast=prop('contrast', dtype='string'),
        device=prop('device', dtype='string'),
        blocking=prop('blocking', dtype='boolean'),
        predelay=prop('predelay', dtype='int'),
        maxevents=prop('maxevents', dtype='int'),
        cameraid=prop('cameraid', dtype='string')
    )

    def __init__(self, name=None, is_active=None,
                 frame_rate=None, format_in=None, format_out=None,
                 width=None, height=None, brightness=None,
                 gain=None, saturation=None, contrast=None,
                 device=None, blocking=None, predelay=None,
                 maxevents=None, cameraid=None):
        params = dict(**locals())
        params.pop('is_active')
        params.pop('self')
        name = params.pop('name')
        Connector.__init__(self, 'uvc', name=name, type='publish',
                           is_active=is_active, properties=params)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        properties = map_properties(cls, properties, delete='type')
        return cls(name=name, is_active=is_active, **properties)
