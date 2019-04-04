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

''' ESP Algorithms '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import six
import xml.etree.ElementTree as ET
from six.moves import urllib
from .base import ESPObject, attribute
from .utils import xml
from .utils.rest import get_params
from .utils.data import get_project_data


class Algorithm(ESPObject):
    '''
    Base class for all ESP algorithms

    Attributes
    ----------
    name : string
        Name of the algorithm
    parameters : dict
        Dictionary of algorithm parameters
    input_map : dict
        Dictionary of input map entry definitions
    output_map : dict
        Dictionary of output map entry definitions

    Parameters
    ----------
    name : string
        Name of the algorithm

    '''

    reference = attribute('reference', dtype='string')
    algorithm_type = attribute('algorithm-type', dtype='string')
    type = attribute('type', dtype='string')

    def __init__(self, name=None, reference=None, algorithm_type=None, type=None):
        ESPObject.__init__(self)
        self.name = name
        self.algorithm_type = algorithm_type
        self.type = type
        self.reference = reference
        self.parameters = {}
        self.input_map = {}
        self.output_map = {}

    @classmethod
    def from_xml(cls, data, session=None):
        '''
        Create algorithm object from XML definition

        Parameters
        ----------
        data : string
            XML string
        session : requests.Session, optional
            Session the algorithm is associated with

        Returns
        -------
        :class:`Algorithm`

        '''
        if isinstance(data, six.string_types):
            data = xml.from_xml(data) 

        out = cls()
        out.session = session

        out._set_attributes(data.attrib)

        params = out.parameters
        for item in data.findall('./parameters/parameter'):
            attrs = dict(item.attrib)
            name = attrs.pop('name', None)
            params[name] = attrs
            dtype = attrs.get('type', '')
            default = attrs.get('default', None)
            if default is not None:
                if dtype.startswith('int'):
                    params[name]['default'] = int(default)
                elif dtype == 'double':
                    params[name]['default'] = float(default)
                elif dtype == 'boolean':
                    params[name]['default'] = default in ['1', 'true'] and True or False
                else:
                    params[name]['default'] = default.strip()

        input_map = out.input_map
        for item in data.findall('./input-map/input-map-entry'):
            attrs = dict(item.attrib)
            name = attrs.pop('name', None)
            input_map[name] = attrs

        output_map = out.output_map
        for item in data.findall('./output-map/output-map-entry'):
            attrs = dict(item.attrib)
            name = attrs.pop('name', None)
            output_map[name] = attrs

        return out

    def __str__(self):
        attrs = []
        if self.name:
            attrs.append('name=%s' % repr(self.name))
        if self.reference:
            attrs.append('reference=%s' % repr(self.reference))
        if self.type:
            attrs.append('type=%s' % repr(self.type))
        if self.algorithm_type:
            attrs.append('algorithm_type=%s' % repr(self.algorithm_type))
        if self.parameters:
            attrs.append('parameters=%s' % list(sorted(self.parameters)))
        if self.input_map:
            attrs.append('input_map=%s' % list(sorted(self.input_map)))
        if self.output_map:
            attrs.append('output_map=%s' % list(sorted(self.output_map)))
        return '%s(%s)' % (type(self).__name__, ', '.join(attrs))

    def __repr__(self):
        return str(self)
