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

''' ESP Connectors '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import six
import xml.etree.ElementTree as ET
from six.moves import urllib
from .base import ESPObject, attribute
from .utils.rest import get_params
from .utils.data import get_project_data

StringParameter = collections.namedtuple('StringParameter', ['name', 'default'])
SelectParameter = collections.namedtuple('SelectParameter', ['name', 'default', 'values'])
BooleanParameter = collections.namedtuple('BoolParameter', ['name', 'default'])
NumericParameter = collections.namedtuple('NumberParameter', ['name', 'default'])


class ConnectorInfo(ESPObject):
    '''
    Base class for all ESP connectors

    Attributes
    ----------
    name : string
        Name of the algorithm
    required_params : dict
        Dictionary of required parameters
    optional_params : dict
        Dictionary of optional parameters

    Parameters
    ----------
    label : string
        Label for the algorithm

    '''

    label = attribute('label', dtype='string')
    type = attribute('type', dtype='string')

    def __init__(self, label, pubsub=None, type=None):
        ESPObject.__init__(self)
        self.label = label
        self.pubsub = pubsub
        self.type = type
        self.required_params = {}
        self.optional_params = {}

    @classmethod
    def from_xml(cls, data, session=None):
        '''
        Create algorithm object from XML definition

        Parameters
        ----------
        data : string
            XML string
        session : requests.Session, optional
            Session the connector is associated with

        Returns
        -------
        :class:`Connector`

        '''
        out = cls('')
        out.session = session

        out._set_attributes(data.attrib)

        for ptype, params in [('required', out.required_params),
                              ('optional', out.optional_params)]:
            for item in data.findall('./%s-parms/parm' % ptype):
                name = item.attrib['key']
                if item.find('./default') is not None:
                    default = item.find('./default').text
                    if item.find('./allowed-values') is not None:
                        if default in ['true', 'false']:
                            params[name] = BooleanParameter(name,
                                           (default == 'true') and True or False)
                        else:
                            params[name] = SelectParameter(name, default,
                                                           [x.text for x in
                                                            item.findall('.//allowed-value')])
                    elif re.match(r'^\d*\.\d+$', default):
                        params[name] = NumericParameter(name, float(default))
                    elif re.match(r'^\d+$', default):
                        params[name] = NumericParameter(name, int(default))
                    else:
                        params[name] = StringParameter(name, default)
                else:
                    params[name] = StringParameter(name, '')

        return out

    def __str__(self):
        attrs = ['label=%s' % repr(self.label)]
        if self.type:
            attrs.append('type=%s' % repr(self.type))
        if self.pubsub:
            attrs.append('pubsub=%s' % repr(self.pubsub))
        return '%s(%s)' % (type(self).__name__, ', '.join(attrs))

    def __repr__(self):
        return str(self)
