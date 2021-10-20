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

''' ESP Base Classes '''

from __future__ import print_function, division, absolute_import, unicode_literals

import six
import weakref
from .utils.rest import RESTHelpers


def attribute(name, dtype=None, values=None):
    '''
    Create an XML attribute-based property

    Parameters
    ----------
    name : string
        The name of the XML attribute
    dtype : string, optional
        The data type of the attribute
        Valid values: int, float, double, string, bool
        Default: string
    values : list or dict, optional
        The valid values of the attribute.  If it is a list, the values
        in the list are used in both the instance attribute and the XML.
        If a dictionary is specified, the keys are the instance attribute
        value and the values are the XML values.

    Returns
    -------
    :class:`Attribute`

    '''
    return Attribute(name, dtype=dtype, values=values)


class Attribute(object):
    '''
    XML attribute-based property

    Parameters
    ----------
    name : string
        The name of the XML attribute
    dtype : string, optional
        The data type of the attribute
        Valid values: int, float, double, string, bool
        Default: string
    values : list or dict, optional
        The valid values of the attribute.  If it is a list, the values
        in the list are used in both the instance attribute and the XML.
        If a dictionary is specified, the keys are the instance attribute
        value and the values are the XML values.

    Returns
    -------
    :class:`Attribute`

    '''

    def __init__(self, name, dtype=None, values=None):
        self.name = name
        self.dtype = dtype or 'string'
        self.values = values
        self.value = weakref.WeakKeyDictionary()

    def get_xml_value(self, instance, owner=None):
        if instance is None:
            instance = self

        value = self.value.get(instance, None)
        if value is not None:

            if isinstance(self.values, dict):
                value = self.values[value]

            dtype = self.dtype
            if dtype == 'bool':
                value = value and 'true' or 'false'
            else:
                value = '%s' % value

        return value

    def get_value(self, instance, owner=None):
        if instance is None:
            instance = self
        return self.value.get(instance, None)

    def __get__(self, instance, owner=None):
        if instance is None:
            instance = self
        return self.value.get(instance, None)

    def __set__(self, instance, value):
        if instance is None:
            instance = self

        dtype = self.dtype

        if value is None:
            self.value[instance] = None
            return

        if dtype == 'int':
            value = int(value)

        elif dtype in ['double', 'float']:
            value = float(value)

        elif dtype == 'bool':
            if isinstance(value, six.string_types):
                value = (value.lower() == 'true')
            else:
                value = bool(value)

        elif dtype == 'string':
            value = '%s' % value

        if self.values:
            if isinstance(self.values, (tuple, list, set)):
                if value not in self.values:
                    raise ValueError('%s is not one of %s' % (value, self.values))
            elif isinstance(self.values, dict):
                if value not in self.values:
                    found = False
                    for key, val in self.values.items():
                        if value == val:
                            value = key
                            found = True
                            break
                    if not found:
                        raise ValueError('%s is not one of %s' %
                                         (value, list(self.values.keys())))

        self.value[instance] = value

    def __delete__(self, instance):
        if instance is None:
            instance = self
        self.value[instance] = None


class ESPObject(RESTHelpers):
    ''' Base class for all ESP objects '''

    def __init__(self, session=None, attrs=None):
        RESTHelpers.__init__(self, session=session)
        self._set_attributes(attrs)

    def _set_attributes(self, kwargs):
        kwargs = kwargs or {}
        xml_map = dict(getattr(type(self), 'xml_map', {}))

        # Always add these keys
        xml_map['name'] = 'name'
        xml_map['contquery'] = 'contquery'
        xml_map['project'] = 'project'

        attrs = {}

        for cls in reversed(type(self).__mro__):
            for key, value in vars(cls).items():
                if isinstance(value, Attribute):
                    attrs[key] = key
                    attrs[value.name] = key

        for key, value in kwargs.items():
            if value is not None and key in attrs:
                setattr(self, attrs[key], value)
            elif value is not None and key in xml_map:
                setattr(self, xml_map[key], value)

    def _get_attributes(self, use_xml_values=True):
        xml_map = dict(getattr(type(self), 'xml_map', {}))

        # Always add these keys
        xml_map['name'] = 'name'
        xml_map['contquery'] = 'contquery'
        xml_map['project'] = 'project'

        out = {}

        for cls in reversed(type(self).__mro__):
            for key, value in vars(cls).items():
                if isinstance(value, Attribute):
                    if use_xml_values:
                        val = value.get_xml_value(self)
                        if val is not None:
                            out[value.name] = val
                    else:
                        val = value.get_value(self)
                        if val is not None:
                            out[key] = val

        for attr_name, xml_name in xml_map.items():
            value = getattr(self, attr_name, None)
            if value is not None:
                if use_xml_values:
                    if type(value) is bool:
                        out[xml_name] = value and 'true' or 'false'
                    else:
                        out[xml_name] = '%s' % value
                else:
                    out[attr_name] = value

        return out

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return hash(other) == hash(self)
