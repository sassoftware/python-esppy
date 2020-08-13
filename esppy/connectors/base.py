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

''' ESP Connector Utilities '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import numbers
import re
import six
from ..base import ESPObject
from ..utils import xml
from ..utils.data import gen_name


RegexType = type(re.compile(r''))


def listify(obj):
    if obj is None:
        return
    if isinstance(obj, (tuple, set, list)):
        return list(obj)
    return [obj]


def map_properties(cls, props, required=None, delete=None):
    '''
    Remap property names

    Parameters
    ----------
    cls : Connector-subclass
        The Connector class which contains the property definitions
    props : dict
        The dictionary of properties
    required : string or list-of-strings, optional
        The list of required keys.  These will be returned in a tuple
        in addition to the remaining dictionary of properties.
    delete : string or list-of-strings, optional
        List of keys to remove

    Returns
    -------
    ((required-params), {properties})
        If ``required`` contains names
    {properties}
        If ``required` is empty

    '''
    delete = listify(delete) + ['self']
    required = listify(required)

    names = {}
    names.update({v.name: k for k, v in six.iteritems(cls.property_defs)})
    names.update({k: k for k, v in six.iteritems(cls.property_defs)})

    out = {}
    for key, value in six.iteritems(props):
        if delete and key in delete:
            continue
        try:
            out[names[key]] = value
        except KeyError:
            raise KeyError('%s is not a valid parameter for %s' % (key, cls))

    if required:
        req_out = []
        for item in required:
            req_out.append(out.pop(item, None))
        return tuple(req_out), out

    return out


def get_subclasses(cls):
    for subclass in cls.__subclasses__():
        for subcls in get_subclasses(subclass):
            yield subcls
        yield subclass


def get_connector_class(elem_or_class, type=None, properties=None):
    '''
    Get a connector class that matches the current element

    Parameters
    ----------
    elem_or_class : string or Element
        The name of the connector class, or an XML definition
    type : string, optional
        The type of the connector.  Ignored if ``elem_or_class`` is an Element.
    properties : dict, optional
        The properties for the connector.  Ignored if ``elem_or_class` is
        an Element.

    Returns
    -------
    :class:`Connector` or subclass of :class:`Connector`

    '''
    if isinstance(elem_or_class, six.string_types):
        if elem_or_class.startswith('<'):
            elem_or_class = xml.ensure_element(elem_or_class)

    if isinstance(elem_or_class, six.string_types):
        cls = elem_or_class

        if type is None:
            type = 'subscribe'

        if properties is None:
            properties = {}

    else:
        elem = elem_or_class

        cls = elem.attrib['class']

        if 'type' in elem.attrib:
            type = elem.attrib['type']
        else:
            type = elem.find('./properties/property[@name="type"]').text

        properties = {}
        for item in elem.findall('./properties/property'):
            properties[item.attrib['name']] = item.text

    if type.startswith('p'):
        type = 'publish'
    else:
        type = 'subscribe'

    out = []
    for item in get_subclasses(Connector):
        if item.connector_key['cls'] == cls and item.connector_key['type'] == type:
            out.append(item)

    if not out:
        return Connector

    if len(out) == 1:
        return out[0]

    # Check extra matching properties
    matches = True
    for item in reversed(sorted(out, key=lambda x: len(x.connector_key))):
        matches = True
        for key, value in six.iteritems(item.connector_key):
            if key == 'cls' or key == 'type':
                continue
            elif isinstance(value, RegexType):
                eprop = properties.get(key)
                if eprop is None or not value.match(eprop):
                    matches = False
                    break
            else:
                eprop = properties.get(key)
                if eprop is None or value != eprop:
                    matches = False
                    break
        if matches:
            break

    if matches:
        return item

    return Connector


def prop(name, dtype, required=False, valid_values=None, valid_expr=None, default=None):
    return ConnectorProperty(name, dtype, required=required,
                             valid_values=valid_values,
                             valid_expr=valid_expr,
                             default=None)


class ConnectorProperty(object):

    def __init__(self, name, dtype, required=False, valid_values=None,
                 valid_expr=None, default=None):
        self.name = name
        self.dtype = dtype
        self.required = required

        if valid_values is None:
            self._valid_values = None
        elif isinstance(valid_values, (six.string_types, RegexType)):
            self._valid_values = [valid_values]
        else:
            self._valid_values = list(valid_values)

        if valid_expr is None:
            self._valid_expr = None
        elif isinstance(valid_expr, (six.string_types, RegexType)):
            self._valid_expr = [valid_expr]
        else:
            self._valid_expr = list(valid_expr)

        if default is None:
            self.default = default
        else:
            self.default = self.validate_value(default)

    def validate_type(self, value):
        '''
        Verify that the given value is the correct type

        Parameters
        ----------
        value : any
            The property value

        Raises
        ------
        TypeError
            If the value is not the declared type

        '''
        unmatched_types = []

        dtypes = self.dtype
        if not isinstance(self.dtype, (list, tuple)):
            dtypes = [self.dtype]

        for dtype in dtypes:
            if dtype in ['int', int]:
                if not isinstance(value, numbers.Integral):
                    unmatched_types.append(dtype)
            elif dtype in ['float', 'double', float]:
                if not isinstance(value, (numbers.Integral, numbers.Real)):
                    unmatched_types.append(dtype)
            elif dtype in ['boolean', 'bool', bool]:
                if value is not True and value is not False:
                    unmatched_types.append(dtype)
            elif dtype in ['string', str]:
                if not isinstance(value, six.string_types):
                    unmatched_types.append(dtype)
            else:
                raise TypeError('Unknown data type: %s' % dtype)

        if len(unmatched_types) == len(dtype):
            raise TypeError('%s is not one of: %s' % ', '.join(unmatched_types))

    def validate_value(self, value):
        '''
        Verify that the value is valid

        Parameters
        ----------
        value : any
            The property value to test

        Returns
        -------
        bool

        '''
        if value is None:
            return not(self.required)

        # If it's an environment variable, always return true
        if isinstance(value, six.string_types):
            print(bool(re.search(r'@\w+@', value)))
        if isinstance(value, six.string_types) and bool(re.search('r@\w+@', value)):
            return True

        # Make sure value is the correct type
        self.validate_type(value)

        # Check specific values and regular expressions
        valid = True
        if self._valid_values:
            regexes = [x for x in self._valid_values if isinstance(x, RegexType)]
            values = [x for x in self._valid_values if not isinstance(x, RegexType)]
            if valid and values and value not in values:
                valid = False
            if valid and regexes:
                for item in regexes:
                    if not item.search(value):
                        valid = False
                        break

        # Check expressions and regular expressions
        if valid and self._valid_expr:
            for item in self._valid_expr:
                if isinstance(item, RegexType) and not re.search(item, value):
                    valid = False
                elif isinstance(item, six.string_types):
                    if not eval(item):
                        valid = False
                        break

        return valid


class Connector(collections.abc.MutableMapping):
    '''
    Window connector

    Parameters
    ----------
    conncls : string
        The connecter class name
    type : string, optional
        The type of the connector
    name : string, optional
        The name of the connector
    properties : dict, optional
        Dictionary of connector properties

    '''
    connector_key = dict(cls='', type='')
    property_defs = dict()

    def __init__(self, conncls, type=None, name=None, is_active=None, properties=None):
        self.cls = conncls
        self.name = name or gen_name(prefix='c_')
        self.type = type
        self.is_active = is_active
        self.properties = {}
        self.set_properties(**properties)

    def copy(self, deep=False):
        '''
        Return a copy of the object

        Parameters
        ----------
        deep : bool, optional
            Should sub-objects be copied as well?

        Returns
        -------
        :class:`Connector`

        '''
        return type(self).from_parameters(conncls=self.cls, type=self.type,
                                          name=self.name, is_active=self.is_active,
                                          properties=self.properties)

    def __copy__(self):
        return self.copy(deep=False)

    def __deepcopy__(self, memo):
        return self.copy(deep=True)

    @classmethod
    def from_parameters(cls, conncls, type=None, name=None, is_active=None,
                        properties=None):
        return cls(conncls, type=type, name=name, is_active=None,
                   properties=properties)

    @classmethod
    def from_element(cls, data, session=None):
        '''
        Construct connector from XML definition

        Parameters
        ----------
        data : xml-string or ElementTree.Element
            The XML definition
        session : requests.Session, optional
            The session object associated with the server

        Returns
        -------
        :class:`Connector`

        '''
        data = xml.ensure_element(data)

        conncls = data.attrib['class']
        name = data.attrib.get('name')
        type = data.attrib.get('type')
        is_active = data.attrib.get('active')

        properties = {}
        for item in data.findall('./properties/property'):
            properties[item.attrib['name']] = item.text

        return get_connector_class(data).from_parameters(conncls, type=type,
                                                         name=name,
                                                         is_active=is_active,
                                                         properties=properties)

    from_xml = from_element

    def to_element(self):
        '''
        Export connector definition to ElementTree.Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('connector', attrib={'class': self.cls,
                                                'name': self.name,
                                                'active': self.is_active,
                                                'type': self.type})

        sorted_items = sorted([(k, v)for k, v in six.iteritems(self.properties)])
        properties = collections.OrderedDict(sorted_items)

        # Add defaults
        for key, value in six.iteritems(type(self).property_defs):
            if value.default is not None and properties.get(key, None) is None:
                xml.add_properties(out, {key: value.default})

        # Always make sure configfilesection= is next
        if properties.get('configfilesection', None) is not None:
            xml.add_properties(out, dict(configfilesection=properties['configfilesection']))
            properties.pop('configfilesection')

        # Add the rest
        xml.add_properties(out, properties)

        return out

    def to_xml(self, pretty=False):
        '''
        Export connector definition to XML

        Parameters
        ----------
        pretty : bool, optional
            Should the output include whitespace for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)

    def set_properties(self, **kwargs):
        '''
        Set connector properties

        Parameters
        ----------
        **kwargs : keyword-parameters, optional
            Key / value pairs of properties

        '''
        for key, value in six.iteritems(kwargs):
            if key == 'type':
                if value.startswith('p'):
                    self.type = 'publish'
                else:
                    self.type = 'subscribe'
                continue
            if key == 'name':
                self.name = value
                continue
            self.properties[key] = value
        self.properties = {k: v for k, v in six.iteritems(self.properties)
                           if v is not None}

    def __getitem__(self, key):
        return self.properties[key]

    def __setitem__(self, key, value):
        self.properties[key] = value

    def __delitem__(self, key):
        del self.properties[key]

    def __iter__(self):
        return iter(self.properties)

    def __len__(self):
        return len(self.properties)

    def __str__(self):
        if self.type and self.name:
            return '%s(%s, name=%s, type=%s, properties=%s)' % \
                   (type(self).__name__, repr(self.cls), repr(self.name),
                    repr(self.type), repr(self.properties))
        if self.type:
            return '%s(%s, type=%s, properties=%s)' % \
                   (type(self).__name__, repr(self.cls),
                    repr(self.type), repr(self.properties))
        return '%s(%s, name=%s, properties=%s)' % \
               (type(self).__name__, repr(self.cls),
                repr(self.name), repr(self.properties))

    def __repr__(self):
        return str(self)
