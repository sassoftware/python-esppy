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

''' ESP Schema '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import json
import os
import re
import requests
import six
import xml.etree.ElementTree as ET
from collections import OrderedDict
from six.moves import urllib
from .base import ESPObject, attribute
from .utils import xml


def clean_dtype(value):
    '''
    Make sure dtype is normalized

    '''
    return re.sub(r'\s+', r'', value)\
             .replace('array(double)', 'array(dbl)')\
             .replace('array(int32)', 'array(i32)')\
             .replace('array(int64)', 'array(i64)')


class FieldDict(collections.abc.MutableMapping):
    ''' Dictionary for holding schema fields '''

    def __init__(self):
        self._data = OrderedDict()

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, name, value):
        key = False
        if '*' in name:
            name = name.replace('*', '')
            key = True

        # obj[name] = SchemaField(...)
        if isinstance(value, SchemaField):
            pass

        # obj[name] = 'type', True|False
        elif isinstance(value, (tuple, list)):
            if len(value) == 1:
                value = SchemaField(name, type=value[0], key=key)
            elif len(value) == 2:
                dtype, key = value
                if isinstance(key, six.string_types):
                    key = (key == 'true') and True or False
                value = SchemaField(name, type=clean_dtype(dtype), key=bool(key))
            else:
                raise ValueError('Too many values in schema field: %s' % value)

        # obj[name] = 'type'
        # obj['*name'] = 'type'
        # obj['name*'] = 'type'
        # obj[name] = '*type'
        # obj[name] = 'type*'
        elif isinstance(value, six.string_types):
            if '*' in value:
                value = SchemaField(name, type=value.replace('*'), key=True)
            else:
                value = SchemaField(name, type=value, key=key)

        # obj[name] = dict(type='type', key=True|False)
        elif isinstance(value, dict):
            value = SchemaField(name, value['type'], value.get('key', key))

        # obj[name] = ?
        else:
            raise TypeError('Unrecognized type for schema field: %s' % value)

        self._data[name] = value

    @property
    def schema_string(self):
        '''
        Construct schema string

        Returns
        -------
        string

        '''
        out = []
        for name, value in self.items():
            out.append('%s%s:%s' % (name, value.key and '*' or '', value.type))
        return ','.join(out)

    @schema_string.setter
    def schema_string(self, value):
        self.clear()
        for field in [x.strip() for x in re.split(r'\s*,\s*|\s+', value.strip())]:
            parts = list(field.split(':', 1))
            if len(parts) == 1:
                name = parts[0]
                dtype = 'inherit'
            else:
                name = parts[0]
                dtype = clean_dtype(parts[1])
            key = False
            if '*' in name:
                key = True
                name = name.replace('*', '')
            self[name] = SchemaField(name.strip(), dtype.strip(), key=key)

    def __delitem__(self, key):
        del self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return repr(self._data)


class SchemaField(ESPObject):
    '''
    Schema field

    Attributes
    ----------
    name : string
        Name of the field
    type : string
        Data type of the field
    key : bool
        Indicates whether or not the field is a key field

    Parameters
    ----------
    name : string
        Name of the field
    type : string
        Data type of the field
    key : bool
        Indicates whether or not the field is a key field

    '''
    type = attribute('type', dtype='string')
    key = attribute('key', dtype='bool')

    def __init__(self, name, type, key=False):
        ESPObject.__init__(self, attrs=dict(type=type, key=key))
        self.name = name

    def copy(self, deep=False):
        '''
        Return a copy of the object

        Parameters
        ----------
        deep : bool, optional
            Should sub-objects be copied as well?

        Returns
        -------
        :class:`SchemaField`

        '''
        return type(self)(name=self.name, type=self.type, key=self.key)

    def __copy__(self):
        return self.copy(deep=False)

    def __deepcopy__(self, memo=None):
        return self.copy(deep=True)

    @classmethod
    def from_xml(cls, data, session=None):
        '''
        Create schema field from XML definition

        Parameters
        ----------
        data : xml-string or ElementTree.Element
            The schema field XML definition
        session : requests.Session
            The ESP session object

        Returns
        -------
        :class:`SchemaField`

        '''
        out = cls('', type='double', key=False)
        out.session = session

        if isinstance(data, six.string_types):
            data = ET.fromstring(data)

        out._set_attributes(data.attrib)

        return out

    from_element = from_xml

    def to_element(self):
        '''
        Export schema field definition to ElementTree.Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('field', xml.get_attrs(self))

    def to_xml(self, pretty=False):
        '''
        Export schema field definition to XML

        Parameters
        ----------
        pretty : bool, optional
            Should the output include whitespace for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)

    def __str__(self):
        return '%s%s:%s' % (self.name, self.key and '*' or '', self.type)

    def __repr__(self):
        return str(self)


class Schema(ESPObject, collections.abc.MutableMapping):
    '''
    Schema definition

    Attributes
    ----------
    fields : FieldDict
        Dictionary of field in the schema

    Parameters
    ----------
    *args : SchemaFields
        One or more :class:`SchemaField` objects
    name : string, optional
        Name for the schema object
    copy_window : string, optional
        Name of window to copy the scheme of
    copy_keys : boolean, optional
        If true, uses the keys from the schema that is copied with
        the copy=window_name attribute.
        Default: false

    '''

    name = attribute('name', dtype='string')
    copy_window = attribute('copy', dtype='string')
    copy_keys = attribute('copy-keys', dtype='bool')

    def __init__(self, *args, **kwargs):
        ESPObject.__init__(self)
        self.name = kwargs.get('name')
        self.copy_window = kwargs.get('copy_window')
        self.copy_keys = kwargs.get('copy_keys')
        self.fields = FieldDict()
        for item in args:
            self.fields[item.name] = item

    def copy(self, deep=False):
        '''
        Return a copy of the object

        Parameters
        ----------
        deep : bool, optional
            Should sub-objects be copied as well?

        Returns
        -------
        :class:`Schema`

        '''
        out = type(self)()
        for name, field in self.fields.items():
            if deep:
                out.fields[name] = field.copy(deep=True)
            else:
                out.fields[name] = field
        return out

    def __copy__(self):
        return self.copy(deep=False)

    def __deepcopy__(self, memo):
        return self.copy(deep=True)

    @property
    def schema_string(self):
        '''
        Construct schema string

        Returns
        -------
        string

        '''
        return self.fields.schema_string

    @schema_string.setter
    def schema_string(self, value):
        self.fields.schema_string = value

    @classmethod
    def from_json(cls, data, session=None):
        '''
        Create schema from JSON definition

        Parameters
        ----------
        data : json-string
            The schema definition in JSON
        session : requests.Session
            The ESP session object

        Returns
        -------
        :class:`Schema`

        '''
        out = cls()
        out.session = session

        fields = json.loads(data)['schema'][0]['fields']
        for field in fields:
            field = field['field']['attributes']
            key = field.get('key', 'false') == 'true'
            name = field['name']
            dtype = clean_dtype(field['type'])
            out.fields[name] = SchemaField(name, dtype, key=key)

        return out

    @classmethod
    def from_string(cls, data, session=None):
        '''
        Create schema from string definition

        Parameters
        ----------
        data : string
            The string schema definition
        session : requests.Session
            The ESP session object

        Returns
        -------
        :class:`Schema`

        '''
        out = cls()
        out.session = session

        for item in re.split(r'\s*,\s*', data.strip()):
            name, dtype = item.split(':')
            is_key = '*' in name
            name = name.replace('*', '')
            field = SchemaField(name, dtype, key=is_key)
            out.fields[name] = field

        return out

    @classmethod
    def from_schema_string(cls, data, session=None):
        '''
        Create schema from schema-string element

        Parameters
        ----------
        data : string
            The string-schema element
        session : requests.Session
            The ESP session object

        Returns
        -------
        :class:`Schema`

        '''
        if isinstance(data, six.string_types):
            if not data.strip().startswith('<'):
                data = '<schema-string>%s</schema-string>' % data
            data = xml.from_xml(data)

        if data.text:
            out = cls.from_string(data.text)
        else:
            out = cls()

        out.session = session

        out._set_attributes(data.attrib)

        return out

    @classmethod
    def from_xml(cls, data, session=None):
        '''
        Create schema from XML definition

        Parameters
        ----------
        data : xml-string or ElementTree.Element
            The schema XML definition
        session : requests.Session
            The ESP session object

        Returns
        -------
        :class:`Schema`

        '''
        out = cls()
        out.session = session

        if isinstance(data, six.string_types):
            data = xml.from_xml(data)

        out._set_attributes(data.attrib)

        for item in data.findall('.//field'):
            field = SchemaField.from_xml(item)
            out.fields[field.name] = field

        return out

    from_element = from_xml

    def to_element(self):
        '''
        Export schema definition to ElementTree.Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('schema', xml.get_attrs(self))

        if self.fields:
            fields = xml.add_elem(out, 'fields')
            for item in self.fields.values():
                xml.add_elem(fields, item.to_element())

        return out

    def to_xml(self, pretty=False):
        '''
        Export schema definition to XML

        Parameters
        ----------
        pretty : bool, optional
            Should the output include whitespace for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)

    def __str__(self):
        return self.schema_string

    def __repr__(self):
        return str(self)

    def add_field(self, name, type, key=False):
        '''
        Add a schema field

        Parameters
        ----------
        name : string
            Name of the field
        type : string
            Data type of the field
        key : bool, optional
            Indicates whether or not the field is a key field

        '''
        self.fields[name] = SchemaField(name, type, key=key)

#
# MutableMapping methods
#

    def __getitem__(self, key):
        return self.fields[key]

    def __setitem__(self, key, value):
        self.fields[key] = value

    def __delitem__(self, key):
        del self.fields[key]

    def __iter__(self):
        return iter(self.fields)

    def __len__(self):
        return len(self.fields)
