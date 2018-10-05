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

''' XML Utilities '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import re
import six
import warnings
import xml.etree.ElementTree as ET
from .keyword import keywordify
from ..base import ESPObject


def _cast_attrs(attrs):
    out = {}
    for name, value in (attrs or {}).items():
        if value is None:
            continue
        elif value is True:
            value = 'true'
        elif value is False:
            value = 'false'
        else:
            value = '%s' % value
        out[name.replace('_', '-')] = value
    return out


def new_elem(elem_name, attrib=None, text_content=None, **kwargs):
    '''
    Create a new element

    Parameters
    ----------
    elem_name : string
       The tag name of the element
    attrib : dict, optional
        The attributes to set
    text_content : string, optional
        The text content of the new element
    **kwargs : keyword arguments, optional
        Additional attributes as keyword arguments

    Returns
    -------
    :class:`ElementTree.Element`

    '''
    attrib = _cast_attrs(attrib)
    kwargs = _cast_attrs(kwargs)
    out = ET.Element(elem_name, attrib=attrib, **kwargs)
    if text_content is not None:
        out.text = '%s' % text_content
    return out


def add_elem(parent_elem, child_elem, attrib=None,
             text_content=None, **kwargs):
    '''
    Add a new element to the specified parent element

    Parameters
    ----------
    parent_elem : ElementTree.Element
        The parent element
    child_elem : string
        The name of an element or an XML fragment
    attrib : dict, optional
        The attributes to set
    text_content : string, optional
        The text content of the new element
    **kwargs : keyword arguments, optional
        Additional attributes as keyword arguments

    Returns
    -------
    :class:`ElementTree.Element`

    '''
    attrib = _cast_attrs(attrib)
    kwargs = _cast_attrs(kwargs)

    # child_elem is an Element
    if isinstance(child_elem, ET.Element):
        out = child_elem
        if attrib:
            for key, value in attrib.items():
                out.set(key, value)
        for key, value in kwargs.items():
            out.set(key, value)
        parent_elem.append(out)

    # child_elem is an XML fragment
    elif re.match(r'^\s*<', child_elem):
        out = ET.fromstring(child_elem)
        if attrib:
            for key, value in attrib.items():
                out.set(key, value)
        for key, value in kwargs.items():
            out.set(key, value)
        parent_elem.append(out)

    # child_elem is an element name
    else:
        out = ET.SubElement(parent_elem, child_elem, attrib=attrib, **kwargs)

    if text_content is not None:
        out.text = '%s' % text_content

    return out


def add_properties(elem, *args, **kwargs):
    '''
    Add a ``properties`` node to the given element

    Parameters
    ----------
    elem : ElementTree.Element
        The element to add properties to
    verbatim : boolean, optional
        Should property names be used verbatim?
    *args : two-element-tuples, optional
        Passed to dict constructor as properties
    **kwargs : keyword arguments, optional
        Passed to dict constructor as properties

    Returns
    -------
    :class:`ElementTree.Element`

    '''
    verbatim = kwargs.pop('verbatim', False)
    bool_as_int = kwargs.pop('bool_as_int', False)
    props = add_elem(elem, 'properties')
    for key, value in sorted(dict(*args, **kwargs).items()):
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            value = ','.join(value)
        elif value is True:
            if bool_as_int:
                value = '1'
            else:
                value = 'true'
        elif value is False:
            if bool_as_int:
                value = '0'
            else:
                value = 'false'
        add_elem(props, 'property',
                 dict(name=keywordify(key)), text_content=value)
    return props


def xml_indent(elem, level=0):
    '''
    Add whitespace to XML for pretty-printing

    Parameters
    ----------
    elem : ElementTree.Element
        The element to modify with whitespace
    level : int, optional
        The level of indent

    Returns
    -------
    ``None``

    '''
    i = '\n' + (level * '  ')
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + '  '
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            xml_indent(elem, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def from_xml(data):
    '''
    Convert XML to ElementTree.Element

    Parameters
    ----------
    data : string
        The XML to parse

    Returns
    -------
    :class:`ElementTree.Element`

    '''
    try:
        return ET.fromstring(data)
    except:
        for i, line in enumerate(data.split('\n')):
            print(i+1, line)
        raise


def to_xml(elem, encoding=None, pretty=False):
    '''
    Export element to XML

    Parameters
    ----------
    elem : ElementTree.Element or xml-string
        The element to export
    encoding : string, optional
        The output encoding

    Returns
    -------
    string

    '''
    if isinstance(elem, six.string_types):
        elem = ET.fromstring(elem)

    # In-place editing!!
    if pretty:
        xml_indent(elem)

    if encoding is None:
        return ET.tostring(elem, encoding='utf-8').decode()

    return ET.tostring(elem, encoding=encoding)


def get_attrs(obj, extra=[], exclude=[]):
    '''
    Retrieve XML attributes from object

    If ``obj`` has an ``xml_map`` dictionary attribute, it indicates
    the object attr to xml attr mapping.

        class MyObject(object):
            xml_map = dict(object_attr='xml_attr',
                           same_name_object_attr='same_name_object_attr')

    Parameters
    ----------
    obj : object
        The object to get attributes from

    Returns
    -------
    dict

    '''
    if isinstance(exclude, six.string_types):
        exclude = [exclude]

    out = obj._get_attributes()

    if isinstance(extra, six.string_types):
        extra = [extra]

    if extra:
        for item in extra:
            out[item] = getattr(obj, item)

    if exclude:
        for item in exclude:
            out.pop(item, None)

    return {k: '%s' % v for k, v in out.items() if v is not None}


def ensure_element(data):
    '''
    Ensure the given object is an ElementTree.Element

    Parameters
    ----------
    data : string or Element

    Returns
    -------
    :class:`ElementTree.Element`

    '''
    if isinstance(data, six.string_types):
        return from_xml(data)
    return data
