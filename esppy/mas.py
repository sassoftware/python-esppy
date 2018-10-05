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

''' ESP MAS Modules '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import six
from six.moves import urllib
from .base import ESPObject
from .utils import xml
from .utils.rest import get_params
from .utils.data import get_project_data, gen_name


class ModuleMember(object):

    def __init__(self, member, sha_key, type, description=None,
                 code_file=None, code=None):
        self.member = member
        self.sha_key = sha_key
        self.type = type
        self.description = description
        self.code_file = code_file
        self.code = code

    @classmethod
    def from_element(cls, data, session=None):
        if isinstance(data, six.string_types):
            data = xml.from_xml(data)

        out = cls(data.attrib['member'], data.attrib['SHAkey'],
                  data.attrib['type'])

        for item in data.findall('./description'):
            out.description = item.text

        for item in data.findall('./code'):
            out.code = item.text
        for item in data.findall('./code-file'):
            out.code_file = item.text

        return out

    from_xml = from_element

    def to_element(self):
        out = xml.new_elem('module-member',
                           attrib=dict(member=self.member,
                                       SHAkey=self.sha_key,
                                       type=self.type))

        if self.description:
            xml.add_elem(out, 'description', text_content=self.description)

        if self.code:
            xml.add_elem(out, 'code', text_content=self.code)
        if self.code_file:
            xml.add_elem(out, 'code-file', text_content=self.code_file)

        return out

    def to_xml(self, pretty=False):
        return xml.to_xml(self.to_element(), pretty=pretty)


class MASModule(ESPObject):
    '''
    MAS Module

    Attributes
    ----------
    module : string
        Name of the MAS module
    project : string
        Name of the project the module belongs to

    Parameters
    ----------
    language : string
        The name of the programming language
    func_names : string or list-of-strings, optional
        The function names exported by the module
    module : string, optional
        Name of the MAS module

    '''

    def __init__(self, language, module, func_names, mas_store=None,
                 mas_store_version=None, description=None, code_file=None,
                 code=None):
        ESPObject.__init__(self)
        self.module = module or gen_name('mas_')
        self.language = language
        if isinstance(func_names, six.string_types):
            func_names = re.split(r'\s*,\s*', func_names.strip())
        if isinstance(func_names, six.string_types):
            self.func_names = re.split(r'\s*,\s', func_names.strip())
        else:
            self.func_names = list(func_names)
        self.mas_store = mas_store
        self.mas_store_version = mas_store_version
        self.description = description
        self.code_file = code_file
        self.code = code
        self.module_members = []
        self.project = None

    @classmethod
    def from_xml(cls, data, session=None):
        '''
        Create MAS module from XML definition

        Parameters
        ----------
        data : xml-string or ElementTree.Element
            XML MAS module definition
        session : requests.Session, optional
            Session that the MAS module is associated with

        Returns
        -------
        :class:`MASModule`

        '''
        if isinstance(data, six.string_types):
            data = xml.from_xml(get_project_data(data))

        out = cls(data.attrib['language'], data.attrib['module'],
                  func_names=data.attrib['func-names'],
                  mas_store=data.attrib.get('mas-store'),
                  mas_store_version=data.attrib.get('mas-store-version'))

        out.session = session

        for item in data.findall('./code-file'):
            out.code_file = item.text
        for item in data.findall('./code'):
            out.code = item.text
        for item in data.findall('./description'):
            out.description = item.text

        for item in data.findall('./module-members/module-member'):
            out.module_members.append(ModuleMember.from_element(item, session=session))

        return out

    from_element = from_xml

    def to_element(self):
        '''
        Convert MAS module to ElementTree.Element definition

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('mas-module',
                           attrib=dict(module=self.module,
                                       language=self.language,
                                       func_names=','.join(self.func_names),
                                       mas_store=self.mas_store,
                                       mas_store_version=self.mas_store_version))

        if self.description:
            xml.add_elem(out, 'description', text_content=self.description)

        if self.code:
            xml.add_elem(out, 'code', text_content=self.code)
        elif self.code_file:
            xml.add_elem(out, 'code-file', text_content=self.code_file)

        if self.module_members:
            members = xml.add_elem(out, 'module-members')
            for item in self.module_members:
                xml.add_elem(members, item.to_element())

        return out

    def to_xml(self, pretty=False):
        '''
        Convert MAS module to XML definition

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)

    def save(self, path=None):
        '''
        Save the module

        Parameters
        ----------
        path : string, optional
            The location to save the module

        '''
        self._put(urllib.parse.urljoin(self.base_url,
                                       'masModules/%s/%s/state' % (self.project, self.module)),
                  params=get_params(value='persisted', path=path))
