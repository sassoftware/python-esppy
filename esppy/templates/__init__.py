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

import os
import six
import re
from.template import Template
from ..utils import xml

import logging

logging.basicConfig(filename=os.getenv("ESPPY_LOG"), level=logging.INFO)


def builtin_template_helper(cls, template_name, file_name,  **kwargs):
    '''
    Helper function during initialization
    '''

    out = cls.from_xml(os.path.join(cls.file_dir, 'xmls', file_name), template_name)

    for key, value in six.iteritems(kwargs):
        for each in out.required_parameter_map.get(key, key):
            window, *win_keys = re.split('\.', each)
            actual_key, model = win_keys[0], win_keys[1] if len(win_keys) > 1 else None
            if actual_key == 'input_map':
                out.set_inputs(window, model, **value)
            elif actual_key == 'output_map':
                out.set_outputs(window, model, **value)
            else:
                out.set_parameters(window, **dict({actual_key: value}))

    return out


def template_info_helper(data):
    if isinstance(data, six.string_types):
        if os.path.isfile(data):
            data = open(data, 'r').read()
        data = xml.from_xml(data)

    for desc in data.findall('./description'):
        description = desc.text

    required_dict = {}
    for item in data.findall('./required-parameter-map/properties/property'):
        field = item.text.split(',')
        required_dict[item.attrib['name']] = field

    return description, required_dict


def add_method(cls, file_name):
    ky = file_name.split('.xml')[0]

    # temp = cls.from_xml(os.path.join(cls.file_dir, 'xmls', file_name), None)
    description, required_dict = template_info_helper(os.path.join(cls.file_dir, 'xmls', file_name))
    cls.template_list[ky]['description'] = description
    cls.template_list[ky]['required_parameter_map'] = required_dict

    def builtin_template(template_name, **kwargs):
        '''
        Create a %s template
        ----------

        Parameters
        template_name : string
            the name of template

        Returns
        -------
        :class:`Template`

        '''
        return builtin_template_helper(cls, template_name, file_name, **kwargs)

    builtin_template.__name__ = ky
    builtin_template.__doc__ %= builtin_template.__name__
    setattr(cls, builtin_template.__name__, builtin_template)


for builtin_file in os.listdir(os.path.join(Template.file_dir, "xmls")):
    if builtin_file.endswith(".xml"):
        add_method(Template, builtin_file)


