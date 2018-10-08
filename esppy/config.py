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

'''
Initialization of ESP options

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import functools
import pandas as pd
from .utils.config import (register_option, check_boolean, check_int, get_option,
                           set_option, reset_option, describe_option, check_url,
                           ESPOptionError, check_string, options, get_suboptions,
                           get_default, check_float, option_context)


# Root of server URLs
ESP_ROOT = 'SASESP'

# Store pandas version
PANDAS_VERSION = tuple([int(x) for x in pd.__version__.split('.')])

CONCAT_OPTIONS = {}
if PANDAS_VERSION >= (0, 23, 0):
    CONCAT_OPTIONS['sort'] = True

#
# Connection options
#

register_option('hostname', 'string', check_string,
                'localhost',
                'Specifies the hostname for the ESP server.',
                environ='ESPHOST')

register_option('port', 'int', check_int, 0,
                'Sets the port number for the ESP server.',
                environ='ESPPORT')

register_option('protocol', 'string', check_string, 'http',
                'Sets the protocol of the ESP server.',
                environ='ESPPROTOCOL')

#
# Project options
#

register_option('display.show_schema', 'boolean', check_boolean, False,
                'Should schemas be displayed when rendering project diagrams?')

register_option('display.show_field_type', 'boolean', check_boolean, True,
                'Should field types be displayed when rendering project diagrams?')

register_option('display.image_scale', 'float',
                functools.partial(check_float, minimum=0.1, maximum=10), 1.0,
                'Specifies the scale factor for rendering project diagrams.')

register_option('display.max_fields', 'int',
                functools.partial(check_int, minimum=0), 20,
                'Specifies the maximum number of schema fields per window to display\n'
                'when rendering a project diagram.')

#
# Debug options
#

register_option('debug.requests', 'boolean', check_boolean, False,
                'Display requested URL when accessing REST interface.')
register_option('debug.request_bodies', 'boolean', check_boolean, False,
                'Display body of request when accessing REST interface.')
register_option('debug.responses', 'boolean', check_boolean, False,
                'Display raw responses from server.')
register_option('debug.events', 'boolean', check_boolean, False,
                'Display raw events from server.')
