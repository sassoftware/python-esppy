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
ESP interface

Examples
--------
>>> import esppy
>>> esp = esppy.ESP('http://myesp.com:8777')
>>> esp.server_info
{'analytics-license': True,
 'engine': 'esp',
 'http-admin': 8610,
 'pubsub': 8611,
 'version': '5.2'}

>>> proj = esp.install_project('model.xml')
>>> proj
Project(name='MyProject')

>>> esp.get_running_projects()
{'MyProject': Project(name='MyProject')}

>>> proj.get_windows()
{'w_calculate': CalculationWindow(name='w_calculate',
                                  contquery='contquery',
                                  project='MyProject')),
 'w_data': SourceWindow(name='w_data',
                        contquery='contquery',
                        project='MyProject')),
 'w_request': SourceWindow(name='w_request',
                           contquery='contquery',
                           project='MyProject')}

>>> proj.stop()
>>> esp.get_running_projects()
{}

>>> esp.get_stopped_projects()
{'MyProject': Project(name='MyProject')}

>>> proj.delete()
>>> esp.get_stopped_projects()
{}

>>> esp.shutdown()

'''

from __future__ import print_function, division, absolute_import, unicode_literals

# Configuration
from . import config
from .config import (set_option, get_option, reset_option, describe_option,
                     options, option_context)

from .connection import ESP
from .exceptions import ESPError

__version__ = '7.1.6'
