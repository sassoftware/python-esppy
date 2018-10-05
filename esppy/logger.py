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

''' ESP Logger '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import six
from six.moves import urllib
from .base import ESPObject
from .utils.rest import get_params
from .utils.data import get_project_data


class Logger(ESPObject):
    '''
    Server logger

    Attributes
    ----------
    name : string
        Name of the logger
    level : string
        Log level

    Parameters
    ----------
    name : string
        Name of the logger
    level : string
        Log level.  This shouuld be one of the following class variables:
            * Logger.INFO
            * Logger.TRACE
            * Logger.ERROR
            * Logger.WARN
            * Logger.FATAL
            * Logger.DEBUG
            * Logger.OFF

    '''

    INFO = 'info'
    TRACE = 'trace'
    ERROR = 'error'
    WARN = 'warn'
    WARNING = 'warn'
    FATAL = 'fatal'
    DEBUG = 'debug'
    OFF = 'off'

    def __init__(self, name=None, level=None):
        ESPObject.__init__(self)
        self.name = name
        if level is not None:
            level = level.lower()
            if level == 'warning':
                level = 'warn'
        self._level = level

    def __str__(self):
        return "%s(name='%s', level='%s')" % (type(self).__name__, self.name, self.level)

    def __repr__(self):
        return str(self)

    @property
    def level(self):
        return self._level

    @level.setter
    def level(self, value):
        self.set_level(value)

    def set_level(self, level):
        '''
        Set the log level

        Parameters
        ----------
        level : string
            Log level.  This should be one of the following:
                * Logger.INFO
                * Logger.TRACE
                * Logger.ERROR
                * Logger.WARN
                * Logger.FATAL
                * Logger.DEBUG
                * Logger.OFF

        '''
        level = level.lower()
        if level == 'warning':
            level = 'warn'
        levels = ['info', 'trace', 'error', 'warn', 'fatal', 'debug', 'off']
        if level not in levels:
            raise ValueError('%s is not a valid logging level' % level)
        self._put('loggers/%s/level' % self.name, params=get_params(value=level))
        self._level = level.lower()
