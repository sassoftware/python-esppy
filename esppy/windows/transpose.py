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

from __future__ import print_function, division, absolute_import, unicode_literals

from xml.etree import ElementTree as xml
from .base import Window, BaseWindow, attribute
from .utils import get_args

class TransposeWindow(Window):
    '''
    Transpose Window

    Parameters
    ----------
    name : string, optional
        The name of the window
    description : string, optional
        Description of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for
        the window and false disables it.
    mode : string
        This is either 'wide' or 'long'.  If 'wide', values --> rows If 'long', rows --> values
    tag_name : string
        If mode is 'wide', this is the name of input field holding the tag.
        If mode is 'long', this is the name of output field holding the tag.
    tag_values : string
        Name of input field(s) holding value.
    tags_included : string
        If mode is 'wide', the name(s) of input field(s) holding value.
        If mode is 'long', this is the value(s) of tag-name.
    group_by : string
        If mode is 'wide', this is a list of input field(s) to group on.
    clear_timeout : string
        This value should be 'never' or time, e.g. 10 seconds

    Returns
    -------
    :class:`TransposeWindow`

    '''

    window_type = 'transpose'

    mode = attribute('mode',dtype='string')
    tag_name = attribute('tag-name',dtype='string')
    tag_values = attribute('tag-values',dtype='string')
    tags_included = attribute('tags-included',dtype='string')
    group_by = attribute('group-by',dtype='string')
    clear_timeout = attribute('clear-timeout',dtype='string')

    def __init__(self,mode=None,tag_name=None,tag_values=None,tags_included=None,group_by=None,clear_timeout=None,
                 name=None, pubsub=None, description=None,
                 output_insert_only=None, collapse_updates=None,
                 pulse_interval=None, exp_max_string=None,
                 index_type=None, pubsub_index_type=None):
        Window.__init__(self, **get_args(locals()))
