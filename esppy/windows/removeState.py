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

class RemoveStateWindow(Window):
    '''
    RemoveStateWindow Window

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
   add_log_fields : bool, optional
        Add originalOC, originalFL to produced events, default=false
   remove : string, optional
        Optional list filtered out attributes

    Returns
    -------
    :class:`RemoveStateWindow`

    '''

    window_type = 'remove-state'

    remove = attribute('remove',dtype='string')
    add_log_fields = attribute('add-log-fields',dtype='string')

    def __init__(self,remove=None,add_log_fields=False,
                 name=None,description=None,pubsub=False):
        Window.__init__(self, **get_args(locals()))
