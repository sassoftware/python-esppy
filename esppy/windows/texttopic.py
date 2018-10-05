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

from .base import Window, attribute
from .utils import get_args


class TextTopicWindow(Window):
    '''
    Text topic window

    Parameters
    ----------
    name : string, optional
        The name of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level
        value of pubsub is manual, true enables publishing and subscribing
        for the window and false disables it.
    description : string, optional
        Description of the window
    output_insert_only : bool, optional
        When true, prevents the window from passing non-insert events to
        other windows.
    collapse_updates : bool, optional
        When true, multiple update blocks are collapsed into a single update block
    pulse_interval : string, optional
        Output a canonical batch of updates at the specified interval
    exp_max_string : int, optional
        Specifies the maximum size of strings that the expression engine uses
        for the window. Default value is 1024.
    index_type : string, optional
        Index type for the window
    pubsub_index_type : string, optional
        Publish/subscribe index type.  Valid values are the same as for the
        `index_type` parameter.
    astore_file : string, optional
        Path to analytic store (ASTORE) file
    ta_path : string, optional
        Path to text analytics directory
    text_field : string, optional
        Name for the string field in the input event to analyze
    include_topic_name : bool, optional
        When true, includes the topic name in the event

    Returns
    -------
    :class:`TextTopicWindow`

    '''

    window_type = 'texttopic'

    astore_file = attribute('astore-file', dtype='string')
    ta_path = attribute('ta-path', dtype='string')
    text_field = attribute('text-field', dtype='string')
    include_topic_name = attribute('include-topic-name', dtype='bool')

    def __init__(self, name=None, pubsub=None, description=None,
                 output_insert_only=None, collapse_updates=None,
                 pulse_interval=None, exp_max_string=None,
                 index_type=None, pubsub_index_type=None,
                 astore_file=None, ta_path=None, text_field=None,
                 include_topic_name=None):
        Window.__init__(self, **get_args(locals()))
