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
from .features import RetentionFeature, SchemaFeature
from .utils import get_args


class SourceWindow(Window, SchemaFeature, RetentionFeature):
    '''
    Source Window

    Parameters
    ----------
    name : string, optional
        The name of the window
    schema : Schema, optional
        The schema of the window
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
    insert_only : bool, optional
        When true, indicates the window will only receive insert events
    autogen_key : bool, optional
        Auto-generate the key. The Source window must be insert-only and
        have a single INT64 or STRING key.

    Attributes
    ----------
    retention : Retention
        Specifies the retention policy and value of the retention object

    Returns
    -------
    :class:`SourceWindow`

    '''
    window_type = 'source'

    insert_only = attribute('insert-only', dtype='bool')
    autogen_key = attribute('autogen-key', dtype='bool')

    def __init__(self, name=None, schema=None, index_type=None, pubsub=None,
                 pubsub_index_type=None, insert_only=None, output_insert_only=None,
                 collapse_updates=None, autogen_key=None, pulse_interval=None,
                 exp_max_string=None):
        Window.__init__(self, **get_args(locals()))
