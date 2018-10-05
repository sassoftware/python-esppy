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

import copy
from .base import Window, attribute
from .features import (SchemaFeature, CXXPluginContextFeature, CXXPluginFeature,
                       DS2TableServerFeature, DSExternalFeature, MASMapFeature)
from .utils import get_args, ensure_element, connectors_to_end
from ..utils import xml


class ProceduralWindow(Window, SchemaFeature, CXXPluginContextFeature,
                       CXXPluginFeature, DSExternalFeature):
    '''
    Procedural window

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
        When true, prevents the window from passing non-insert events
        to other windows.
    collapse_updates : bool, optional
        When true, multiple update blocks are collapsed into a single update block
    pulse_interval : string, optional
        Output a canonical batch of updates at the specified interval.
    exp_max_string : int, optional
        Specifies the maximum size of strings that the expression engine
        uses for the window. Default value is 1024.
    index_type : string, optional
        Index type for the window
    pubsub_index_type : string, optional
        Publish/subscribe index type.  Valid values are the same as for the
        `index_type` parameter.
    produces_only_inserts : bool, optional
        Set to true when you know that the Procedural window always
        produces inserts.

    Attributes
    ----------
    cxx_plugin_context : CXXPluginContext
        Specifies a shared library and function name.
    cxx_plugin : CXXPlugin
        A shared library and function name pair that specifies Procedural
        window handlers.
    ds_external : DSExternal
        Contains a block of DATA step code that is used as an input
        handler for Procedural windows.

    Returns
    -------
    :class:`ProceduralWindow`

    '''

    window_type = 'procedural'

    produces_only_inserts = attribute('produces-only-inserts', dtype='bool')

    def __init__(self, name=None, schema=None, pubsub=None,
                 description=None, output_insert_only=None,
                 collapse_updates=None, pulse_interval=None,
                 exp_max_string=None, index_type=None,
                 pubsub_index_type=None, produces_only_inserts=None):
        Window.__init__(self, **get_args(locals()))
