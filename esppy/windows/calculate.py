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

from .base import BaseWindow, attribute, INDEX_TYPES
from .features import (ParametersFeature, SchemaFeature, InputMapFeature,
                       OutputMapFeature, MASMapFeature, ConnectorsFeature)
from .utils import get_args


class CalculateWindow(BaseWindow, SchemaFeature,
                      ParametersFeature, InputMapFeature, OutputMapFeature,
                      MASMapFeature, ConnectorsFeature):
    '''
    Calculation Window

    Parameters
    ----------
    name : string, optional
        The name of the window
    schema : Schema, optional
        The schema of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for the
        window and false disables it.
    description : string, optional
        Description of the window
    algorithm : string, optional
        The name of the algorithm
    input_map : dict, optional
        Input mappings
    output_map : dict, optional
        Output mappings
    index_type : string, optional
        Index type for the window
    produces_only_inserts : bool, optional
        Set to true when you know that the procedural window always
        produces inserts
    **parameters : keyword arguments, optional
        The parameters to the algorithm

    Returns
    -------
    :class:`CalculateWindow`

    '''

    window_type = 'calculate'

    algorithm = attribute('algorithm', dtype='string')
    index_type = attribute('index', dtype='string', values=INDEX_TYPES)
    produces_only_inserts = attribute('output-insert-only', dtype='bool')

    def __init__(self, name=None, schema=None, pubsub=None,
                 description=None, algorithm=None, index_type=None,
                 produces_only_inserts=None, input_map=None,
                 output_map=None, **parameters):
        BaseWindow.__init__(self, **get_args(locals()))
        self.set_parameters(**parameters)
        self.set_inputs(**(input_map or {}))
        self.set_outputs(**(output_map or {}))
