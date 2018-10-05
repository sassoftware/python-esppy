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

from .base import BaseWindow, attribute
from .features import ParametersFeature, ConnectorsFeature
from .utils import get_args


class ModelReaderWindow(BaseWindow, ParametersFeature, ConnectorsFeature):
    '''
    Model reader window

    Parameters
    ----------
    name : string, optional
        The name of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for
        the window and false disables it.
    description : string, optional
        Description of the window
    model_type : string, optional
        Type of the model.
        Valid Values: astore or recommender

    Returns
    -------
    :class:`ModelReaderWindow`

    '''

    window_type = 'model-reader'

    model_type = attribute('model-type', dtype='string',
                           values=['astore', 'recommender'])

    def __init__(self, name=None, pubsub=None, description=None, model_type=None):
        BaseWindow.__init__(self, **get_args(locals()))
