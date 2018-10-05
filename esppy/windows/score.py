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

import os
import pandas as pd
import six
from .base import BaseWindow, attribute
from .features import SchemaFeature, ModelsFeature, ConnectorsFeature
from .utils import get_args, ensure_element


class ScoreWindow(BaseWindow, SchemaFeature, ModelsFeature, ConnectorsFeature):
    '''
    Score window

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

    Attributes
    ----------
    online_models : list-of-OnlineModels
        List of online model objects
    offline_models : list-of-OfflineModels
        List of offline model objects

    Returns
    -------
    :class:`ScoreWindow`

    '''

    window_type = 'score'

    def __init__(self, name=None, schema=None, pubsub=None, description=None,
                 copyvars=None):
        BaseWindow.__init__(self, **get_args(locals()))
        # Set the online model for subclasses
        if type(self).__name__ != 'ScoreWindow':
            self.add_online_model(type(self).__name__)

    def _create_schema_list(self, variables):
        '''
        Extract schema information from DataFrame

        Parameters
        ----------
        variables : DataFrame
            The DataFrame containing schema information

        Returns
        -------
        list

        '''
        labels = []
        labels.append('id*:int64')
        for name, dtype in zip(variables['Name'], variables['Type']):
            if dtype == 'Num':
                labels.append(name + ':double')
            elif dtype == 'Char':
                labels.append(name + ':string')
        return labels

    def import_schema_from_astore_output(self, output_variables_input):
        '''
        Import a schema from the astore CAS action output format

        Parameters
        ----------
        output_variables_input : DataFrame or list or string
            The schema definition

        '''
        if isinstance(output_variables_input, six.string_types):
            if os.path.isfile(output_variables_input):
                output_variables_input = pd.read_csv(output_variables_input)
            else:
                output_variables_input = pd.read_csv(six.StringIO(output_variables_input))
        if isinstance(output_variables_input, pd.DataFrame):
            self.schema = self._create_schema_list(output_variables_input)
        elif isinstance(output_variables_input, (tuple, list)):
            self.schema = list(output_variables_input)
