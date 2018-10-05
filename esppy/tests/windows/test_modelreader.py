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

# NOTE: This test requires a running ESP server.  You must use an ~/.authinfo
#       file to specify your username and password.  The ESP host and port must
#       be specified using the ESPHOST and ESPPORT environment variables.
#       A specific protocol ('http' or 'https') can be set using
#       the ESPPROTOCOL environment variable.

import os
import unittest
import esppy
from . import utils as tm


class TestModelReaderWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('modelreader_window.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto',
                                  n_threads='1', use_tagged_token=True)

        cq = esp.ContinuousQuery(name='cq_01')

        w_source = esp.SourceWindow(name='w_source',
                                    schema=('Species:string', 'SepalLength:double',
                                            'SepalWidth:double', 'PetalLength:double',
                                            'PetalWidth:double', 'id*:int64'),
                                    insert_only=True, index_type='empty')

        w_request = esp.SourceWindow(name='w_request',
                                     schema=('req_id*:int64','req_key:string','req_val:string'),
                                     insert_only=True, index_type='empty')

        w_reader = esp.ModelReaderWindow(name='w_reader', pubsub=True)

        w_score = esp.ScoreWindow(name='w_score',
                                  schema=('id*:int64', 'SepalLength:double',
                                          'SepalWidth:double', 'PetalLength:double',
                                          'PetalWidth:double', 'Species:string',
                                          'P_SpeciesVersicolor:double',
                                          'P_SpeciesVirginica:double',
                                          'P_SpeciesSetosa:double',
                                          'I_Species:string', '_WARN_:string'))
        w_score.add_offline_model(model_type='astore',
                                  output_map=dict(test='testabc'))
        w_score.add_connector('fs',
                              conn_name='sub', conn_type='subscribe',
                              type='sub',
                              fstype='csv',
                              fsname='result.out',
                              snapshot=True)

        proj.add_query(cq)
        cq.add_windows(w_source, w_request, w_score, w_reader)

        w_source.add_target(w_score,role='data')
        w_reader.add_target(w_score,role='model')
        w_request.add_target(w_reader,role='request')

        self._test_model_file('modelreader_window.xml', proj)
