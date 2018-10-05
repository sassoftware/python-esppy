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


@unittest.skip('Crashes server')
class TestModelSupervisorWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('modelsuper_window.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto',
                                  n_threads='1', use_tagged_token=True)

        cq = esp.ContinuousQuery(name='cq_01', trace='w_scoring w_training')

        w_source = esp.SourceWindow(name='w_source',
                                    schema=('id*:int32','x_c:double','y_c:double'))
        w_source.add_connector('fs',
                                   conn_name='pub', conn_type='publish',
                                   type='pub',
                                   fstype='csv',
                                   fsname='modelsuper_window.csv',
                                   transactional=True,
                                   blocksize='1',
        )
        w_supervisor = esp.ModelSupervisorWindow(name='w_supervisor',
                                                 pubsub=True,
                                                 deployment_policy='immediate',
                                                 capacity='100')

        w_request = esp.SourceWindow(name='w_request',
                                     schema=('req_id*:int64','req_key:string',
                                             'req_val:string'),
                                     index_type='empty', insert_only=True)

        w_training = esp.TrainWindow(name='w_training', algorithm='KMEANS')
        w_training.set_parameters(nClusters='2', initSeed='1',
                                  dampingFactor='0.8', fadeOutFactor='0.05',
                                  disturbFactor='0.01', nInit='50',
                                  velocity='5', commitInterval='25')
        w_training.set_inputs(inputs='x_c,y_c')

        w_scoring = esp.ScoreWindow(name='w_scoring',
                                    schema=('id*:int64','x_c:double','y_c:double',
                                            'seg:int32','min_dist:double','model_id:int64'))
        w_scoring.add_online_model('KMEANS',
                                   input_map={'inputs':'x_c,y_c'},
                                   output_map={'labelOut':'seg',
                                               'minDistanceOut':'min_dist',
                                               'modelIdOut':'model_id'})

        proj.add_query(cq)
        cq.add_window(w_supervisor)
        cq.add_window(w_request)
        cq.add_window(w_training)
        cq.add_window(w_scoring)
        cq.add_window(w_source)

        w_source.add_target(w_training,role='data')
        w_training.add_target(w_supervisor,role='model')
        w_request.add_target(w_supervisor,role='request')
        w_supervisor.add_target(w_scoring,role='model')
        w_source.add_target(w_scoring,role='data')

        self._test_model_file('modelsuper_window.xml', proj)
