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

import os
import unittest
import esppy
from . import utils as tm


class TestCalculateWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('calculate_window.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest',
                                  pubsub='auto', n_threads='1', use_tagged_token=True)

        cq = esp.ContinuousQuery(name='cq_01',trace='w_calculate')

        src = esp.SourceWindow(name='src_win',
                               schema=('id*:int64','x_c:double','y_c:double'),
                               insert_only=True, autogen_key=False)
        src.add_connector('fs', conn_name='pub', conn_type='publish',
                          type='pub',
                          fstype='csv',
                          fsname='calculate_window.csv',
                          transactional=True,
                          blocksize='1')

        calculate_win = esp.CalculateWindow(name='calculate_win',
                                            algorithm='Correlation',
                                            schema=('id*:int64','y_c:double',
                                                    'x_c:double','corOut:double'))
        calculate_win.set_parameters(windowLength='5')
        calculate_win.set_inputs(x='x_c',y='y_c')
        calculate_win.set_outputs(corOut='corOut')
        calculate_win.add_connector('fs',
                                   conn_name='sub', conn_type='subscribe',
                                   type='sub',
                                   fstype='csv',
                                   fsname='result.out',
                                   snapshot=True)

        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(calculate_win)

        src.add_target(calculate_win,role='data')

        self._test_model_file('calculate_window.xml', proj)
