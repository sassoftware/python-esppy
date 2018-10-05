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


class TestProceduralWindow(tm.WindowTestCase):

    @unittest.skip('Need way to load plugin')
    def test_xml(self):
        self._test_model_file('procedural_window.xml')

    @unittest.skip('Need way to load plugin')
    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest')

        cq = esp.ContinuousQuery(name='cq_01')

        input1 = esp.SourceWindow(name='input1',
                                  schema=('a*:int32','b*:string','c*:double','d*:date',
                                          'intdata:int32','charData:string',
                                          'floatData:double','dateD:date'))
        input1.add_connector('fs',
                             conn_name='pub', conn_type='publish',
                             type='pub',
                             fstype='csv',
                             fsname='procedural_input1.csv',
                             transactional=True,
                             blocksize='1',
                             dateformat='%Y-%m-%d %H:%M:%S')

        input2 = esp.SourceWindow(name='input2',
                                  schema=('a*:int32','b*:string','c*:double',
                                          'd*:date','intdata:int32',
                                          'charData:string','floatData:double','dateD:date'))
        input2.add_connector('fs',
                             conn_name='pub', conn_type='publish',
                             type='pub',
                             fstype='csv',
                             fsname='procedural_input2.csv',
                             transactional=True,
                             blocksize='1',
                             dateformat='%Y-%m-%d %H:%M:%S')

        proc_win = esp.ProceduralWindow(name='proc_win',
                                        schema=('a*:int32','b*:string','c*:double',
                                                'd*:date','intdata:int32','charData:string',
                                                'floatData:double','dateD:date'))
        proc_win.add_cxx_plugin('input1', 'libmethod', ['input1Method', 'input2Method'])
        proc_win.add_connector('fs',
                               conn_name='sub', conn_type='subscribe',
                               type='sub',
                               fstype='csv',
                               fsname='result.out',
                               snapshot=True,
                               dateformat='%Y-%m-%d %H:%M:%S')

        proj.add_query(cq)
        cq.add_windows(input1, input2, proc_win)

        input1.add_target(proc_win)
        input2.add_target(proc_win)

        self._test_model_file('procedural_window.xml', proj)
