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


class TestFunctionalWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('functional_window.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto', n_threads='1')

        cq = esp.ContinuousQuery(name='cq_01',trace='src_win transform')

        src = esp.SourceWindow(name='src_win',
                               schema=('id*:int64','symbol:string','quant:int32',
                                       'dateD:date','price:double','timeD:stamp'))
        src.add_connector('fs',
                          conn_name='pub', conn_type='publish',
                          type='pub',
                          fstype='csv',
                          fsname='functional_window.csv',
                          dateformat='%Y-%m-%d %H:%M:%S')

        functional_win = esp.FunctionalWindow(name='transform',
                                              schema=('id*:int64', 'symbol:string',
                                                      'total:string', 'timeD:stamp',
                                                      'timeString:string',
                                                      'dateD:date', 'dateSstring:string'))
        functional_win.set_function_context_functions(
            total='string(product($quant, $price))',
            timeString='timeGmtString(quotient($timeD,1000000),\'%Y-%m-%d %H:%M:%S\')',
            dateSstring='timeGmtString($dateD,\'%Y-%m-%d %H:%M:%S\')')
        functional_win.add_connector('fs',
                                     conn_name='sub', conn_type='subscribe',
                                     type='sub',
                                     fstype='csv',
                                     fsname='result.out',
                                     snapshot=True,
                                     dateformat='%Y-%m-%d %H:%M:%S')

        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(functional_win)

        src.add_target(functional_win)

        self._test_model_file('functional_window.xml', proj)
