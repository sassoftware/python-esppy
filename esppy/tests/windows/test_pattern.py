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


class TestPatternWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('pattern_window.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto',
                                  n_threads='1', compress_open_patterns=True)

        cq = esp.ContinuousQuery(name='cq_01')

        src = esp.SourceWindow(name='src_win', 
                               schema=('ID*:int32','symbol:string',
                                       'price:double','buy:int32','tradeTime:date'))
        src.add_connector('fs',
                          conn_name='pub', conn_type='publish',
                          type='pub',
                          fstype='csv',
                          fsname='pattern_window.csv',
                          transactional=True,
                          blocksize='1',
                          dateformat='%Y-%m-%d %H:%M:%S')

        pattern_win = esp.PatternWindow(name='pattern_win',
                                        schema=('ID*:int64','ID1:int32','ID2:int32'))
        pattern1=pattern_win.create_pattern(name='pattern1')
        pattern1.add_event('src_win','e1','symbol=="IBM" and price > 100.00 and b == buy')
        pattern1.add_event('src_win','e2','symbol=="SUN" and price > 25.000 and b == buy')
        pattern1.set_logic('fby(e1, e2)')
        pattern1.add_field_selection('e1','ID')
        pattern1.add_field_selection('e2','ID')
        pattern1.add_timefield('tradeTime','src_win')
        pattern_win.add_connector('fs',
                                  conn_name='sub', conn_type='subscribe',
                                  type='sub',
                                  fstype='csv',
                                  fsname='result.out',
                                  snapshot=True)

        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(pattern_win)

        src.add_target(pattern_win)

        self._test_model_file('pattern_window.xml', proj)
