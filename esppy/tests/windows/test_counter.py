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


class TestCounterWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('counter_window.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto', n_threads='1')

        cq = esp.ContinuousQuery(name='cq_01')

        src = esp.SourceWindow(name='src_win',
                               schema=('ID*:int32','symbol:string','currency:int32',
                                       'udate:int64','msecs:int32','price:double','quant:int32',
                                       'venue:int32','broker:int32','buyer:int32','seller:int32',
                                       'buysellflg:int32'))

        counter_win = esp.CounterWindow(name='count',
                                        count_interval='5 seconds',
                                        clear_interval='20 seconds')

        compute_win = esp.ComputeWindow(name='compute_count',
                                        schema=('input*:string','totalCount:int32'))
        compute_win.add_field_expression('totalCount')
        compute_win.add_connector('fs',
                                   conn_name='sub', conn_type='subscribe',
                                   type='sub',
                                   fstype='csv',
                                   fsname='result.out',
                                   snapshot=True,
                                   collapse=True)

        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(counter_win)
        cq.add_window(compute_win)

        src.add_target(counter_win)
        counter_win.add_target(compute_win)

        self._test_model_file('counter_window.xml', proj)
