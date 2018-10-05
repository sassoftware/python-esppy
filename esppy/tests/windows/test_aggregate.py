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
from . import utils as tm


class TestAggregateWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('aggregate_window.xml')

    def test_field_expressions(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest')

        cq = esp.ContinuousQuery(name='cq_01')

        src = esp.SourceWindow(name='src_win',
                               schema=('tradeID*:string','security:string',
                                       'quantity:int32','price:double',
                                       'traderID:int64','dateD:date','timeD:stamp'))
        src.add_connector('fs', conn_name='pub', conn_type='publish',
                           type='pub',
                           fstype='csv',
                           fsname='aggregate_window.csv',
                           transactional=True,
                           dateformat='%Y-%m-%d %H:%M:%S')

        aggregate_win = esp.AggregateWindow(name='aggregate',
                                            schema=('traderID*:int64',
                                                    'cat_security:string',
                                                    'cat_quantity:string',
                                                    'cat_price:string',
                                                    'cat_dateD:string',
                                                    'cat_timeD:string'))
        aggregate_win.add_field_expression('ESP_aCat(security,",")')
        aggregate_win.add_field_expression('ESP_aCat(quantity,",")')
        aggregate_win.add_field_expression('ESP_aCat(price,",")')
        aggregate_win.add_field_expression('ESP_aCat(dateD,",")')
        aggregate_win.add_field_expression('ESP_aCat(timeD,",")')
        aggregate_win.add_connector('fs',
                                   conn_name='fsout', conn_type='subscribe',
                                   type='sub',
                                   fstype='csv',
                                   fsname='result.out',
                                   snapshot=True,
                                   dateformat='%Y-%m-%d %H:%M:%S')

        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(aggregate_win)

        src.add_target(aggregate_win)

        self._test_model_file('aggregate_window.xml', proj)

    @unittest.skip('Need way to load plugin')
    def test_plugin(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto', n_threads='1')

        cq = esp.ContinuousQuery(name='cq_01')

        src = esp.SourceWindow(name='src_win',
                               schema=('ID*:int32', 'symbol:string',
                                       'quantity:int32', 'price:double'))
        src.add_connector('fs', conn_name='pub', conn_type='publish',
                          type='pub',
                          fstype='csv',
                          fsname='aggregate_with_plugin.csv',
                          transactional=True)

        aggregate_win = esp.AggregateWindow(name='aw',
                                            schema=('symbol*:string',
                                                    'totalQuant:int32',
                                                    'maxPrice:double'))
        aggregate_win.add_field_plugin(plugin='plugin',function='summationAggr',additive=True)
        aggregate_win.add_field_plugin(plugin='plugin',function='maximumAggr',additive_insert_only=True)
        aggregate_win.add_connector('fs',
                                   conn_name='sub', conn_type='subscribe',
                                   type='sub',
                                   fstype='csv',
                                   fsname='result.out',
                                   snapshot=True)

        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(aggregate_win)
        src.add_target(aggregate_win)

        self._test_model_file('aggregate_with_plugin.xml', proj)
