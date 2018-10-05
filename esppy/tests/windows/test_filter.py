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


class TestFilterWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('filter_with_expression.xml')

    def test_expression(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest')

        cq = esp.ContinuousQuery(name='cq_01')

        src = esp.SourceWindow(name='src_win',
                               schema=('id*:int32','charData:string','int64D:int64',
                                       'doubleD:double','dateD:date',
                                       'timestampD:stamp','moneyD:money'))
        src.add_connector('fs', conn_name='pub', conn_type='publish',
                          type='pub',
                          fstype='csv',
                          fsname='filter_with_expression.csv',
                          transactional=True,
                          blocksize='3',
                          dateformat='%Y-%m-%d %H:%M:%S')

        filterWin = esp.FilterWindow(name='filter_w')
        filterWin.set_expression('charData=="update\'s result"')
        filterWin.add_connector('fs',
                                conn_name='sub', conn_type='subscribe',
                                type='sub',
                                fstype='csv',
                                fsname='filter.out',
                                snapshot=True,
                                dateformat='%Y-%m-%d %H:%M:%S')

        filterWin2 = esp.FilterWindow(name='filter_w2')
        filterWin2.expression='charData=="insert4"'
        filterWin2.add_connector('fs',
                                 conn_name='sub', conn_type='subscribe',
                                 type='sub',
                                 fstype='csv',
                                 fsname='filter2.out',
                                 snapshot=True,
                                 dateformat='%Y-%m-%d %H:%M:%S')
        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(filterWin)
        cq.add_window(filterWin2)
        src.add_target(filterWin)
        src.add_target(filterWin2)

        self._test_model_file('filter_with_expression.xml', proj)

    @unittest.skip('Need way to load plugin')
    def test_plugin(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest')

        cq = esp.ContinuousQuery(name='cq_01')

        src = esp.SourceWindow(name='src_win',schema=('ID*:int64','symbol:string',
                                                      'quant:int32','price:money'))

        filterWin = esp.FilterWindow(name='filter_w')
        filterWin.set_plugin('libmethod','findTheElement',
                             context_name='libmethod',context_function='get_derived_context')
        filterWin.add_connector('fs',
                                conn_name='fsout', conn_type='subscribe',
                                type='sub',
                                fstype='csv',
                                fsname='output',
                                snapshot=True)
        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(filterWin)
        src.add_target(filterWin)

        self._test_model_file('filter_with_plugin.xml', proj)

    def test_udf(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest')

        cq = esp.ContinuousQuery(name='cq_01')

        src = esp.SourceWindow(name='src_win',
                               schema=('id*:int32','charData:string','int64D:int64',
                                       'doubleD:double','dateD:date','moneyD:money'))
        src.add_connector('fs',
                          conn_name='pub', conn_type='publish',
                          type='pub',
                          fstype='csv',
                          fsname='filter_with_udf.csv',
                          transactional=True,
                          blocksize='4',
                          dateformat='%Y-%m-%d %H:%M:%S')

        filterWin = esp.FilterWindow(name='filter_w')
        filterWin.set_expression_initializer(funcs={
            'udf1:double': 'private real p p = parameter(1); return (p*2)',
            'udf2:string': 'private string p p = parameter(1); return upper(p)'
        })
        filterWin.set_expression('(udf1(doubleD) > 200.0) and match_string(udf2(charData),\'INSERT\')')
        filterWin.add_connector('fs',
                                conn_name='sub', conn_type='subscribe',
                                type='sub',
                                fstype='csv',
                                fsname='filter.out',
                                snapshot=True,
                                dateformat='%Y-%m-%d %H:%M:%S')

        proj.add_query(cq)
        cq.add_window(src)
        cq.add_window(filterWin)
        src.add_target(filterWin)

        self._test_model_file('filter_with_udf.xml', proj)
