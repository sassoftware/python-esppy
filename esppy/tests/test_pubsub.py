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

import datetime
import os
import six
import esppy
import sys
import time
import unittest
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestPubSub(tm.TestCase):

    def setUp(self):
        self.s = esppy.ESP(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        model_xml_path = os.path.join(DATA_DIR, 'sub_data_csv_model.xml')
        name = 'project_01_UnitTest'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.project = self.s.load_project(model_xml_path, name=name)

    def tearDown(self):
        self.s.delete_projects(filter="match(name, '^.*UnitTest.*$')")

    def test_sub_data_csv(self):
        src = self.s.get_window('project_01_UnitTest/cq_01/src_win')
        compute_count = self.s.get_window(
                            'project_01_UnitTest/cq_01/compute_count')

        src.subscribe()
        compute_count.subscribe(mode='updating')
        time.sleep(5)

        src.publish_events(os.path.join(DATA_DIR, 'sub_data_csv_data.csv'))
        time.sleep(5)

        self.assertContentsEqual(src.to_csv(), 
            os.path.join(DATA_DIR, 'expected', 'sub_data_csv_result1.csv'))
        self.assertContentsEqual(compute_count.to_csv(), 
            os.path.join(DATA_DIR, 'expected', 'sub_data_csv_result2.csv'))


if __name__ == '__main__':
   tm.runtests()
