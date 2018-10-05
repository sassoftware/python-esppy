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
import unittest
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestLogger(tm.TestCase):

    def setUp(self):
        self.s = esppy.ESP(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.project = self.s.install_project(model_xml_path)
        self.query = self.project.queries['contquery']

    def tearDown(self):
        self.s.delete_projects(filter="match(name, '^.*UnitTest.*$')")

    def test_str(self):
        logger = self.s.get_logger('esp')
        logger.set_level('warn')
        self.assertEqual(str(logger), "Logger(name='esp', level='warn')")

    def test_repr(self):
        logger = self.s.get_logger('esp')
        logger.set_level('warn')
        self.assertEqual(repr(logger), "Logger(name='esp', level='warn')")

    def test_set_level(self):
        logger = self.s.get_logger('esp')
        logger.set_level('info')
        self.assertEqual(logger.level, 'info')

        # Using method
        logger.set_level('WARN')
        self.assertEqual(logger.level, 'warn')

        # Using property
        logger.level = 'ERROR'
        self.assertEqual(logger.level, 'error')


if __name__ == '__main__':
   tm.runtests()
