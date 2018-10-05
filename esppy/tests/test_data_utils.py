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
import esppy
import os
import six
import sys
import unittest
from esppy.utils.data import get_project_data
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestDataUtils(tm.TestCase):

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

    def test_get_project_data(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')

        # From Project
        out = get_project_data(self.project)
        self.assertEqual(out, self.project.to_xml())

        # From file object
        out = get_project_data(open(model_xml_path))
        self.assertEqual(out, open(model_xml_path).read())

        # From XML string
        out = get_project_data(open(model_xml_path).read())
        self.assertEqual(out, open(model_xml_path).read())

        # From file path
        out = get_project_data(model_xml_path)
        self.assertEqual(out, open(model_xml_path).read())

        # From URL
        out = get_project_data('file://' + model_xml_path)
        self.assertEqual(out, open(model_xml_path).read())

        # Unknown type
        with self.assertRaises(TypeError):
            get_project_data(0)


if __name__ == '__main__':
   tm.runtests()
