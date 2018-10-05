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


class TestAlgorithm(tm.TestCase):

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
        out = str(self.s.get_algorithm('calculate', 'Summary'))
        self.assertEqual(out.replace("u'", "'"),
                         "Algorithm(name='Summary', parameters=['windowLength'], input_map=['input'], output_map=['cssOut', 'cvOut', 'maxOut', 'meanOut', 'minOut', 'nOut', 'nmissOut', 'stdOut', 'stderrOut', 'sumOut', 'ussOut', 'varOut'])")

    def test_repr(self):
        out = repr(self.s.get_algorithm('calculate', 'Summary'))
        self.assertEqual(out.replace("u'", "'"),
                         "Algorithm(name='Summary', parameters=['windowLength'], input_map=['input'], output_map=['cssOut', 'cvOut', 'maxOut', 'meanOut', 'minOut', 'nOut', 'nmissOut', 'stdOut', 'stderrOut', 'sumOut', 'ussOut', 'varOut'])")

    def test_subclass_str(self):
        self.assertTrue(hasattr(self.s, 'calculate'))
        self.assertTrue(hasattr(self.s.calculate, 'Summary'))

        out = str(self.s.calculate.Summary())
        self.assertRegex(out, r"Summary\(name=u?'w_\w+', contquery=None, project=None\)")

    def test_subclass_repr(self):
        self.assertTrue(hasattr(self.s, 'calculate'))
        self.assertTrue(hasattr(self.s.calculate, 'Summary'))

        out = repr(self.s.calculate.Summary())
        self.assertRegex(out, r"Summary\(name=u?'w_\w+', contquery=None, project=None\)")


if __name__ == '__main__':
   tm.runtests()
