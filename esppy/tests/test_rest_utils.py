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
from esppy.utils.rest import get_params, RESTHelpers, to_underscore, to_camel
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestRESTUtils(tm.TestCase):

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

    def test_to_camel(self):
        self.assertEqual(to_camel('foo_bar_baz'), 'fooBarBaz')
        self.assertEqual(to_camel('Foo_Bar_Baz'), 'FooBarBaz')

    def test_to_underscore(self):
        self.assertEqual(to_underscore('FooBarBaz'), 'foo_bar_baz')
        self.assertEqual(to_underscore('fooBarBaz'), 'foo_bar_baz')

    def test_get_params(self):
        self.assertEqual(get_params(a=None,
                                    b=True,
                                    c=False,
                                    d=self.project,
                                    e=['x', 'y', 'z'],
                                    f=100,
                                    g=[self.project, self.project['contquery']]),
                         dict(b='true',
                              c='false',
                              d='ESPUnitTestProjectSA',
                              e='x|y|z',
                              f='100',
                              g='ESPUnitTestProjectSA|contquery'))


if __name__ == '__main__':
   tm.runtests()
