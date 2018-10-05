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

''' Utilities for testing windows '''

from __future__ import print_function, division, absolute_import, unicode_literals

import copy
import os
import re
import sys
import unittest
import six
import esppy
from .. import utils as tm
from ..utils import xml

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = os.path.normpath(os.path.join(tm.get_data_dir(), '..', 'windows', 'data'))


class WindowTestCase(tm.TestCase):

    def setUp(self):
        self.s = esppy.ESP(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

    def tearDown(self):
        self.s.delete_projects(filter="match(name, '^.*UnitTest.*$')")

    def _load_project(self, model):
        model_file = os.path.join(DATA_DIR, model)
        model_xml = tm.normalize_xml(open(model_file, 'r').read())
        return self.s.Project.from_xml(model_xml)

    def _test_model_file(self, model, project=None):
        model_file = os.path.join(DATA_DIR, model)
        model_xml = tm.normalize_xml(open(model_file, 'r').read())

        if project is None:
            proj1 = self.s.Project.from_xml(model_xml)
        else:
            proj1 = project

        proj1_xml = tm.normalize_xml(proj1.to_xml(pretty=True))

        proj2 = self.s.load_project(model_file, start=False)
        proj2_xml = tm.normalize_xml(proj2.to_xml(pretty=True))

        tm.elements_equal(model_xml, proj1_xml)
        tm.elements_equal(model_xml, proj2_xml)

    def _test_copy(self, model, contquery, window):
        proj1 = self._load_project(model)

        # Shallow copy
        xml1 = proj1[contquery][window].to_xml(pretty=True)
        xml2 = proj1[contquery][window].copy().to_xml(pretty=True)

        self.assertEqual(xml1, xml2)

        # Deep copy
        xml2 = proj1[contquery][window].copy(deep=True).to_xml(pretty=True)

        self.assertEqual(xml1, xml2)
