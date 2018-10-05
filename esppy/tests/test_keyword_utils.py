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
from esppy.utils.keyword import dekeywordify, keywordify
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestKeywordUtils(tm.TestCase):

    def test_dekeywordify(self):
        self.assertEqual(dekeywordify('class'), 'class_')
        self.assertEqual(dekeywordify('for'), 'for_')
        self.assertEqual(dekeywordify('foo'), 'foo')

    def test_keywordify(self):
        self.assertEqual(keywordify('class_'), 'class')
        self.assertEqual(keywordify('for_'), 'for')
        self.assertEqual(keywordify('foo_'), 'foo_')


if __name__ == '__main__':
   tm.runtests()
