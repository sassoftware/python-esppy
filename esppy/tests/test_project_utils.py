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
from esppy.utils.project import expand_path
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestProjectUtils(tm.TestCase):

    def test_expand_path(self):
        self.assertEqual(expand_path('*.*.*'), [None, None, None])
        self.assertEqual(expand_path('*/*/*'), [None, None, None])
        self.assertEqual(expand_path('a.*.foo'), ['a', None, 'foo'])
        self.assertEqual(expand_path('a.b.foo'), ['a', 'b', 'foo'])
        self.assertEqual(expand_path('a.b|c.foo'), ['a', ['b', 'c'], 'foo'])
        self.assertEqual(expand_path('a.b|c.foo|bar|baz'), ['a', ['b', 'c'], ['foo', 'bar', 'baz']])
        self.assertEqual(expand_path('a/b|c/foo|bar|baz'), ['a', ['b', 'c'], ['foo', 'bar', 'baz']])


if __name__ == '__main__':
   tm.runtests()
