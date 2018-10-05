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
from esppy.metadata import Metadata
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class MetadataSubclass(Metadata):

    def __init__(self, session=None):
        Metadata.__init__(self, session=session)
        self._data = {}

    def __metadata__(self):
        return self._data

    def __setitem__(self, key, value):
        self._data[key] = value 

    def __delitem__(self, key):
        del self._data[key]


class TestMetadata(tm.TestCase):

    def setUp(self):
        self.md = Metadata()
        self.mdsub = MetadataSubclass()

    def test_metadata(self):
        with self.assertRaises(NotImplementedError):
            self.md.__metadata__()

        self.assertTrue(isinstance(self.mdsub.__metadata__(), dict))

    def test_getitem(self):
        with self.assertRaises(NotImplementedError):
            self.md['foo']

        with self.assertRaises(KeyError):
            self.mdsub['foo']

        self.mdsub['foo'] = 'bar'

        self.assertEqual(self.mdsub['foo'], 'bar')

    def test_setitem(self):
        with self.assertRaises(NotImplementedError):
            self.md['foo'] = 'bar'

        self.mdsub['foo'] = 'bar'

        self.assertEqual(self.mdsub['foo'], 'bar')

    def test_delitem(self):
        with self.assertRaises(NotImplementedError):
            del self.md['foo']

        with self.assertRaises(KeyError):
            del self.mdsub['foo']

        self.mdsub['foo'] = 'bar'

        self.assertTrue(self.mdsub['foo'], 'bar')

        del self.mdsub['foo']
        
        with self.assertRaises(KeyError):
            self.mdsub['foo']

    def test_iter(self):
        with self.assertRaises(NotImplementedError):
            for key in self.md:
                pass

        self.mdsub.update(dict(a=1, b=2, c=3))

        for i, key in enumerate(sorted(self.mdsub)):
            if i == 0:
                self.assertEqual(key, 'a')
            elif i == 1: 
                self.assertEqual(key, 'b')
            else:
                self.assertEqual(key, 'c')

    def test_len(self):
        with self.assertRaises(NotImplementedError):
            len(self.md)

        self.mdsub.update(dict(a=1, b=2, c=3))

        self.assertEqual(len(self.mdsub), 3)

    def test_str(self):
        with self.assertRaises(NotImplementedError):
            str(self.md)
        self.assertEqual(str(self.mdsub), '{}')

    def test_repr(self):
        with self.assertRaises(NotImplementedError):
            repr(self.md)
        self.assertEqual(repr(self.mdsub), '{}')


if __name__ == '__main__':
   tm.runtests()
