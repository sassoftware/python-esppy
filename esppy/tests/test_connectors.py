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

import copy
import six
import unittest
from . import utils as tm
from ..connectors.base import (get_connector_class, listify, map_properties,
                               Connector, get_subclasses)
from ..connectors.fs import FileSubscriber, FilePublisher, SocketPublisher


class TestUtils(tm.TestCase):

    def test_listify(self):
        self.assertEqual(listify(None), None)
        self.assertEqual(listify(1), [1])
        self.assertEqual(listify('a'), ['a'])
        self.assertEqual(listify([1, 2]), [1, 2])
        self.assertEqual(listify((1, 2)), [1, 2])

    def test_map_properties(self):
        properties = dict(fsname='filename.txt', fstype='csv', snapshot=True, type='subscribe')
        req, prop = map_properties(FileSubscriber, properties, required='fsname',
                                   delete='type')
        self.assertEqual(list(req), ['filename.txt'])
        self.assertEqual(prop, dict(fstype='csv', snapshot=True))

        # Bad parameter
        prop['foo'] = 'bar'
        with self.assertRaises(KeyError):
            map_properties(FileSubscriber, prop, required='fsname', delete='type')

        # No required values
        properties = dict(fsname='filename.txt', fstype='csv', snapshot=True, type='subscribe')
        prop = map_properties(FileSubscriber, properties, delete='type')
        self.assertEqual(prop, dict(fstype='csv', snapshot=True, fsname='filename.txt'))

    def test_get_subclasses(self):
        out = list(get_subclasses(Connector))
        self.assertIn(FileSubscriber, out)
        self.assertIn(FilePublisher, out)


class TestConnector(tm.TestCase):

    def test_get_connector_class(self):
        # Publisher XML
        out = get_connector_class(r'''<connector name='pub' class='fs'>
                <properties>
                  <property name='type'>pub</property>
                  <property name='fstype'>csv</property>
                  <property name='fsname'>input/input1.csv</property>
                  <property name='dateformat'>%Y-%m-%d %H:%M:%S</property>
                </properties>
              </connector>''')
        self.assertTrue(out is FilePublisher)

        # Subscriber XML
        out = get_connector_class(r'''<connector name='sub' class='fs'>
                <properties>
                  <property name='type'>sub</property>
                  <property name='fstype'>csv</property>
                  <property name='fsname'>input/input1.csv</property>
                  <property name='dateformat'>%Y-%m-%d %H:%M:%S</property>
                </properties>
              </connector>''')
        self.assertTrue(out is FileSubscriber)

        # Subscriber XML with type attribute
        out = get_connector_class(r'''<connector name='sub' class='fs' type='sub'>
                <properties>
                  <property name='fstype'>csv</property>
                  <property name='fsname'>input/input1.csv</property>
                  <property name='dateformat'>%Y-%m-%d %H:%M:%S</property>
                </properties>
              </connector>''')
        self.assertTrue(out is FileSubscriber)

        # Publisher name
        out = get_connector_class('fs', type='publish',
                                  properties=dict(fstype='csv', fsname='input1.csv',
                                                  dateformat='%Y-%m-%d %H:%M:%S'))
        self.assertTrue(out is FilePublisher)

        # Subscriber name
        out = get_connector_class('fs', type='subscribe',
                                  properties=dict(fstype='csv', fsname='input1.csv',
                                                  dateformat='%Y-%m-%d %H:%M:%S'))
        self.assertTrue(out is FileSubscriber)

        # Unspecified type
        out = get_connector_class('fs',
                                  properties=dict(fstype='csv', fsname='input1.csv',
                                                  dateformat='%Y-%m-%d %H:%M:%S'))
        self.assertTrue(out is FileSubscriber)

        # Unrecognized type
        out = get_connector_class('foo',
                                  properties=dict(fstype='csv', fsname='input1.csv',
                                                  dateformat='%Y-%m-%d %H:%M:%S'))
        self.assertTrue(out is Connector)

        # Regex property value selector
        out = get_connector_class('fs', type='publish',
                                  properties=dict(fstype='csv', fsname=':2900',
                                                  dateformat='%Y-%m-%d %H:%M:%S'))
        self.assertTrue(out is SocketPublisher)


if __name__ == '__main__':
    tm.runtests()
