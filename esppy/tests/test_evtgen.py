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
import pandas as pd
import six
import esppy
import sys
import unittest
from esppy.config import ESP_ROOT
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestEventGenerators(tm.TestCase):

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
        for key, value in self.s.get_event_generators().items():
            if 'UnitTest' in key:
                self.s.delete_event_generator(key)

    def test_str(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'],
                                           data='file://%s' % data_csv_path,
                                           name='UnitTestEvtGen')
        self.assertRegex(str(eg), "EventGenerator\(name=u?'UnitTestEvtGen', publish_target=u?'dfESP://.+?:\d+/%s'\)" % self.query['w_data'].fullname.replace('.', '/'))

    def test_repr(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'],
                                           data='file://%s' % data_csv_path,
                                           name='UnitTestEvtGen')
        self.assertRegex(repr(eg), "EventGenerator\(name=u?'UnitTestEvtGen', publish_target=u?'dfESP://.+?:\d+/%s'\)" % self.query['w_data'].fullname.replace('.', '/'))

    def test_event_data(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'], name='UnitTestEvtGen')

        # Set file object
        eg.event_data = open(data_csv_path)
        self.assertEqual(eg.event_data, open(data_csv_path, 'r').read())

        # Set file path
        eg.event_data = data_csv_path
        self.assertEqual(eg.event_data, open(data_csv_path, 'r').read())

        # Set URL
        eg.event_data = 'file://%s' % data_csv_path
        self.assertEqual(eg.event_data, 'file://%s' % data_csv_path)

        # Set CSV content
        eg.event_data = open(data_csv_path, 'rb').read().decode('utf-8')
        self.assertEqual(eg.event_data, open(data_csv_path, 'rb').read().decode('utf-8'))

        # Set DataFrame
        eg.event_data = pd.read_csv(data_csv_path, header=None)
        self.assertEqual(eg.event_data, open(data_csv_path, 'rb').read().decode('utf-8'))

    def test_publish_target(self):
        eg = self.s.create_event_generator(self.query['w_data'], name='UnitTestEvtGen')
        self.assertRegex(eg.publish_target, r'^dfESP://.+?:\d+/%s$' % self.query['w_data'].fullname.replace('.', '/'))

        # Set target to new window
        eg.publish_target = self.query['w_calculate']
        self.assertRegex(eg.publish_target, r'^dfESP://.+?:\d+/%s$' % self.query['w_calculate'].fullname.replace('.', '/'))

        # Set target to explicit URL
        eg.publish_target = 'dfESP://foo.com:12345/project/contquery/window'
        self.assertEqual(eg.publish_target, 'dfESP://foo.com:12345/project/contquery/window')

    def test_is_running(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'],
                                           data='file://%s' % data_csv_path,
                                           name='UnitTestEvtGen')
        self.assertFalse(eg.is_running)

        eg.start()
        self.assertTrue(eg.is_running)

        eg.stop()
        self.assertFalse(eg.is_running)

    def test_url(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'],
                                           data='file://%s' % data_csv_path,
                                           name='UnitTestEvtGen')
        self.assertRegex(eg.url, r'^https?://.+?:\d+/%s/eventGenerators/%s' % (ESP_ROOT, eg.name))

    def test_from_xml(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'],
                                           data='file://%s' % data_csv_path,
                                           name='UnitTestEvtGen')
        xml = eg.to_xml()
        neweg = eg.from_xml(xml)
        self.assertEqual(neweg.name, eg.name)
        self.assertEqual(neweg.publish_target, eg.publish_target)

    def test_start(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'],
                                           data='file://%s' % data_csv_path,
                                           name='UnitTestEvtGen')
        self.assertFalse(eg.is_running)
        eg.start()
        self.assertTrue(eg.is_running)

    @unittest.skip('Hangs waiting for server')
    def test_stop(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'],
                                           data='file://%s' % data_csv_path,
                                           name='UnitTestEvtGen')
        eg.start()
        self.assertTrue(eg.is_running)
        eg.stop()
        self.assertFalse(eg.is_running)

    def test_initialize(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'],
                                           data='file://%s' % data_csv_path,
                                           name='UnitTestEvtGen')
        # TODO: Not sure how to verify this
        eg.initialize()

    def test_delete(self):
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        eg = self.s.create_event_generator(self.query['w_data'],
                                           data='file://%s' % data_csv_path,
                                           name='UnitTestEvtGen')
        self.assertIn('UnitTestEvtGen', self.s.get_event_generators())
        eg.delete()
        self.assertNotIn('UnitTestEvtGen', self.s.get_event_generators())


class TestEventResources(tm.TestCase):

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
        for key, value in self.s.get_event_generators().items():
            if 'UnitTest' in key:
                self.s.delete_event_generator(key)


    def test_types(self):
        from ..evtgen import MapURL, ListURL, SetURL

        eg = self.s.create_event_generator(self.query['w_data'],
                                           name='UnitTestEvtGen')

        eg.resources['dict'] = dict(a=1, b=2)
        eg.resources['list'] = [10, 20]
        eg.resources['set'] = set([100, 200])

        with self.assertRaises(ValueError):
            eg.resources['map-url'] = 'http://esp-server.com/map.txt'

        with self.assertRaises(TypeError):
            eg.resources['map-url'] = None

        eg.resources['map-url'] = MapURL('http://esp-server.com/map.txt')
        eg.resources['list-url'] = ListURL('http://esp-server.com/list.txt')
        eg.resources['set-url'] = SetURL('http://esp-server.com/set.txt')

        eg.resources.add_map_urls(**{'map-url-2': 'http://esp-server.com/map-2.txt'})
        eg.resources.add_list_urls(**{'list-url-2': 'http://esp-server.com/list-2.txt'})
        eg.resources.add_set_urls(**{'set-url-2': 'http://esp-server.com/set-2.txt'})

        self.assertEqual(eg.resources['dict'], dict(a=1, b=2))
        self.assertEqual(eg.resources['list'], [10, 20])
        self.assertEqual(eg.resources['set'], set([100, 200]))

        self.assertEqual(eg.resources['map-url'].url, 'http://esp-server.com/map.txt')
        self.assertEqual(eg.resources['list-url'].url, 'http://esp-server.com/list.txt')
        self.assertEqual(eg.resources['set-url'].url, 'http://esp-server.com/set.txt')

        self.assertEqual(eg.resources['map-url-2'].url, 'http://esp-server.com/map-2.txt')
        self.assertEqual(eg.resources['list-url-2'].url, 'http://esp-server.com/list-2.txt')
        self.assertEqual(eg.resources['set-url-2'].url, 'http://esp-server.com/set-2.txt')

        xml = eg.to_xml(pretty=True)
        neweg = eg.from_xml(xml)

        self.assertEqual(eg.name, neweg.name)
        self.assertEqual(set(eg.resources.keys()), set(neweg.resources.keys()))

    def test_str(self):
        eg = self.s.create_event_generator(self.query['w_data'],
                                           name='UnitTestEvtGen')
        eg.resources['dict'] = dict(a=1)
        eg.resources['list'] = [10, 20]
        eg.resources['set'] = set([100])

        egstr = str(eg.resources)
        self.assertRegex(egstr, r"'dict': \{'a': 1\}")
        self.assertRegex(egstr, r"'set': (\{100\}|set\(\[100\]\))")
        self.assertRegex(egstr, r"'list': \[10, 20\]")

    def test_repr(self):
        eg = self.s.create_event_generator(self.query['w_data'],
                                           name='UnitTestEvtGen')
        eg.resources['dict'] = dict(a=1)
        eg.resources['list'] = [10, 20]
        eg.resources['set'] = set([100])

        egstr = repr(eg.resources)
        self.assertRegex(egstr, r"'dict': \{'a': 1\}")
        self.assertRegex(egstr, r"'set': (\{100\}|set\(\[100\]\))")
        self.assertRegex(egstr, r"'list': \[10, 20\]")

    def test_delitem(self):
        eg = self.s.create_event_generator(self.query['w_data'],
                                           name='UnitTestEvtGen')
        eg.resources['dict'] = dict(a=1)
        eg.resources['list'] = [10, 20]
        eg.resources['set'] = set([100])

        self.assertEqual(set(eg.resources.keys()), set(['dict', 'list', 'set']))

        del eg.resources['dict']
        del eg.resources['list']

        self.assertEqual(set(eg.resources.keys()), set(['set']))

        del eg.resources['set']

        with self.assertRaises(KeyError):
            del eg.resources['set']

        
if __name__ == '__main__':
   tm.runtests()
