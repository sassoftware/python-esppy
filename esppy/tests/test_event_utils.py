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
import time
import unittest
from esppy.utils.events import get_dataframe, get_schema, get_events
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestEventUtils(tm.TestCase):

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

    def test_get_dataframe(self):
        # From Window
        df = get_dataframe(self.query['w_data'])
        self.assertEqual(len(df), 0)
        self.assertEqual(list(df.columns), ['x_c', 'y_c'])
        self.assertEqual(list(df.index.names), ['id'])

        df = df.reset_index()
        self.assertEqual([str(x) for x in df.dtypes.tolist()],
                         ['int64', 'float64', 'float64'])

        # From Schema
        df = get_dataframe(self.query['w_data'].schema)
        self.assertEqual(len(df), 0)
        self.assertEqual(list(df.columns), ['x_c', 'y_c'])
        self.assertEqual(list(df.index.names), ['id'])

        df = df.reset_index()
        self.assertEqual([str(x) for x in df.dtypes.tolist()],
                         ['int64', 'float64', 'float64'])

        # From schema-less Window
        win = self.query['w_data']
        win.schema.fields.clear()
        df = get_dataframe(win)
        self.assertEqual(len(df), 0)
        self.assertEqual(list(df.columns), ['x_c', 'y_c'])
        self.assertEqual(list(df.index.names), ['id'])

        df = df.reset_index()
        self.assertEqual([str(x) for x in df.dtypes.tolist()],
                         ['int64', 'float64', 'float64'])

    def test_get_schema(self):
        sch = get_schema(self.s, 'ESPUnitTestProjectSA.contquery.w_data')
        self.assertEqual(sch.schema_string, 'id*:int64,x_c:double,y_c:double')
       
        sch = get_schema(self.s, self.query['w_data'])
        self.assertEqual(sch.schema_string, 'id*:int64,x_c:double,y_c:double')

        win = self.query['w_data']
        win.schema.fields.clear()
        sch = get_schema(self.s, win)
        self.assertEqual(sch.schema_string, 'id*:int64,x_c:double,y_c:double')

    def test_get_xml_events(self):
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        w_data = self.query['w_data']
        sch = w_data.schema.copy(deep=True)

        # Window as obj
        def get_message(sock, message):
            out = get_events(w_data, message, format='xml', single=True) 
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='xml',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

        # Schema as obj
        def get_message(sock, message):
            out = get_events(sch, message, format='xml', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='xml',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

        # Schema-less Window as obj
        w_data.schema.fields.clear()

        def get_message(sock, message):
            out = get_events(w_data, message, format='xml', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='xml',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

    def test_get_csv_events(self):
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        w_data = self.query['w_data']
        sch = w_data.schema.copy(deep=True)

        # Window as obj
        def get_message(sock, message):
            out = get_events(w_data, message, format='csv', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='csv',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

        # Schema as obj
        def get_message(sock, message):
            out = get_events(sch, message, format='csv', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='csv',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

        # Schema-less Window as obj
        w_data.schema.fields.clear()

        def get_message(sock, message):
            out = get_events(sch, message, format='csv', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='csv',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

    def test_get_json_events(self):
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        w_data = self.query['w_data']
        sch = w_data.schema.copy(deep=True)

        # Window as obj
        def get_message(sock, message):
            out = get_events(w_data, message, format='json', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='json',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

        # Schema as obj
        def get_message(sock, message):
            out = get_events(sch, message, format='json', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='json',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

        # Schema-less Window as obj
        w_data.schema.fields.clear()

        def get_message(sock, message):
            out = get_events(sch, message, format='json', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='json',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

    def test_get_properties_events(self):
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        w_data = self.query['w_data']
        sch = w_data.schema.copy(deep=True)

        # Window as obj
        def get_message(sock, message):
            out = get_events(w_data, message, format='properties', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='properties',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

        # Schema as obj
        def get_message(sock, message):
            out = get_events(sch, message, format='properties', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='properties',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()

        # Schema-less Window as obj
        w_data.schema.fields.clear()

        # Schema-less Window as obj
        def get_message(sock, message):
            out = get_events(sch, message, format='properties', single=True)
            self.assertEqual(list(out.index.names), ['id'])
            self.assertEqual(list(out.columns), ['x_c', 'y_c'])
            self.assertEqual(len(out), 1)

        sub = w_data.create_subscriber(mode='streaming', format='properties',
                                       on_message=get_message)
        self.assertTrue(sub.callbacks['on_message'] is get_message)
        sub.start()
        w_data.publish_events(data_path)
        time.sleep(3)
        sub.close()


if __name__ == '__main__':
   tm.runtests()
