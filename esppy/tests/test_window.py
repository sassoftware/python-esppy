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

import copy
import datetime
import esppy
import os
import pandas as pd
import six
import sys
import time
import unittest
from esppy.config import ESP_ROOT
from esppy.plotting import ChartLayout
from esppy.windows import Subscriber, Publisher, Window
from esppy.windows.base import param_iter, var_mapper, Target
from esppy.utils import xml
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestParameterIterator(tm.TestCase):

    def test_empty(self):
        piter = param_iter({})
        self.assertEqual(next(piter), {})
        self.assertEqual(next(piter), {})

    def test_dict(self):
        for i, item in enumerate(param_iter(dict(a=[1, 2, 3], b='foo'))):
            if i == 0:
                self.assertEqual(item, dict(a=1, b='foo')) 
            elif i == 1:
                self.assertEqual(item, dict(a=2, b='foo')) 
            elif i == 2:
                self.assertEqual(item, dict(a=3, b='foo')) 
            elif i == 3:
                self.assertEqual(item, dict(a=1, b='foo')) 
                break

    def test_list(self):
        for i, item in enumerate(param_iter([dict(a=1, b='foo'), dict(b=2)])):
            if i == 0:
                self.assertEqual(item, dict(a=1, b='foo'))
            elif i == 1:
                self.assertEqual(item, dict(b=2))
            elif i == 2:
                self.assertEqual(item, dict(a=1, b='foo'))
                break


class TestUtils(tm.TestCase):

    def test_var_mapper(self):
        data = dict(num=[1, 2], char=['a', 'b'], bool=[True, False])

        df = pd.DataFrame.from_dict(data)

        # Dict mapping
        out = var_mapper(df,  dict(num=dict(outname={1:100, 2:200})))
        self.assertEqual(out, dict(outname=[100, 200]))

        # Function mapping
        def map_chars(x):
            out = []
            for item in x.values:
                if item == 'a':
                    out.append('FIRST')
                else:
                    out.append(item.upper())
            return out

        out = var_mapper(df,  dict(char=dict(outname=map_chars)))
        self.assertEqual(out, dict(outname=['FIRST', 'B']))


class TestTarget(tm.TestCase):

    def setUp(self):
        self.target1 = Target('T1', role='data', slot=0)
        self.target1a = Target('T1', role='data', slot=0)
        self.target2 = Target('T2', role='model', slot=1)
        self.target3 = Target('T3', role='left')
        self.target4 = Target('T4', role='right')
        self.target5 = Target('T5', slot=2)
        self.target6 = Target('T6')

    def test_equality(self):
        self.assertEqual(self.target1, self.target1) 
        self.assertNotEqual(self.target1, self.target1a) 

    def test_str(self):
        self.assertEqual(str(self.target1), "Target('T1', role='data', slot=0)")
        self.assertEqual(str(self.target2), "Target('T2', role='model', slot=1)")
        self.assertEqual(str(self.target3), "Target('T3', role='left')")
        self.assertEqual(str(self.target4), "Target('T4', role='right')")
        self.assertEqual(str(self.target5), "Target('T5', slot=2)")
        self.assertEqual(str(self.target6), "Target('T6')")

    def test_repr(self):
        self.assertEqual(repr(self.target1), "Target('T1', role='data', slot=0)")
        self.assertEqual(repr(self.target2), "Target('T2', role='model', slot=1)")
        self.assertEqual(repr(self.target3), "Target('T3', role='left')")
        self.assertEqual(repr(self.target4), "Target('T4', role='right')")
        self.assertEqual(repr(self.target5), "Target('T5', slot=2)")
        self.assertEqual(repr(self.target6), "Target('T6')")


class TestSubscriber(tm.TestCase):

    def setUp(self):
        self.s = esppy.ESP(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.project = self.s.install_project(model_xml_path)
        self.query = self.project.queries['contquery']
        self.window = self.project.queries['contquery']['w_data']
        self.sub = None

    def tearDown(self):
        self.s.delete_projects(filter="match(name, '^.*UnitTest.*$')")
        if self.sub is not None:
            self.sub.close()

    def test_init(self):
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        events = dict(message=None, event=None, open=None, close=None)

        def on_event(sock, event):
            events['event'] = event

        def on_message(sock, message):
            events['message'] = message

        def on_open(sock):
            events['open'] = True

        def on_close(sock, code, reason=None):
            events['close'] = True

        self.sub = Subscriber(self.window,
                              mode='streaming',
                              on_event=on_event,
                              on_message=on_message,
                              on_close=on_close,
                              on_open=on_open)
        self.sub.start()
        self.window.publish_events(data_path)
        time.sleep(3) 
        self.sub.close()

        # Wait for on_close event
        count = 0
        while events['close'] is not True:
            time.sleep(2) 
            count += 1
            if count >= 15:
                break

        self.assertTrue(isinstance(events['event'], pd.DataFrame))
        self.assertTrue(isinstance(events['message'], six.string_types))
        self.assertTrue(events['open'] is True)
        self.assertTrue(events['close'] is True)

    def test_url(self):
        sub = Subscriber(self.window)
        # NOTE: schema is always forced to true internally
        self.assertEqual(sub.url.split('/%s/subscribers/' % ESP_ROOT, 1)[-1],
                         'ESPUnitTestProjectSA/contquery/w_data/?'
                         'format=xml&mode=updating&pagesize=50&schema=true')

    def test_properties(self):
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        self.sub = sub = Subscriber(self.window)

        self.assertFalse(sub.is_active)

        sub.start()
        self.window.publish_events(data_path)
        time.sleep(3) 

        self.assertTrue(sub.is_active)

        sub.mode = 'streaming'
        sub.pagesize = 10
        sub.sort = 'id'
        sub.interval = 200
        sub.filter = 'in(id, 1, 2, 3, 4, 5, 6)'
        sub.separator = ';'

        self.assertEqual(sub.mode, 'streaming')
        self.assertEqual(sub.pagesize, 10)
        self.assertEqual(sub.sort, 'id')
        self.assertEqual(sub.interval, 200)
        self.assertEqual(sub.filter, 'in(id, 1, 2, 3, 4, 5, 6)')
        self.assertEqual(sub.separator, ';')

        time.sleep(3) 


class TestPublisher(tm.TestCase):

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


class TestWindow(tm.TestCase):

    def setUp(self):
        self.s = esppy.ESP(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.project = self.s.install_project(model_xml_path)
        self.query = self.project.queries['contquery']

    def tearDown(self):
        try:
            self.s.delete_projects(filter="match(name, '^.*UnitTest.*$')")
        except:
            pass

    def test_schema(self):
        win = Window(schema='foo*:int64,bar:int32,baz:double') 
        self.assertEqual(win.schema.schema_string, 'foo*:int64,bar:int32,baz:double')

        win.schema = 'a:double', 'b:double'
        self.assertEqual(win.schema.schema_string, 'a:double,b:double')

    def test_subscriber_url(self):
        self.assertEqual(self.query['w_data'].subscriber_url.split('/%s/subscribers/' % ESP_ROOT)[-1],
                         'ESPUnitTestProjectSA/contquery/w_data/')

    def test_publisher_url(self):
        self.assertEqual(self.query['w_data'].publisher_url.split('/%s/publishers/' % ESP_ROOT)[-1],
                         'ESPUnitTestProjectSA/contquery/w_data/')

    def test_event_horizon(self):
        win = self.query['w_data']

        # TODO: Need a way to verify

        # Datetime
        win.subscribe(horizon=datetime.datetime.now() + datetime.timedelta(hours=1))
        time.sleep(2)
        win.unsubscribe()
        time.sleep(1)

        # Time delta
        win.subscribe(horizon=datetime.timedelta(hours=1))
        time.sleep(2)
        win.unsubscribe()
        time.sleep(1)

        # Date
        win.subscribe(horizon=(datetime.datetime.now() + datetime.timedelta(hours=1)).date())
        time.sleep(2)
        win.unsubscribe()
        time.sleep(1)

        # Time
        win.subscribe(horizon=(datetime.datetime.now() + datetime.timedelta(hours=1)).time())
        time.sleep(2)
        win.unsubscribe()
        time.sleep(1)

        # Number of events
        win.subscribe(horizon=20)
        time.sleep(2)
        win.unsubscribe()
        time.sleep(1)

        # Unknown type
        win.subscribe(win)
        time.sleep(2)
        win.unsubscribe()
        time.sleep(1)

    def test_add_event_transformer(self):
        win = self.query['w_data']

        def square_x(event):
            event['x_c'] = event['x_c'] * event['x_c']
            return event

        win.add_event_transformer('clip', upper=4)
        win.add_event_transformer(square_x)

        data_path = os.path.join(DATA_DIR, 'data_sa.csv')

        # Start subscriber
        win.subscribe()
        time.sleep(2)
        
        win.publish_events(data_path)
        time.sleep(2)

        self.assertEqual(list(win.data.index), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
        self.assertEqual(win['x_c'].tolist(), [1, 4, 9, 16, 16, 16, 9, 16, 4, 16, 1])
        self.assertEqual(win['y_c'].tolist(), [1, 1, 0, 4, 4, 4, 1, 4, 2, 4, 3])

        def negate_x(event):
            event['x_c'] = -event['x_c']
            return event

        win.add_event_transformer(negate_x)

        self.assertEqual(list(win.data.index), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
        self.assertEqual(win['x_c'].tolist(), [-1, -4, -9, -16, -16, -16, -9, -16, -4, -16, -1])
        self.assertEqual(win['y_c'].tolist(), [1, 1, 0, 4, 4, 4, 1, 4, 2, 4, 3])

        win.add_event_transformer('abs')

        self.assertEqual(list(win.data.index), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
        self.assertEqual(win['x_c'].tolist(), [1, 4, 9, 16, 16, 16, 9, 16, 4, 16, 1])
        self.assertEqual(win['y_c'].tolist(), [1, 1, 0, 4, 4, 4, 1, 4, 2, 4, 3])

        # TODO: Remove once server crash is fixed
        win._subscriber.close()

    def test_create_subscriber(self):
        win = self.query['w_data']

        out = dict(message=None, event=None, open=None, close=None, error=None)

        def on_message(sock, message):
            out['message'] = message

        def on_event(sock, event):
            out['event'] = event

        def on_open(sock):
            out['open'] = True

        def on_close(sock, code, reason=None):
            out['close'] = True

        def on_error(sock, error):
            out['error'] = error 

        sub = win.create_subscriber(on_message=on_message,
                                    on_event=on_event,
                                    on_open=on_open,
                                    on_close=on_close,
                                    on_error=on_error)
        sub.start()
        time.sleep(2)

        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        win.publish_events(data_path)
        time.sleep(2)

        sub.close()

        # Wait for on_close event
        count = 0
        while out['close'] is not True:
            time.sleep(2) 
            count += 1
            if count >= 15:
                break

        self.assertTrue(isinstance(out['message'], six.string_types))
        self.assertTrue(isinstance(out['event'], pd.DataFrame))
        self.assertTrue(out['open'] is True)
        self.assertTrue(out['close'] is True)
        self.assertTrue(out['error'] is None)
        
    def test_create_publisher(self):
        win = self.query['w_data']

        out = dict(events=None)

        def on_event(sock, event):
            out['events'] = event

        sub = win.create_subscriber(on_event=on_event)
        sub.start()
        pub = win.create_publisher(format='csv', blocksize=10)

        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        pub.send(open(data_path).read())
        time.sleep(5)
        pub.close()

        self.assertTrue(isinstance(out['events'], pd.DataFrame))
        self.assertEqual(len(out['events']), 10)

        sub.close()

    def test_publish_events(self):
        win = self.query['w_data']

        out = dict(msg=None)

        def on_message(sock, message): 
            out['msg'] = message 

        sub = win.create_subscriber(on_message=on_message, format='csv')
        sub.start()

        # File path
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        win.publish_events(data_path)
        time.sleep(3)
        self.assertEqual(out['msg'].strip(), 'I,N, 11,1.000000,3.000000')
        out['msg'] = None

        # File object
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        win.publish_events(open(data_path))
        time.sleep(3)
        self.assertEqual(out['msg'].strip(), 'I,N, 11,1.000000,3.000000')
        out['msg'] = None

        # CSV
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        win.publish_events(open(data_path).read())
        time.sleep(3)
        self.assertEqual(out['msg'].strip(), 'I,N, 11,1.000000,3.000000')
        out['msg'] = None

        # DataFrame
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        df = pd.read_csv(data_path, header=None,
                                    names=['opcode', 'opmode', 'id', 'x_c', 'y_c'])
        win.publish_events(df)
        time.sleep(3)
        self.assertEqual(out['msg'].strip(), 'I,N, 11,1.000000,3.000000')
        out['msg'] = None

        del df['opcode']
        del df['opmode']

        # XML
        xml = []
        for row in df.itertuples(index=False):
            xml.append('<event opcode="insert">')
            for col, value in zip(df.columns, row):
                xml.append('  <%s>%s</%s>' % (col, value, col))
            xml.append('</event>')
            xml.append('')
        xml = '\n'.join(xml)
        win.publish_events(xml)
        time.sleep(3)
        self.assertEqual(out['msg'].strip(), 'I,N, 11,1.000000,3.000000')
        out['msg'] = None

        # JSON
        json = []
        for row in df.itertuples(index=False):
            json.append('{')
            json.append('  "opcode":"insert",')
            json.append('  "event":{')
            for col, value in zip(df.columns, row):
                json.append('  "%s":"%s",' % (col, value))
            json[-1] = json[-1][:-1]
            json.append('  }')
            json.append('}')
            json.append('')
        json = '\n'.join(json)
        win.publish_events(json)
        time.sleep(3)
        self.assertEqual(out['msg'].strip(), 'I,N, 11,1.000000,3.000000')
        out['msg'] = None

        # Properties
        prop = []
        for row in df.itertuples(index=False):
            prop.append('opcode=insert')
            for col, value in zip(df.columns, row):
                prop.append('%s=%s' % (col, value))
            prop.append('')
        prop.append('')
        prop = '\n'.join(prop)
        win.publish_events(prop)
        time.sleep(3)
        self.assertEqual(out['msg'].strip(), 'I,N, 11,1.000000,3.000000')
        out['msg'] = None

        # Unknown
        with self.assertRaises(TypeError):
            win.publish_events(None)

        sub.close()

    def test_apply_transformers(self):
        win = self.query['w_data']

        def neg_square_x(event):
            event['x_c'] = -event['x_c'] * event['x_c']
            return event

        win.add_event_transformer(neg_square_x)

        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        df = pd.read_csv(data_path, header=None, names=['opcode', 'opmode', 'id', 'x_c', 'y_c'])
        df = df.set_index('id')
        del df['opcode']
        del df['opmode']

        out = win.apply_transformers(df)
        self.assertEqual(out.to_csv(),
                         'id,x_c,y_c\n'
                         '1,-1,1\n'
                         '2,-4,1\n'
                         '3,-9,0\n'
                         '4,-36,5\n'
                         '5,-36,6\n'
                         '6,-49,5\n'
                         '7,-9,1\n'
                         '8,-25,5\n'
                         '9,-4,2\n'
                         '10,-49,4\n'
                         '11,-1,3\n')

        win.event_transformers[:] = []
        win.add_event_transformer('abs')

        out = win.apply_transformers(df)
        self.assertEqual(out.to_csv(),
                         'id,x_c,y_c\n'
                         '1,1,1\n'
                         '2,4,1\n'
                         '3,9,0\n'
                         '4,36,5\n'
                         '5,36,6\n'
                         '6,49,5\n'
                         '7,9,1\n'
                         '8,25,5\n'
                         '9,4,2\n'
                         '10,49,4\n'
                         '11,1,3\n')

    def test_contquery(self):
        win = self.query['w_data']
        self.assertEqual(win.contquery, 'contquery')
        self.assertEqual(win.project, 'ESPUnitTestProjectSA')

        win.contquery = 'foo'
        win.project = 'bar'
        self.assertEqual(win.contquery, 'foo')
        self.assertEqual(win.project, 'bar')

        win.contquery = self.query
        self.assertEqual(win.contquery, 'contquery')
        self.assertEqual(win.project, 'ESPUnitTestProjectSA')

    def test_project(self):
        win = self.query['w_data']
        self.assertEqual(win.project, 'ESPUnitTestProjectSA')

        win.project = 'foo'
        self.assertEqual(win.project, 'foo')

        win.project = self.project
        self.assertEqual(win.project, 'ESPUnitTestProjectSA')

    @unittest.skip('Event generators don\'t work with embedded data')
    def test_create_event_generator(self):
        win = self.query['w_data']
        win.subscribe()

        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        egen = win.create_event_generator(data_path)
        egen.start()
        egen.delete()

        time.sleep(20)
        print(win)
        self.assertEqual(len(win), 11)

        win.unsubscribe()

    def test_add_targets(self):
        win = self.query['w_data']
        win.add_targets('foo', 'bar', role='data')
        targets = [(x.name, x.role) for x in win.targets]
        self.assertIn(('w_calculate', 'data'), targets)
        self.assertIn(('foo', 'data'), targets)
        self.assertIn(('bar', 'data'), targets)

    def test_delete_targets(self):
        win = self.query['w_data']
        win.add_targets('foo', 'bar', role='data')
        targets = [(x.name, x.role) for x in win.targets]
        self.assertIn(('w_calculate', 'data'), targets)
        self.assertIn(('foo', 'data'), targets)
        self.assertIn(('bar', 'data'), targets)

        win.delete_targets('foo', 'w_calculate')
        targets = [(x.name, x.role) for x in win.targets]
        self.assertNotIn(('w_calculate', 'data'), targets)
        self.assertNotIn(('foo', 'data'), targets)
        self.assertIn(('bar', 'data'), targets)

    def test_fullname(self):
        self.assertEqual(self.query['w_data'].fullname,
                         'ESPUnitTestProjectSA.contquery.w_data')

    def test_schema_string(self):
        win = self.query['w_data']
        self.assertEqual(win.schema_string, 'id*:int64,x_c:double,y_c:double')

        win.schema.fields.clear()
        self.assertEqual(win.schema_string, '')

        win.schema_string = 'foo:double,bar:int64'
        self.assertEqual(len(win.schema.fields), 2)
        self.assertEqual(win.schema.fields['foo'].type, 'double')
        self.assertEqual(win.schema.fields['bar'].type, 'int64')

    def test_url(self):
        self.assertEqual(self.query['w_data'].url.split('/%s/' % ESP_ROOT, 1)[-1],
                         'ESPUnitTestProjectSA/contquery/w_data/') 

    def test__verify_project(self):
        win = self.query['w_data']
        self.assertTrue(win._verify_project() is None)

        win.project = None
        with self.assertRaises(ValueError):
            win._verify_project()

        win.project = self.project
        win.contquery = None
        with self.assertRaises(ValueError):
            win._verify_project()

    def test_copy(self):
        win = self.query['w_data']

        # Using copy method
        wcopy = win.copy()
        self.assertEqual(wcopy.project, win.project)
        self.assertEqual(wcopy.contquery, win.contquery)
        self.assertEqual(wcopy.schema_string, win.schema_string)
        self.assertEqual(wcopy.event_transformers, win.event_transformers)

        # Using copy method with deep=True
        wcopy = win.copy(deep=True)
        self.assertEqual(wcopy.project, win.project)
        self.assertEqual(wcopy.contquery, win.contquery)
        self.assertEqual(wcopy.schema_string, win.schema_string)
        self.assertTrue(wcopy.schema is not win.schema)
        self.assertEqual(wcopy.event_transformers, win.event_transformers)

        # Using copy.copy
        wcopy = copy.copy(win)
        self.assertEqual(wcopy.project, win.project)
        self.assertEqual(wcopy.contquery, win.contquery)
        self.assertEqual(wcopy.schema_string, win.schema_string)
        self.assertEqual(wcopy.event_transformers, win.event_transformers)

        # Using copy.deepcopy
        wcopy = copy.deepcopy(win)
        self.assertEqual(wcopy.project, win.project)
        self.assertEqual(wcopy.contquery, win.contquery)
        self.assertEqual(wcopy.schema_string, win.schema_string)
        self.assertTrue(wcopy.schema is not win.schema)
        self.assertEqual(wcopy.event_transformers, win.event_transformers)

    def test_to_element(self):
        win = self.query['w_data']
        elem = win.to_element()
        out = xml.to_xml(elem, pretty=True)
        self.assertEqual(out.strip(), '''<window-source index="pi_EMPTY" insert-only="true" name="w_data">
  <schema>
    <fields>
      <field key="true" name="id" type="int64" />
      <field key="false" name="x_c" type="double" />
      <field key="false" name="y_c" type="double" />
    </fields>
  </schema>
  <connectors>
    <connector class="fs" name="publisher" type="publish">
      <properties>
        <property name="blocksize">1</property>
        <property name="fsname">/u/kesmit/gitlab/python-esp/esppy/tests/data//data_sa.csv</property>
        <property name="fstype">csv</property>
        <property name="transactional">true</property>
      </properties>
    </connector>
  </connectors>
</window-source>''')

    def test_to_xml(self):
        win = self.query['w_data']
        out = win.to_xml(pretty=True)
        self.assertEqual(out.strip(), '''<window-source index="pi_EMPTY" insert-only="true" name="w_data">
  <schema>
    <fields>
      <field key="true" name="id" type="int64" />
      <field key="false" name="x_c" type="double" />
      <field key="false" name="y_c" type="double" />
    </fields>
  </schema>
  <connectors>
    <connector class="fs" name="publisher" type="publish">
      <properties>
        <property name="blocksize">1</property>
        <property name="fsname">/u/kesmit/gitlab/python-esp/esppy/tests/data//data_sa.csv</property>
        <property name="fstype">csv</property>
        <property name="transactional">true</property>
      </properties>
    </connector>
  </connectors>
</window-source>''')

    def test_to_graph(self):
        # TODO: Not sure how to verify this
        grph = self.query['w_data'].to_graph(schema=True)

    def test_repr_svg(self):
        # TODO: Not sure how to verify this
        grph = self.query['w_data']._repr_svg_()

    def test_mapping_methods(self):
        win = self.query['w_data']
        win.subscribe()
 
        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        win.publish_events(data_path)
        time.sleep(3)

        self.assertTrue(win['x_c'] is not None)
        time.sleep(3)
        self.assertEqual(len(win['x_c']), 11)

        with self.assertRaises(RuntimeError):
            win['x_c'] = range(100, 111)

        with self.assertRaises(RuntimeError):
             del win['x_c']

        self.assertTrue(isinstance(win.x_c, pd.Series))
        self.assertTrue(isinstance(win.data, pd.DataFrame))

        self.assertEqual(list(iter(win)), ['x_c', 'y_c'])

    def test_enable_tracing(self):
        self.query['w_data'].enable_tracing()

    def test_disable_tracing(self):
        self.query['w_data'].disable_tracing()

    @unittest.skip('Hangs server')
    def test_get_events(self):
        win = self.query['w_data']

        data_path = os.path.join(DATA_DIR, 'data_sa.csv')
        win.publish_events(data_path)
        time.sleep(5)

        out = win.get_events()
        self.assertTrue(isinstance(out, pd.DataFrame))
        self.assertEqual(list(out.columns), ['x_c', 'y_c'])
        self.assertEqual(list(out.index.names), ['id'])
        self.assertEqual(len(out), 11)

    def test_str(self):
        self.assertEqual(str(self.query['w_data']),
                         "SourceWindow(name='w_data', contquery='contquery',"
                         " project='ESPUnitTestProjectSA')")

    def test_repr(self):
        self.assertEqual(repr(self.query['w_data']),
                         "SourceWindow(name='w_data', contquery='contquery',"
                         " project='ESPUnitTestProjectSA')")

    def test_to_data_callback(self):
        win = self.query['w_data']

        def var_generator(data):
            return dict(sum=data['x_c'] + data['y_c'])

        out = win.to_data_callback(x='id', y=['x_c', 'y_c'], var_generator=var_generator)
        data = out(initial=True)
        self.assertEqual(sorted(data.keys()), ['id', 'sum', 'x_c', 'y_c'])
        data = out()
        self.assertEqual(sorted(data.keys()), ['id', 'sum', 'x_c', 'y_c'])
        data = out(terminate=True)
        self.assertTrue(data is None)

        out = win.to_data_callback(x=['id'], y='x_c', extra='y_c')
        data = out(initial=True)
        self.assertEqual(sorted(data.keys()), ['id', 'x_c', 'y_c'])
        data = out()
        self.assertEqual(sorted(data.keys()), ['id', 'x_c', 'y_c'])
        data = out(terminate=True)
        self.assertTrue(data is None)

        out = win.to_data_callback(x=['id'], y='x_c', extra=['y_c'])
        data = out(initial=True)
        self.assertEqual(sorted(data.keys()), ['id', 'x_c', 'y_c'])
        data = out()
        self.assertEqual(sorted(data.keys()), ['id', 'x_c', 'y_c'])
        data = out(terminate=True)
        self.assertTrue(data is None)

        out = win.to_data_callback(extra=['id', 'x_c', 'y_c'])
        data = out(initial=True)
        self.assertEqual(sorted(data.keys()), ['id', 'x_c', 'y_c'])
        data = out()
        self.assertEqual(sorted(data.keys()), ['id', 'x_c', 'y_c'])
        data = out(terminate=True)
        self.assertTrue(data is None)

    def test_streaming_line(self):
        fig = self.query['w_data'].streaming_line(x='id', y=['x_c', 'y_c'])
        out = fig._repr_html_()

        fig = self.query['w_data'].streaming_line(x='id', y='x_c')
        out = fig._repr_html_()

    def test_streaming_scatter(self):
        fig = self.query['w_data'].streaming_scatter(x='id', y=['x_c', 'y_c'])
        out = fig._repr_html_()

        fig = self.query['w_data'].streaming_scatter(x='id', y='x_c')
        out = fig._repr_html_()

    def test_streaming_area(self):
        fig = self.query['w_data'].streaming_area(x='id', y=['x_c', 'y_c'])
        out = fig._repr_html_()

        fig = self.query['w_data'].streaming_area(x='id', y='x_c')
        out = fig._repr_html_()

    def test_streaming_bubble(self):
        fig = self.query['w_data'].streaming_bubble(x='id', y=['x_c', 'y_c'], radius='id')
        out = fig._repr_html_()

        fig = self.query['w_data'].streaming_bubble(x='id', y='x_c', radius='id')
        out = fig._repr_html_()

    def test_streaming_bar(self):
        fig = self.query['w_data'].streaming_bar(x='id', y=['x_c', 'y_c'])
        out = fig._repr_html_()

        fig = self.query['w_data'].streaming_bar(x='id', y='x_c')
        out = fig._repr_html_()

    def test_streaming_hbar(self):
        fig = self.query['w_data'].streaming_hbar(x='id', y=['x_c', 'y_c'])
        out = fig._repr_html_()

        fig = self.query['w_data'].streaming_hbar(x='id', y='x_c')
        out = fig._repr_html_()

    def test_streaming_donut(self):
        fig = self.query['w_data'].streaming_donut(x='id', y='x_c')
        out = fig._repr_html_()

    def test_streaming_pie(self):
        fig = self.query['w_data'].streaming_pie(x='id', y='x_c')
        out = fig._repr_html_()

    def test_streaming_images(self):
        fig = self.query['w_data'].streaming_images('x_c')
        out = fig._repr_html_()

    def test_chart_layout(self):
        fig1 = self.query['w_data'].streaming_line(x='id', y=['x_c', 'y_c'])
        fig2 = self.query['w_data'].streaming_line(x='id', y=['x_c', 'y_c'])
        fig3 = self.query['w_data'].streaming_line(x='id', y=['x_c', 'y_c'])

        layout = ChartLayout(fig1, (fig2, fig3)) 
        
        out = layout._repr_html_()

    def test_streaming_hist(self):
        fig = self.query['w_data'].streaming_hist(centers=['c1', 'c2', 'c3'],
                                                  heights=['h1', 'h2', 'h3'])
        out = fig._repr_html_()

    def test_copyvars(self):
        # Specified columns
        win = self.s.CalculateWindow(input_map=dict(a='A1', b='B1', c='C1'),
                                     output_map=dict(x='X1', y='Y1', z='Z1'))

        win.copyvars = ['A1', 'C1']
        self.assertEqual(list(win.schema.fields.keys()), ['A1', 'C1'])

        # With key
        win = self.s.CalculateWindow(input_map=dict(a='A1*', b='B1', c='C1'),
                                     output_map=dict(x='X1', y='Y1', z='Z1'))

        win.copyvars = ['A1', 'C1']
        self.assertEqual(list(win.schema.fields.keys()), ['A1', 'C1'])
        self.assertTrue(win.schema['A1'].key)
        self.assertFalse(win.schema['C1'].key)

        # With data type
        win = self.s.CalculateWindow(input_map=dict(a='A1:int32', b='B1', c='C1'),
                                     output_map=dict(x='X1', y='Y1', z='Z1'))

        win.copyvars = ['A1', 'C1']
        self.assertEqual(list(win.schema.fields.keys()), ['A1', 'C1'])
        self.assertEqual(win.schema['A1'].type, 'int32')
        self.assertEqual(win.schema['C1'].type, 'double')

        # With data type and key
        win = self.s.CalculateWindow(input_map=dict(a='A1*:int32', b='B1', c='C1'),
                                     output_map=dict(x='X1', y='Y1', z='Z1'))

        win.copyvars = ['A1', 'C1']
        self.assertEqual(list(win.schema.fields.keys()), ['A1', 'C1'])
        self.assertEqual(win.schema['A1'].type, 'int32')
        self.assertEqual(win.schema['C1'].type, 'double')
        self.assertTrue(win.schema['A1'].key)
        self.assertFalse(win.schema['C1'].key)

        # With array type
        win = self.s.CalculateWindow(input_map=dict(a='A1[1-5]:int32', b='B1', c='C1'),
                                     output_map=dict(x='X1', y='Y1', z='Z1'))

        win.copyvars = ['A1', 'C1']
        self.assertEqual(list(win.schema.fields.keys()), ['A1', 'C1'])
        self.assertEqual(win.schema['A1'].type, 'array(i32)')
        self.assertEqual(win.schema['C1'].type, 'double')


if __name__ == '__main__':
   tm.runtests()
