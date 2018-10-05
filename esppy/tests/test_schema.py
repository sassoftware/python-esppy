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
import os
import six
import esppy
import sys
import unittest
from esppy.schema import SchemaField, Schema
from esppy.utils import xml
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestSchemaField(tm.TestCase):

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

    def test_init(self):
        field = SchemaField('a', 'int64', key='true')
        self.assertEqual(field.name, 'a')
        self.assertEqual(field.type, 'int64')
        self.assertTrue(field.key is True)

        field = SchemaField('a', 'int64', key='false')
        self.assertEqual(field.name, 'a')
        self.assertEqual(field.type, 'int64')
        self.assertTrue(field.key is False)

        field = SchemaField('a', 'int64', key=1)
        self.assertEqual(field.name, 'a')
        self.assertEqual(field.type, 'int64')
        self.assertTrue(field.key is True)

        field = SchemaField('a', 'int64', key=0)
        self.assertEqual(field.name, 'a')
        self.assertEqual(field.type, 'int64')
        self.assertTrue(field.key is False)

    def test_copy(self):
        field = SchemaField('foo', 'int64', key='true')

        # Use method
        fcopy = field.copy()
        self.assertTrue(field is not fcopy)
        self.assertEqual(field.name, fcopy.name) 
        self.assertEqual(field.type, fcopy.type) 
        self.assertEqual(field.key, fcopy.key) 

        # Use method with deep=True
        fcopy = field.copy(deep=True)
        self.assertTrue(field is not fcopy)
        self.assertEqual(field.name, fcopy.name) 
        self.assertEqual(field.type, fcopy.type) 
        self.assertEqual(field.key, fcopy.key) 

        # Use copy.copy
        fcopy = copy.copy(field)
        self.assertTrue(field is not fcopy)
        self.assertEqual(field.name, fcopy.name) 
        self.assertEqual(field.type, fcopy.type) 
        self.assertEqual(field.key, fcopy.key) 

        # Use copy.deepcopy
        fcopy = copy.deepcopy(field)
        self.assertTrue(field is not fcopy)
        self.assertEqual(field.name, fcopy.name) 
        self.assertEqual(field.type, fcopy.type) 
        self.assertEqual(field.key, fcopy.key) 

    def test_from_xml(self):
        field = SchemaField.from_xml('<field name="foo" type="int64" />')
        self.assertEqual(field.name, 'foo')
        self.assertEqual(field.type, 'int64')
        self.assertEqual(field.key, False)

        field = SchemaField.from_xml('<field name="foo" type="int64" key="true" />')
        self.assertEqual(field.name, 'foo')
        self.assertEqual(field.type, 'int64')
        self.assertEqual(field.key, True)

        field = SchemaField.from_xml(xml.new_elem('field', attrib=dict(name='foo',
                                                                       type='int32',
                                                                       key=True)))
        self.assertEqual(field.name, 'foo')
        self.assertEqual(field.type, 'int32')
        self.assertEqual(field.key, True)

    def test_to_element(self):
        field = SchemaField.from_xml('<field name="foo" type="int64" />')
        field = field.to_element()
        self.assertEqual(field.tag, 'field')
        self.assertEqual(field.attrib, {'type': 'int64', 'name': 'foo', 'key': 'false'})

    def test_to_xml(self):
        field = SchemaField.from_xml('<field name="foo" type="int64" />')
        field = field.to_xml()
        self.assertEqual(field, '<field key="false" name="foo" type="int64" />')

    def test_str(self):
        field = SchemaField('foo', 'int64', key='true')
        self.assertEqual(str(field), 'foo*:int64')

        field = SchemaField('foo', 'int64', key='false')
        self.assertEqual(str(field), 'foo:int64')

    def test_repr(self):
        field = SchemaField('foo', 'int64', key='true')
        self.assertEqual(repr(field), 'foo*:int64')

        field = SchemaField('foo', 'int64', key='false')
        self.assertEqual(repr(field), 'foo:int64')


class TestSchema(tm.TestCase):

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

    def test_init(self):
        sch = Schema(SchemaField('a', 'int64'),
                     SchemaField('b', 'double', key=True),
                     SchemaField('c', 'int32'))
        self.assertEqual(list(sch.keys()), ['a', 'b', 'c'])
        self.assertEqual([x.type for x in sch.values()],
                         ['int64', 'double', 'int32']) 
        self.assertEqual([x.key for x in sch.values()],
                         [False, True, False])

    def test_copy(self):
        win = self.project['contquery']['w_data']

        # Use the copy method
        scopy = win.schema.copy()
        self.assertEqual(list(scopy.keys()), ['id', 'x_c', 'y_c'])
        self.assertEqual([x.type for x in scopy.values()],
                         ['int64', 'double', 'double'])
        self.assertEqual([x.key for x in scopy.values()],
                         [True, False, False])
        self.assertTrue(scopy['id'] is win.schema['id'])
        self.assertTrue(scopy['x_c'] is win.schema['x_c'])
        self.assertTrue(scopy['y_c'] is win.schema['y_c'])

        # Use the copy method with deep=True
        scopy = win.schema.copy(deep=True)
        self.assertEqual(list(scopy.keys()), ['id', 'x_c', 'y_c'])
        self.assertEqual([x.type for x in scopy.values()],
                         ['int64', 'double', 'double'])
        self.assertEqual([x.key for x in scopy.values()],
                         [True, False, False])
        self.assertTrue(scopy['id'] is not win.schema['id'])
        self.assertTrue(scopy['x_c'] is not win.schema['x_c'])
        self.assertTrue(scopy['y_c'] is not win.schema['y_c'])

        # Use copy.copy
        scopy = copy.copy(win.schema)
        self.assertEqual(list(scopy.keys()), ['id', 'x_c', 'y_c'])
        self.assertEqual([x.type for x in scopy.values()],
                         ['int64', 'double', 'double'])
        self.assertEqual([x.key for x in scopy.values()],
                         [True, False, False])
        self.assertTrue(scopy['id'] is win.schema['id'])
        self.assertTrue(scopy['x_c'] is win.schema['x_c'])
        self.assertTrue(scopy['y_c'] is win.schema['y_c'])

        # Use copy.deepcopy
        scopy = copy.deepcopy(win.schema)
        self.assertEqual(list(scopy.keys()), ['id', 'x_c', 'y_c'])
        self.assertEqual([x.type for x in scopy.values()],
                         ['int64', 'double', 'double'])
        self.assertEqual([x.key for x in scopy.values()],
                         [True, False, False])
        self.assertTrue(scopy['id'] is not win.schema['id'])
        self.assertTrue(scopy['x_c'] is not win.schema['x_c'])
        self.assertTrue(scopy['y_c'] is not win.schema['y_c'])

    def test_schema_string(self):
        sch = Schema(SchemaField('foo', 'int64'),
                     SchemaField('bar', 'double', key=True),
                     SchemaField('baz', 'int32'))
        
        self.assertEqual(len(sch), 3)
        self.assertEqual(sch.schema_string, 'foo:int64,bar*:double,baz:int32')

        sch.schema_string = 'one*:int64,two:int64,three:double,four:int64'

        self.assertEqual(sch.schema_string, 
                         'one*:int64,two:int64,three:double,four:int64')
        self.assertEqual(len(sch), 4)
        self.assertEqual(sch.fields['one'].name, 'one')
        self.assertEqual(sch.fields['one'].type, 'int64')
        self.assertEqual(sch.fields['one'].key, True)
        self.assertEqual(sch.fields['two'].name, 'two')
        self.assertEqual(sch.fields['two'].type, 'int64')
        self.assertEqual(sch.fields['two'].key, False)
        self.assertEqual(sch.fields['three'].name, 'three')
        self.assertEqual(sch.fields['three'].type, 'double')
        self.assertEqual(sch.fields['three'].key, False)
        self.assertEqual(sch.fields['four'].name, 'four')
        self.assertEqual(sch.fields['four'].type, 'int64')
        self.assertEqual(sch.fields['four'].key, False)

        sch.schema_string = 'one*:int64,two'

        self.assertEqual(len(sch), 2)
        self.assertEqual(sch.fields['one'].name, 'one')
        self.assertEqual(sch.fields['one'].type, 'int64')
        self.assertEqual(sch.fields['one'].key, True)
        self.assertEqual(sch.fields['two'].name, 'two')
        self.assertEqual(sch.fields['two'].type, 'inherit')
        self.assertEqual(sch.fields['two'].key, False)

    def test_from_xml(self):
        sch = Schema.from_xml('<schema><fields>'
                              '<field name="foo" type="int64" key="true"/>'
                              '<field name="bar" type="int32"/>'
                              '<field name="baz" type="int32"/>'
                              '</fields></schema>')

        self.assertEqual(len(sch), 3)
        self.assertEqual(sch.fields['foo'].name, 'foo')
        self.assertEqual(sch.fields['foo'].type, 'int64')
        self.assertEqual(sch.fields['foo'].key, True)
        self.assertEqual(sch.fields['bar'].name, 'bar')
        self.assertEqual(sch.fields['bar'].type, 'int32')
        self.assertEqual(sch.fields['bar'].key, False)
        self.assertEqual(sch.fields['baz'].name, 'baz')
        self.assertEqual(sch.fields['baz'].type, 'int32')
        self.assertEqual(sch.fields['baz'].key, False)

    def test_to_element(self):
        sch = Schema.from_xml('<schema><fields>'
                              '<field name="foo" type="int64" key="true"/>'
                              '<field name="bar" type="int32"/>'
                              '<field name="baz" type="int32"/>'
                              '</fields></schema>')
        elem = sch.to_element()
        self.assertEqual(elem.tag, 'schema')
        for fields in elem:
            for i, field in enumerate(fields):
                if i == 0:
                    self.assertEqual(field.tag, 'field')
                    self.assertEqual(field.attrib, dict(name='foo', type='int64', key='true'))
                elif i == 1:
                    self.assertEqual(field.tag, 'field')
                    self.assertEqual(field.attrib, dict(name='bar', type='int32', key='false'))
                elif i == 2:
                    self.assertEqual(field.tag, 'field')
                    self.assertEqual(field.attrib, dict(name='baz', type='int32', key='false'))
                else:
                    raise ValueError('Too many items')

    def test_to_xml(self):
        sch = Schema.from_xml('<schema><fields>' + \
                              '<field name="foo" type="int64" key="true" />' + \
                              '<field name="bar" type="int32" key="false" />' + \
                              '<field name="baz" type="int32" key="false" />' + \
                              '</fields></schema>')
        self.assertEqual(sch.to_xml(), '<schema><fields>'
                                       '<field key="true" name="foo" type="int64" />'
                                       '<field key="false" name="bar" type="int32" />'
                                       '<field key="false" name="baz" type="int32" />'
                                       '</fields></schema>')

    def test_setitem(self):
        sch = Schema()
        sch['foo'] = 'double', True
        sch['bar'] = 'int32'
        sch['baz*'] = 'int64'
        self.assertEqual(sch.schema_string, 'foo*:double,bar:int32,baz*:int64')
        self.assertEqual(list(sch.keys()), ['foo', 'bar', 'baz'])
        self.assertEqual([x.name for x in sch.values()], ['foo', 'bar', 'baz'])
        self.assertEqual([x.type for x in sch.values()], ['double', 'int32', 'int64'])
        self.assertEqual([x.key for x in sch.values()], [True, False, True])

    def test_update_field(self):
        sch = Schema()
        sch['foo'] = 'double', True
        sch['bar'] = 'int32'
        self.assertEqual(sch.schema_string, 'foo*:double,bar:int32')

        sch['foo'].type = 'int64'
        self.assertEqual(sch.schema_string, 'foo*:int64,bar:int32')

        sch['bar'].key = True
        self.assertEqual(sch.schema_string, 'foo*:int64,bar*:int32')

    def test_delitem(self):
        sch = Schema()
        sch['foo'] = 'double', True
        sch['bar'] = 'int32'

        self.assertEqual(list(sch.fields.keys()), ['foo', 'bar'])

        del sch['foo']

        self.assertEqual(list(sch.fields.keys()), ['bar'])

    def test_str(self):
        sch = Schema()
        sch.schema_string = 'foo*:double,bar:int64'
        self.assertEqual(str(sch), 'foo*:double,bar:int64')

    def test_repr(self):
        sch = Schema()
        sch.schema_string = 'foo*:double,bar:int64'
        self.assertEqual(repr(sch), 'foo*:double,bar:int64')

    def test_mapping_methods(self):
        sch = Schema()
        sch.schema_string = 'foo*:double,bar:int64,baz:int32'
        
        self.assertEqual(sch['foo'].name, 'foo')
        self.assertEqual(sch['foo'].type, 'double')
        self.assertEqual(sch['foo'].key, True)

        sch['foo'] = SchemaField('foo', 'int64', key=False)
        self.assertEqual(sch['foo'].name, 'foo')
        self.assertEqual(sch['foo'].type, 'int64')
        self.assertEqual(sch['foo'].key, False)

        self.assertEqual(sch.schema_string, 'foo:int64,bar:int64,baz:int32')
        del sch['foo']
        self.assertEqual(sch.schema_string, 'bar:int64,baz:int32')

        self.assertEqual(len(list(sch)), 2)
        for i, item in enumerate(sch):
            if i == 0:
                self.assertEqual(item, 'bar')
            elif i == 1:
                self.assertEqual(item, 'baz')

        self.assertEqual(len(sch), 2) 
 

if __name__ == '__main__':
   tm.runtests()
