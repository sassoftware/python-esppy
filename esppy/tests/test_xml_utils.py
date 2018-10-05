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
from esppy.utils.xml import (new_elem, add_elem, add_properties, from_xml,
                             to_xml, xml_indent, get_attrs)
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestXMLUtils(tm.TestCase):

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

    def test_new_elem(self):
        # empty
        elem = new_elem('foo')
        self.assertEqual(to_xml(elem), '<foo />')

        # With attributes
        elem = new_elem('foo', attrib=dict(a='100', b='200'))
        self.assertEqual(to_xml(elem), '<foo a="100" b="200" />')

        # With text content
        elem = new_elem('foo', attrib=dict(a='100', b='200'), text_content='Hi')
        self.assertEqual(to_xml(elem), '<foo a="100" b="200">Hi</foo>')

        # With keyword parameters
        elem = new_elem('foo', attrib=dict(a='100', b='200'), other='value')
        self.assertEqual(to_xml(elem), '<foo a="100" b="200" other="value" />')

    def test_add_elem(self):
        # tag name
        parent = new_elem('foo')
        add_elem(parent, 'bar', a='100', b='200', attrib=dict(c="300"))
        self.assertEqual(to_xml(parent), '<foo><bar a="100" b="200" c="300" /></foo>')

        # element
        parent = new_elem('foo')
        add_elem(parent, new_elem('bar', a='100', b='200'), attrib=dict(c="300"), other='value')
        self.assertEqual(to_xml(parent), '<foo><bar a="100" b="200" c="300" other="value" /></foo>')

        # XML
        parent = new_elem('foo')
        add_elem(parent, '<bar b="200" a="100" />', attrib=dict(c="300"), other='value')
        self.assertEqual(to_xml(parent), '<foo><bar a="100" b="200" c="300" other="value" /></foo>')

    def test_xml_indent(self):
        a = new_elem('a', x='100', y='200')
        b = new_elem('b', m='1000', n='2000')
        c = new_elem('c', text_content='Hi')
        add_elem(a, b)
        add_elem(b, c)
        xml_indent(a)
        self.assertEqual(to_xml(a), '<a x="100" y="200">\n  <b m="1000" n="2000">\n    <c>Hi</c>\n  </b>\n</a>\n')

    def test_to_xml(self):
        a = new_elem('a', x='100', y='200')
        b = new_elem('b', m='1000', n='2000')
        c = new_elem('c', text_content='Hi')
        add_elem(a, b)
        add_elem(b, c)
        self.assertEqual(to_xml(a), '<a x="100" y="200"><b m="1000" n="2000"><c>Hi</c></b></a>')

        a = new_elem('a', x='100', y='200')
        b = new_elem('b', m='1000', n='2000')
        c = new_elem('c', text_content='Hi')
        add_elem(a, b)
        add_elem(b, c)
        self.assertEqual(to_xml(a, pretty=True), '<a x="100" y="200">\n  <b m="1000" n="2000">\n    <c>Hi</c>\n  </b>\n</a>\n')

        a = new_elem('a', x='100', y='200')
        b = new_elem('b', m='1000', n='2000')
        c = new_elem('c', text_content='Hi')
        add_elem(a, b)
        add_elem(b, c)
        self.assertEqual(to_xml(to_xml(a), pretty=True), '<a x="100" y="200">\n  <b m="1000" n="2000">\n    <c>Hi</c>\n  </b>\n</a>\n')

        a = new_elem('a', x='100', y='200')
        b = new_elem('b', m='1000', n='2000')
        c = new_elem('c', text_content='Hi')
        add_elem(a, b)
        add_elem(b, c)
        print(to_xml(a, encoding='ascii'))
        self.assertEqual(to_xml(a, encoding='ascii'), b'<?xml version=\'1.0\' encoding=\'ascii\'?>\n<a x="100" y="200"><b m="1000" n="2000"><c>Hi</c></b></a>')

    @unittest.skip('Test is obsolete; needs to be rewritten for "xml_map" and "attribute"s')
    def test_get_attrs(self):
        project = self.project

        class XMLAttrObject1(object):
            attr_map = ['a', 'b', 'c']
            def __init__(self):
                self.a = 1
                self.b = 2
                self.c = 3
                self.d = 4
                self.e = 5

        class XMLAttrObject2(object):
            attr_map = {'a': 'attr1', 'b': 'attr2', 'c': None}
            def __init__(self):
                self.a = 100
                self.b = 200
                self.c = 300

        class XMLAttrObject3(object):
            attr_map = 10

        class XMLAttrObject4(object):
            def __init__(self):
                self.a = 1000
                self.b = 2000
                self.c = 3000
                self.d = None 
                self.e = [1, 2, 3]
                self.f = {'key': 'value'}
                self.g = True
                self.h = False
                self._i = 10
                self.cls = 20
                self.my_project = project

        x1 = XMLAttrObject1()
        x2 = XMLAttrObject2()
        x3 = XMLAttrObject3()
        x4 = XMLAttrObject4()

        out = get_attrs(x1)
        self.assertEqual(out, {'a': '1', 'b': '2', 'c': '3'})

        out = get_attrs(x1, exclude='a')
        self.assertEqual(out, {'b': '2', 'c': '3'})

        out = get_attrs(x1, extra='d')
        self.assertEqual(out, {'a': '1', 'b': '2', 'c': '3', 'd': '4'})

        out = get_attrs(x1, extra=['d', 'e'])
        self.assertEqual(out, {'a': '1', 'b': '2', 'c': '3', 'd': '4', 'e': '5'})

        out = get_attrs(x2)
        self.assertEqual(out, {'attr1': '100', 'attr2': '200', 'c': '300'})

        with self.assertRaises(TypeError):
            get_attrs(x3)

        out = get_attrs(x4)
        self.assertEqual(out, {'a': '1000', 'b': '2000', 'c': '3000', 'g': 'true',
                               'h': 'false', 'class': '20',
                               'my-project': 'ESPUnitTestProjectSA'})

        out = get_attrs(x4, exclude=['c', 'd', 'e', 'f', 'g', 'h'])
        self.assertEqual(out, {'a': '1000', 'b': '2000', 'class': '20',
                               'my-project': 'ESPUnitTestProjectSA'})

if __name__ == '__main__':
   tm.runtests()
