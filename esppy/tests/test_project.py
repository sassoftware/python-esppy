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
import esppy.utils.xml as xml
import os
import pandas as pd
import shutil
import six
import sys
import tempfile
import time
import unittest
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestProject(tm.TestCase):

    def setUp(self):
        self.s = esppy.ESP(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.project = self.s.install_project(model_xml_path)

    def tearDown(self):
        self.s.delete_projects(filter="match(name, '^.*UnitTest.*$')")

    def test_metadata(self):
        metadata = dict(self.project.metadata.items())

        self.project.metadata.clear()

        self.assertEqual(set(self.project.metadata.keys()), set())

        # Set keys
        self.project.metadata['foo'] = 'bar'
        self.project.metadata['one'] = '2'

        self.assertEqual(self.project.metadata, {'foo': 'bar', 'one': '2'})
        self.assertEqual(self.project.metadata['foo'], 'bar')
        self.assertEqual(self.project.metadata['one'], '2')

        with self.assertRaises(KeyError):
            self.project.metadata['none']

        # Update keys
        self.project.metadata['foo'] = 'baz'

        self.assertEqual(self.project.metadata, {'foo': 'baz', 'one': '2'})

        # Delete keys
        del self.project.metadata['foo']

        self.assertEqual(self.project.metadata, {'one': '2'})
        self.assertEqual(self.project.metadata['one'], '2')

        with self.assertRaises(KeyError):
            self.project.metadata['foo']

        del self.project.metadata['one']

        self.assertEqual(self.project.metadata, {})
        self.assertEqual(list(self.project.metadata.keys()), [])
        self.assertEqual(list(self.project.metadata.values()), [])

        # Update any original metadata
        self.project.metadata.update(metadata)

        self.assertEqual(self.project.metadata, metadata)

    def test_queries_str(self):
        self.assertEqual(str(self.project.queries),
                        "{'contquery': ContinuousQuery(name='contquery', project='ESPUnitTestProjectSA')}")

    def test_queries_repr(self):
        self.assertEqual(repr(self.project.queries),
                        "{'contquery': ContinuousQuery(name='contquery', project='ESPUnitTestProjectSA')}")

    def test_queries_len(self):
        self.assertEqual(len(self.project.queries), 1)

    def test_queries_iter(self):
        for item in self.project.queries:
            self.assertEqual(item, 'contquery')

    def test_get_windows(self):
        # Get all windows
        out = self.project.get_windows()
        self.assertEqual(len(out), 3)
        self.assertTrue('w_calculate' in out)
        self.assertTrue('w_data' in out)
        self.assertTrue('w_request' in out)

        # Subset names
        out = self.project.get_windows('w_data|w_request')
        self.assertEqual(len(out), 2)
        self.assertTrue('w_data' in out)
        self.assertTrue('w_request' in out)

        # Non-existent names
        out = self.project.get_windows('foo')
        self.assertEqual(len(out), 0)

        # Filters
        out = self.project.get_windows(filter='match(name, "data|request")')
        self.assertEqual(len(out), 2)
        self.assertTrue('w_data' in out)
        self.assertTrue('w_request' in out)

    def test_get_window(self):
        # Get specific window
        out = self.project.get_window('contquery.w_data')
        self.assertEqual(out.name, 'w_data')
        self.assertTrue(len(out.schema.fields) > 0)

        # Non-existent name
        with self.assertRaises(KeyError):
            self.project.get_window('contquery.foo')

    def test_name(self):
        self.assertEqual(self.project.name, 'ESPUnitTestProjectSA')

        for key, value in self.project.queries.items():
            self.assertEqual(value.project, 'ESPUnitTestProjectSA')
            for wkey, wvalue in value.windows.items():
                self.assertEqual(wvalue.project, 'ESPUnitTestProjectSA')

        self.project.name = 'Foo'

        self.assertEqual(self.project.name, 'Foo')

        for key, value in self.project.queries.items():
            self.assertEqual(value.project, 'Foo')
            for wkey, wvalue in value.windows.items():
                self.assertEqual(wvalue.project, 'Foo')

    def test_copy(self):
        # Use copy method directly
        proj = self.project.copy()
        self.assertEqual(proj.name, self.project.name)
        self.assertEqual(len(proj.queries), 1)
        self.assertEqual(sorted(proj.queries.keys()), ['contquery'])
        self.assertTrue(proj.queries['contquery'] is self.project.queries['contquery'])
        
        # Use copy.copy
        proj = copy.copy(self.project)
        self.assertEqual(proj.name, self.project.name)
        self.assertEqual(len(proj.queries), 1)
        self.assertEqual(sorted(proj.queries.keys()), ['contquery'])
        self.assertTrue(proj.queries['contquery'] is self.project.queries['contquery'])
        
        # Use copy(deep=True) method directly
        proj = self.project.copy(deep=True)
        self.assertEqual(proj.name, self.project.name)
        self.assertEqual(len(proj.queries), 1)
        self.assertEqual(sorted(proj.queries.keys()), ['contquery'])
        self.assertTrue(proj.queries['contquery'] is not self.project.queries['contquery'])
        self.assertEqual(sorted(proj.queries['contquery'].windows.keys()),
                         ['w_calculate', 'w_data', 'w_request'])
        self.assertTrue(proj['contquery']['w_calculate'] is not
                        self.project['contquery']['w_calculate'])
        self.assertTrue(proj['contquery']['w_data'] is not
                        self.project['contquery']['w_data'])
        self.assertTrue(proj['contquery']['w_request'] is not
                        self.project['contquery']['w_request'])

        # Use copy.deepcopy 
        proj = copy.deepcopy(self.project)
        self.assertEqual(proj.name, self.project.name)
        self.assertEqual(len(proj.queries), 1)
        self.assertEqual(sorted(proj.queries.keys()), ['contquery'])
        self.assertTrue(proj.queries['contquery'] is not self.project.queries['contquery'])
        self.assertEqual(sorted(proj.queries['contquery'].windows.keys()),
                         ['w_calculate', 'w_data', 'w_request'])
        self.assertTrue(proj['contquery']['w_calculate'] is not
                        self.project['contquery']['w_calculate'])
        self.assertTrue(proj['contquery']['w_data'] is not
                        self.project['contquery']['w_data'])
        self.assertTrue(proj['contquery']['w_request'] is not
                        self.project['contquery']['w_request'])

    def test_fullname(self):
        self.assertEqual(self.project.fullname, 'ESPUnitTestProjectSA')
        self.project.name = 'Foo'
        self.assertEqual(self.project.fullname, 'Foo')

    def test_from_xml(self):
        with self.assertRaises(ValueError):
             self.project.from_xml('<foo />')

    def test_to_element(self):
        before_xml = self.project.to_xml(pretty=True)
        after_xml = xml.to_xml(self.project.to_element(), pretty=True)
        self.assertEqual(before_xml, after_xml)

    def test_to_graph(self):
        # TODO: Not sure how to verify
        out = self.project.to_graph(schema=True)

    def test_repr_svg(self):
        # TODO: Not sure how to verify
        out = self.project._repr_svg_()

    def test_str(self):
        self.assertEqual(str(self.project), "Project(name='ESPUnitTestProjectSA')")

    def test_repr(self):
        self.assertEqual(repr(self.project), "Project(name='ESPUnitTestProjectSA')")

    def test_save(self):
        tmp = tempfile.mkdtemp()
        self.project.save(tmp)
        self.assertTrue(os.path.isdir(tmp))
        self.assertTrue(os.path.isfile(os.path.join(tmp, 'project.state')))
        self.assertTrue(os.path.isdir(os.path.join(tmp, 'queries')))
        shutil.rmtree(tmp, ignore_errors=True)

    def test_restore(self):
        tmp = tempfile.mkdtemp()
        self.project.save(tmp)
        self.project.stop()
        self.project.restore(tmp)
        self.assertTrue('ESPUnitTestProjectSA' in self.s.get_projects())
        shutil.rmtree(tmp, ignore_errors=True)

    @unittest.skip('Crashes server')
    def test_update(self):
        xml = self.project.to_xml()
        xml = xml.replace('name="w_data"', 'name="data_win"')
        xml = xml.replace('source="w_data"', 'source="data_win"')
        self.project.update(xml)

    def test_delete(self):
        self.assertIn('ESPUnitTestProjectSA', self.s.get_projects())
        self.project.delete()
        self.assertNotIn('ESPUnitTestProjectSA', self.s.get_projects())

    def test_create_mas_module(self):
        out = self.project.create_mas_module('python', 'mymod', ['func1', 'func2'])
        self.assertTrue(type(out).__name__, 'MASModule')

    def test_get_mas_modules(self):
        # TODO: Not sure what output should be
        out = self.project.get_mas_modules()

    @unittest.skip('Need a MAS module loaded')
    def test_get_mas_module(self):
        out = self.project.get_mas_modules(name)
        
    @unittest.skip('Need a MAS module loaded')
    def test_replace_mas_module(self):
        out = self.project.replace_mas_modules(name, module)

    def test_add_connectors(self):
        self.project.add_connectors('CG_1_sub',
                                    {'cq_01/tradesWindow/trades_sub': 'running'})
        self.project.add_connectors('CG_1_pub',
                                    {'cq_01/tradesWindow/trades_pub': 'finished'})

        elem = self.project.to_element()

        groups = {}
        for item in elem.findall('.//connector-groups/connector-group'):
            name = item.attrib['name']
            entries = {}
            for conn in item.findall('./connector-entry'):
                entries[conn.attrib['connector']] = conn.attrib['state'] 
            groups[name] = entries
        
        self.assertEqual(groups,
             {'CG_1_sub': {'cq_01/tradesWindow/trades_sub': 'running'},
              'CG_1_pub': {'cq_01/tradesWindow/trades_pub': 'finished'}})
        
    def test_get_stats(self):
        out = self.project.get_stats()
        self.assertTrue(isinstance(out.stats, pd.DataFrame))

    def test_validate(self):
        self.assertTrue(self.project.validate())

    def test_add_query(self):
        self.assertNotIn('new-query', self.project.queries)
        newq = self.project.add_query('new-query')
        self.assertIn('new-query', self.project.queries)

    def test_getitem(self):
        self.assertTrue(type(self.project['contquery']).__name__, 'ContinuousQuery')

        with self.assertRaises(KeyError):
            self.project['Foo']

    def test_setitem(self):
        with self.assertRaises(KeyError):
            self.project['new-query']

        newq = self.project.add_contquery('new-query')

        self.assertTrue(newq is self.project['new-query'])

        del self.project['new-query']

        with self.assertRaises(KeyError):
            self.project['new-query']

        self.project['new-query'] = newq

        self.assertTrue(newq is self.project['new-query'])

    def test_delitem(self):
        self.assertIn('contquery', self.project)
        del self.project['contquery']
        self.assertNotIn('contquery', self.project)

    def test_iter(self):
        for item in self.project:
            self.assertEqual(item, 'contquery')

    def test_len(self):
        self.assertEqual(len(self.project), 1)

    def test_contains(self):
        self.assertIn('contquery', self.project)
        self.assertNotIn('foo', self.project)

    def test_get_project_stats(self):
        ps = self.s.get_project_stats()
        self.assertEqual(len(ps), 0)


if __name__ == '__main__':
   tm.runtests()
