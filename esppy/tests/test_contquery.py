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
from esppy.config import ESP_ROOT
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestContinuousQuery(tm.TestCase):

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

    def test_metadata(self):
        metadata = dict(self.query.metadata.items())

        self.query.metadata.clear()

        self.assertEqual(set(self.query.metadata.keys()), set())

        # Set keys
        self.query.metadata['foo'] = 'bar'
        self.query.metadata['one'] = 2

        self.assertEqual(self.query.metadata, {'foo': 'bar', 'one': '2'})
        self.assertEqual(self.query.metadata['foo'], 'bar')
        self.assertEqual(self.query.metadata['one'], '2')

        with self.assertRaises(KeyError):
            self.query.metadata['none']

        # Update keys
        self.query.metadata['foo'] = 'baz'

        self.assertEqual(self.query.metadata, {'foo': 'baz', 'one': '2'})

        # Delete keys
        del self.query.metadata['foo']

        self.assertEqual(self.query.metadata, {'one': '2'})
        self.assertEqual(self.query.metadata['one'], '2')

        with self.assertRaises(KeyError):
            self.query.metadata['foo']

        del self.query.metadata['one']

        self.assertEqual(self.query.metadata, {})
        self.assertEqual(list(self.query.metadata.keys()), [])
        self.assertEqual(list(self.query.metadata.values()), [])

        # Update any original metadata
        self.query.metadata.update(metadata)

        self.assertEqual(self.query.metadata, metadata)

    def test_copy(self):
        # Shallow copy
        cpy = self.query.copy()

        self.assertTrue(cpy is not self.query)
        self.assertEqual(cpy.name, self.query.name)
        self.assertEqual(cpy.project, self.query.project)
        self.assertEqual(set(cpy.windows.keys()), set(self.query.windows.keys()))

        for key in self.query.windows.keys():
            self.assertTrue(cpy.windows[key] is self.query.windows[key]) 

        cpy = copy.copy(self.query)

        self.assertTrue(cpy is not self.query)
        self.assertEqual(cpy.name, self.query.name)
        self.assertEqual(cpy.project, self.query.project)
        self.assertEqual(set(cpy.windows.keys()), set(self.query.windows.keys()))

        for key in self.query.windows.keys():
            self.assertTrue(cpy.windows[key] is self.query.windows[key])

        # Deep copy
        cpy = self.query.copy(deep=True)

        self.assertTrue(cpy is not self.query)
        self.assertEqual(cpy.name, self.query.name)
        self.assertEqual(cpy.project, self.query.project)
        self.assertEqual(set(cpy.windows.keys()), set(self.query.windows.keys()))

        for key in self.query.windows.keys():
            self.assertTrue(cpy.windows[key] is not self.query.windows[key])

        cpy = copy.deepcopy(self.query)

        self.assertTrue(cpy is not self.query)
        self.assertEqual(cpy.name, self.query.name)
        self.assertEqual(cpy.project, self.query.project)
        self.assertEqual(set(cpy.windows.keys()), set(self.query.windows.keys()))

        for key in self.query.windows.keys():
            self.assertTrue(cpy.windows[key] is not self.query.windows[key])

    def test_fullname(self):
        self.assertEqual(self.query.fullname, 'ESPUnitTestProjectSA.contquery')

    def test_metadata(self):
        metadata = dict(self.query.metadata.items())

        self.query.metadata.clear()

        self.assertEqual(set(self.query.metadata.keys()), set())

        # Set keys
        self.query.metadata['foo'] = 'bar'
        self.query.metadata['one'] = 2

        self.assertEqual(self.query.metadata, {'foo': 'bar', 'one': 2})
        self.assertEqual(self.query.metadata['foo'], 'bar')
        self.assertEqual(self.query.metadata['one'], 2)

        with self.assertRaises(KeyError):
            self.query.metadata['none']

        # Update keys
        self.query.metadata['foo'] = 'baz'

        self.assertEqual(self.query.metadata, {'foo': 'baz', 'one': 2})

        # Delete keys
        del self.query.metadata['foo']

        self.assertEqual(self.query.metadata, {'one': 2})
        self.assertEqual(self.query.metadata['one'], 2)

        with self.assertRaises(KeyError):
            self.query.metadata['foo']

        del self.query.metadata['one']

        self.assertEqual(self.query.metadata, {})
        self.assertEqual(list(self.query.metadata.keys()), [])
        self.assertEqual(list(self.query.metadata.values()), [])

        # Update any original metadata
        self.query.metadata.update(metadata)

        self.assertEqual(self.query.metadata, metadata)

    def test_url(self):
        self.assertRegex(self.query.url, r'^https?://.+?/%s/%s/%s/$' %
                                         (ESP_ROOT, self.query.project, self.query.name))

        # Set project to None
        self.query.project = None

        with self.assertRaises(ValueError):
            self.query.url

    def test_to_graph(self):
        out = self.query.to_graph()
        # TODO: Not sure how to verify; just make sure it runs

    def test_repr_svg(self):
        out = self.query._repr_svg_()
        # TODO: Not sure how to verify; just make sure it runs

    def test_str(self):
        qstr = str(self.query)
        self.assertEqual(qstr, "ContinuousQuery(name='%s', project='%s')" % 
                               (self.query.name, self.query.project))

    def test_repr(self):
        qstr = repr(self.query)
        self.assertEqual(qstr, "ContinuousQuery(name='%s', project='%s')" % 
                               (self.query.name, self.query.project))

    def test_from_xml(self):
        xml = self.query.to_xml()
        newquery = self.query.from_xml(xml)

        self.assertEqual(self.query.name, newquery.name)
        self.assertEqual(sorted(self.query.windows.keys()),
                         sorted(newquery.windows.keys()))

    def test_rename_window(self):
        self.assertEqual([x.name for x in self.query['w_data'].targets],
                         ['w_calculate'])
        self.assertEqual([x.name for x in self.query['w_request'].targets],
                         ['w_calculate'])

        self.query.rename_window('w_calculate', 'summary')

        self.assertIn('summary', self.query.windows)
        self.assertNotIn('w_calculate', self.query.windows)

        self.assertEqual([x.name for x in self.query['w_data'].targets],
                         ['summary'])
        self.assertEqual([x.name for x in self.query['w_request'].targets],
                         ['summary'])

    def test_delete_windows(self):
        self.assertEqual(set(self.query.windows.keys()),
                         set(['w_calculate', 'w_data', 'w_request']))

        self.query.delete_windows('w_data', 'w_request')

        self.assertEqual(set(self.query.windows.keys()),
                         set(['w_calculate']))

        self.query.delete_window('w_calculate')

        self.assertEqual(set(self.query.windows.keys()), set())

        with self.assertRaises(KeyError):
            self.query.delete_window('foo')

    def test_add_window(self):
        window = self.s.SourceWindow('new-window')
        self.query.add_window(window)
        self.assertIn('new-window', self.query.windows)
        self.assertEqual(window.name, 'new-window')
        self.assertEqual(window.contquery, self.query.name)
        self.assertEqual(window.project, self.query.project)
        self.assertTrue(window.session is self.query.session)

    def test_get_windows(self):
        # Get all windows
        out = self.query.get_windows()
        self.assertEqual(len(out), 3)
        self.assertEqual(list(sorted(out.keys())), ['contquery.w_calculate',
                                                    'contquery.w_data',
                                                    'contquery.w_request'])

        # Subset names
        out = self.query.get_windows('w_data|w_request')
        self.assertEqual(len(out), 2)
        self.assertEqual(list(sorted(out.keys())), ['contquery.w_data',
                                                    'contquery.w_request'])

        # Non-existent names
        out = self.query.get_windows('foo')
        self.assertEqual(len(out), 0)

        # Filters
        out = self.query.get_windows(filter='match(name,"data|request")')
        self.assertEqual(len(out), 2)
        self.assertEqual(list(sorted(out.keys())), ['contquery.w_data',
                                                    'contquery.w_request'])

    def test_get_window(self):
        # Get specific window
        out = self.query.get_window('w_data')
        self.assertEqual(out.name, 'w_data')
        self.assertTrue(len(out.schema.fields) > 0)

        # Non-existent name
        with self.assertRaises(KeyError):
            self.query.get_window('foo')

    def test_mapping_methods(self):
        from ..windows import SourceWindow

        # __getitem__
        self.assertEqual(self.query['w_data'], self.query.windows['w_data'])
        
        # __setitem__
        window = SourceWindow(name='new_window')
        self.query['new_name'] = window
        self.assertIn('new_name', self.query)
        self.assertEqual(window.name, 'new_name')
        self.assertEqual(window.contquery, self.query.name)
        self.assertEqual(window.project, self.query.project)
        self.assertTrue(window.session, self.query.session)
 
        self.query['w_calculate'].add_target('new_name')
        self.assertEqual([x.name for x in self.query['w_calculate'].targets],
                         ['new_name'])

        # __delitem__
        del self.query['new_name']
        self.assertNotIn('new_name', self.query.windows)
        self.assertNotIn('new_name', self.query)
        self.assertEqual([x.name for x in self.query['w_calculate'].targets], [])

    def test_len(self):
        self.assertEqual(len(self.query.windows), 3)
        self.assertEqual(len(self.query), 3)
        
        del self.query['w_data']

        self.assertEqual(len(self.query.windows), 2)
        self.assertEqual(len(self.query), 2)

    def test_windows_str(self):
        wstr = str(self.query.windows)
        self.assertIn("'w_request': SourceWindow(name='w_request', contquery='contquery', project='ESPUnitTestProjectSA')", wstr)
        self.assertIn("'w_data': SourceWindow(name='w_data', contquery='contquery', project='ESPUnitTestProjectSA')", wstr)
        self.assertIn("'w_calculate': CalculateWindow(name='w_calculate', contquery='contquery', project='ESPUnitTestProjectSA'", wstr)

    def test_windows_repr(self):
        wstr = repr(self.query.windows)
        self.assertIn("'w_request': SourceWindow(name='w_request', contquery='contquery', project='ESPUnitTestProjectSA')", wstr)
        self.assertIn("'w_data': SourceWindow(name='w_data', contquery='contquery', project='ESPUnitTestProjectSA')", wstr)
        self.assertIn("'w_calculate': CalculateWindow(name='w_calculate', contquery='contquery', project='ESPUnitTestProjectSA'", wstr)


if __name__ == '__main__':
   tm.runtests()
