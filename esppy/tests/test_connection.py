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
from ..utils import xml
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestConnection(tm.TestCase):

    def setUp(self):
        self.s = esppy.ESP(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

    def tearDown(self):
        self.s.delete_projects(filter="match(name, '^.*UnitTest.*$')")
        try:
            self.s.delete_event_generator('MyUnitTestEventGenerator')
        except:
            pass

    def test_env_connection(self):
        os.environ['ESPHOST'] = HOST
        os.environ['ESPPORT'] = str(PORT)
        os.environ['ESPPPROTOCOL'] = PROTOCOL or 'http'
        os.environ['ESPUSER'] = USER or ''
        os.environ['ESPPASSWORD'] = PASSWD or ''
        conn = esppy.ESP()
        self.assertEqual(str(conn).replace("u'", "'"),
                         ("ESP('http://%s:%s')" % (HOST, PORT)).replace("u'", "'"))

    def test_url_connection(self):
        conn = esppy.ESP('%s://%s:%s' % (PROTOCOL or 'http', HOST, PORT))
        self.assertEqual(str(conn).replace("u'", "'"),
                         ("ESP('%s://%s:%s')" % (PROTOCOL or 'http', HOST, PORT)).replace("u'", "'"))

    def test_server_info(self):
        keys = set(self.s.server_info.keys())
        self.assertIn('analytics-license', keys)
        self.assertIn('engine', keys)
        #self.assertIn('plugindir', keys)
        self.assertIn('pubsub', keys)
        self.assertIn('version', keys)

    def test_metadata(self):
        metadata = dict(self.s.metadata.items())

        self.s.metadata.clear()

        self.assertEqual(set(self.s.metadata.keys()), set())

        # Set keys
        self.s.metadata['foo'] = 'bar'
        self.s.metadata['one'] = 2

        self.assertEqual(self.s.metadata, {'foo': 'bar', 'one': '2'})
        self.assertEqual(self.s.metadata['foo'], 'bar')
        self.assertEqual(self.s.metadata['one'], '2')

        with self.assertRaises(KeyError):
            self.s.metadata['none']

        # Update keys
        self.s.metadata['foo'] = 'baz'

        self.assertEqual(self.s.metadata, {'foo': 'baz', 'one': '2'})

        # Delete keys
        del self.s.metadata['foo']

        self.assertEqual(self.s.metadata, {'one': '2'})
        self.assertEqual(self.s.metadata['one'], '2')

        with self.assertRaises(KeyError):
            self.s.metadata['foo']

        del self.s.metadata['one']

        self.assertEqual(self.s.metadata, {})
        self.assertEqual(list(self.s.metadata.keys()), [])
        self.assertEqual(list(self.s.metadata.values()), [])

        # Update any original metadata
        self.s.metadata.update(metadata)

        self.assertEqual(self.s.metadata, metadata)

    def test_api_docs(self):
        keys = list(self.s.api_docs.keys())
        self.assertIn('basePath', keys)
        self.assertIn('info', keys)
        self.assertIn('schemes', keys)
        self.assertIn('swagger', keys)
        self.assertIn('tags', keys)
        self.assertIn('paths', keys)

    def test_install_project(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')

        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        # Create model from path
        self.s.install_project(model_xml_path)

        proj = self.s.get_project(name) 

        self.assertEqual(proj.name, name)
        self.assertIn(name, self.s.get_running_projects())
        self.assertNotIn(name, self.s.get_stopped_projects())

        # Test install with overwrite=False
        with self.assertRaises(esppy.ESPError):
            self.s.install_project(model_xml_path, overwrite=False)

        # Test install with name override
        newname = 'ESPUnitTestProject'

        proj = self.s.install_project(model_xml_path, name=newname)

        self.assertEqual(proj.name, newname)
        self.assertIn(name, self.s.get_running_projects())
        self.assertIn(newname, self.s.get_running_projects())
        self.assertNotIn(name, self.s.get_stopped_projects())
        self.assertNotIn(newname, self.s.get_stopped_projects())

        # Test install of Project object
        createname = 'ESPUnitTestCreate'

        proj = self.s.create_project(createname)

#       self.assertTrue(proj.validate())

# TODO: Project validates, but won't install
#       self.s.install_project(proj)

#       self.assertEqual(proj.name, createname)
#       self.assertIn(name, self.s.get_running_projects())
#       self.assertIn(newname, self.s.get_running_projects())
#       self.assertIn(createname, self.s.get_running_projects())
#       self.assertNotIn(name, self.s.get_stopped_projects())
#       self.assertNotIn(newname, self.s.get_stopped_projects())
#       self.assertNotIn(createname, self.s.get_stopped_projects())

        self.s.delete_project(name)

    def test_create_project(self):
        from esppy.windows import Target

        name = 'ESPUnitTestCreate'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        # Create empty project
        proj = self.s.create_project(name)

        #self.assertTrue(proj.validate())

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.assertEqual(list(proj.queries.keys()), [])

        # Create project from data
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        proj = self.s.create_project(model_xml_path)

        # TODO: Fails in the server for some unknown reason
        # self.assertTrue(proj.validate())

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        w_calculate = proj['contquery']['w_calculate']
        w_data = proj['contquery']['w_data']
        w_request = proj['contquery']['w_request']

        self.assertEqual(set(proj.queries.keys()),
                         set(['contquery']))
        self.assertEqual(set(proj['contquery'].keys()),
                         set(['w_calculate', 'w_data', 'w_request']))

        self.assertEqual(list(w_data.schema.fields.keys()),
                         ['id', 'x_c', 'y_c'])
        self.assertEqual([(x.name, x.role) for x in w_data.targets],
                         [('w_calculate', 'data')])
        self.assertEqual(len(w_data.connectors), 1)
        self.assertEqual(w_data.connectors[0].name, 'publisher')
        self.assertEqual(w_data.connectors[0].cls, 'fs')
        self.assertEqual(w_data.connectors[0].type, 'publish')
        self.assertEqual(set(w_data.connectors[0].properties.keys()),
                         set(['fstype', 'fsname', 'transactional', 'blocksize']))

        self.assertEqual(list(w_calculate.schema.fields.keys()),
                         ['y_c', 'x_c', 'id', 'min_x', 'max_x', 'alert'])
        # TODO: It seems like these should be ints, but the model file doesn't specify
        self.assertEqual(w_calculate.parameters,
                         dict(normalMin='2', normalMax='5'))
        self.assertEqual(w_calculate.input_map,
                         dict(input='x_c'))
        self.assertEqual(w_calculate.output_map,
                         dict(minOut='min_x', maxOut='max_x', alertOut='alert'))

        self.assertEqual(list(w_request.schema.fields.keys()),
                         ['req_id', 'req_key', 'req_val'])
        self.assertEqual([(x.name, x.role) for x in w_request.targets],
                         [('w_calculate', 'request')])

    def test_get_project(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.s.install_project(model_xml_path)

        # Non-existent name
        with self.assertRaises(KeyError):
            self.s.get_project('ThisProjectDoesNotExist')

        # Default settings
        out = self.s.get_project(name)
        self.assertEqual(out.name, name)
        self.assertEqual(len(out.queries), 1)
        self.assertEqual(len(out.queries['contquery'].windows), 3)
        self.assertEqual(len(out.queries['contquery'].windows['w_data'].schema.fields), 3)

    def test_get_projects(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.s.install_project(model_xml_path)

        # Default settings
        out = self.s.get_projects()
        self.assertIn(name, out)
        self.assertEqual(out[name].name, name)
        self.assertEqual(len(out[name].queries), 1)
        self.assertEqual(len(out[name].queries['contquery'].windows), 3)
        self.assertEqual(len(out[name].queries['contquery'].windows['w_data'].schema.fields), 3)

        # Specify name
        out = self.s.get_projects(name=[name])
        self.assertEqual(len(out), 1)
        self.assertIn(name, out)
        self.assertEqual(out[name].name, name)
        self.assertEqual(len(out[name].queries), 1)
        self.assertEqual(len(out[name].queries['contquery'].windows), 3)
        self.assertEqual(len(out[name].queries['contquery'].windows['w_data'].schema.fields), 3)

        # Specify non-existent name
        out = self.s.get_projects(name='ThisProjectDoesNotExist')
        self.assertEqual(len(out), 0)

        # Specify filter
        out = self.s.get_projects(filter=['match(name, ".*%s.*")' % name])
        self.assertEqual(len(out), 1)
        self.assertIn(name, out)
        self.assertEqual(out[name].name, name)
        self.assertEqual(len(out[name].queries), 1)
        self.assertEqual(len(out[name].queries['contquery'].windows), 3)
        self.assertEqual(len(out[name].queries['contquery'].windows['w_data'].schema.fields), 3)

        # Specify no-result filter
#       out = self.s.get_projects(filter='match(name, ".*ThisProjectDoesNotExist.*")')
#       self.assertEqual(len(out), 0)

    def test_get_running_projects(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.assertTrue(name not in self.s.get_running_projects().keys())

        proj = self.s.install_project(model_xml_path)

        self.assertTrue(name in self.s.get_running_projects().keys())

        proj.stop()

        self.assertTrue(name not in self.s.get_running_projects().keys())

    def test_get_running_project(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        with self.assertRaises(KeyError):
            self.s.get_running_project(name)

        self.s.install_project(model_xml_path)

        proj = self.s.get_running_project(name)

        self.assertEqual(proj.name, name)

    def test_start_projects(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        proj = self.s.install_project(model_xml_path)
        proj.stop() 

        self.assertTrue(name not in self.s.get_running_projects().keys())
        self.assertTrue(name in self.s.get_stopped_projects().keys())
         
        self.s.start_projects(filter='match(name,"ESPUnitTestProjectSA")')
        
        self.assertTrue(name in self.s.get_running_projects().keys())
        self.assertTrue(name not in self.s.get_stopped_projects().keys())

    def test_start_project(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        proj = self.s.install_project(model_xml_path)
        proj.stop()

        self.assertTrue(name not in self.s.get_running_projects().keys())
        self.assertTrue(name in self.s.get_stopped_projects().keys())

        self.s.start_project(name)

        self.assertTrue(name in self.s.get_running_projects().keys())
        self.assertTrue(name not in self.s.get_stopped_projects().keys())

    def test_stop_projects(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        proj = self.s.install_project(model_xml_path)

        self.assertTrue(name in self.s.get_running_projects().keys())
        self.assertTrue(name not in self.s.get_stopped_projects().keys())

        self.s.stop_projects(filter='match(name,"ESPUnitTestProjectSA")')

        self.assertTrue(name not in self.s.get_running_projects().keys())
        self.assertTrue(name in self.s.get_stopped_projects().keys())

    def test_stop_project(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        proj = self.s.install_project(model_xml_path)

        self.assertTrue(name in self.s.get_running_projects().keys())
        self.assertTrue(name not in self.s.get_stopped_projects().keys())

        self.s.stop_project(name)

        self.assertTrue(name not in self.s.get_running_projects().keys())
        self.assertTrue(name in self.s.get_stopped_projects().keys())

    def test_get_stopped_project(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        with self.assertRaises(KeyError):
            self.s.get_stopped_project(name)

        proj = self.s.install_project(model_xml_path)
        proj.stop()

        proj = self.s.get_stopped_project(name)
        self.assertEqual(proj.name, name)

    def test_get_windows(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        n_windows = len(self.s.get_windows())

        self.s.install_project(model_xml_path)

        # Get all windows
        out = self.s.get_windows()
        self.assertEqual(len(out), n_windows + 3)
        w_names = list(out.keys())
        self.assertIn('ESPUnitTestProjectSA.contquery.w_calculate', w_names)
        self.assertIn('ESPUnitTestProjectSA.contquery.w_data', w_names)
        self.assertIn('ESPUnitTestProjectSA.contquery.w_request', w_names)

        # Subset names
        out = self.s.get_windows('w_data|w_request')
        windows = [x for x in out.keys() if x.endswith('.w_data') or
                                            x.endswith('.w_request')]
        self.assertTrue(len(windows), len(out))
        keys = [x for x in out.keys() if x.startswith('ESPUnitTestProjectSA.')]
        self.assertEqual(len(keys), 2)
        self.assertEqual(list(sorted(keys)), ['ESPUnitTestProjectSA.contquery.w_data',
                                              'ESPUnitTestProjectSA.contquery.w_request'])

        # Non-existent names
        out = self.s.get_windows('foo')
        self.assertEqual(len(out), 0)

        # Filters
        out = self.s.get_windows(filter='match(name, "data|request")')
        windows = [x for x in out.keys() if x.endswith('.w_data') or
                                            x.endswith('.w_request')]
        self.assertTrue(len(windows), len(out))
        keys = [x for x in out.keys() if x.startswith('ESPUnitTestProjectSA.')]
        self.assertEqual(len(keys), 2)
        self.assertEqual(list(sorted(keys)), ['ESPUnitTestProjectSA.contquery.w_data',
                                              'ESPUnitTestProjectSA.contquery.w_request'])

    def test_get_window(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.s.install_project(model_xml_path)

        # Get specific window
        out = self.s.get_window('ESPUnitTestProjectSA.contquery.w_data')
        self.assertEqual(out.name, 'w_data')
        self.assertTrue(len(out.schema.fields) > 0)

        # Non-existent name
        with self.assertRaises(KeyError):
            self.s.get_window('ESPUnitTestProjectSA.contquery.foo')

    def test_str(self):
        out = str(self.s)
        self.assertEqual(out.replace("u'", "'"),
                         ("ESP('http://%s:%s')" % (HOST, PORT)).replace("u'", "'"))

    def test_repr(self):
        out = repr(self.s)
        self.assertEqual(out.replace("u'", "'"),
                         ("ESP('http://%s:%s')" % (HOST, PORT)).replace("u'", "'"))

    def test_get_loggers(self):
        loggers = self.s.get_loggers()
        self.assertIn('esp', loggers)
        self.assertIn('esp.http', loggers)
        self.assertIn('esp.server', loggers)
        self.assertIn('esp.windows', loggers)

    def test_get_logger(self):
        logger = self.s.get_logger('esp')
        self.assertTrue(type(logger).__name__, 'Logger')
        self.assertEqual(logger.name, 'esp')

        with self.assertRaises(KeyError):
            self.s.get_logger('non-existent-logger')

    def test_connector_info(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        self.s.install_project(model_xml_path)

        info = self.s.connector_info

        for key, value in info.items():
            self.assertEqual(type(value).__name__, 'ConnectorInfo')

    def test_validate_project(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        name = 'ESPUnitTestProjectSA'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        out = self.s.validate_project(model_xml_path)
        self.assertTrue(out is True)

        out = self.s.validate_project('<project><foo /></project>') 
        self.assertTrue(out is False)

    def test_get_event_generators(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        name = 'ESPUnitTestProjectSA'
        eg_name = 'MyUnitTestEventGenerator'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        proj = self.s.install_project(model_xml_path)

        self.assertNotIn(eg_name, self.s.get_event_generators())

        w_data = proj['contquery']['w_data']

        evtgen = self.s.create_event_generator(w_data, data='file://%s' % data_csv_path,
                                               name=eg_name)

        self.assertIn(eg_name, self.s.get_event_generators())
         
        self.s.delete_event_generator(eg_name)

    def test_get_event_generator_state(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        name = 'ESPUnitTestProjectSA'
        eg_name = 'MyUnitTestEventGenerator'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        proj = self.s.install_project(model_xml_path)

        self.assertNotIn(eg_name, self.s.get_event_generators())

        w_data = proj['contquery']['w_data']

        evtgen = self.s.create_event_generator(w_data, data='file://%s' % data_csv_path,
                                               name=eg_name)

        state = self.s.get_event_generator_state(eg_name)
        self.assertTrue(state)

        self.s.delete_event_generator(eg_name)

    def test_delete_event_generator(self):
        model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
        data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
        name = 'ESPUnitTestProjectSA'
        eg_name = 'MyUnitTestEventGenerator'

        with self.assertRaises(KeyError):
            self.s.get_project(name)

        proj = self.s.install_project(model_xml_path)

        self.assertNotIn(eg_name, self.s.get_event_generators())

        w_data = proj['contquery']['w_data']

        evtgen = self.s.create_event_generator(w_data, data='file://%s' % data_csv_path,
                                               name=eg_name)

        state = self.s.get_event_generator_state(eg_name)
        self.assertTrue(state)

        self.s.delete_event_generator(eg_name)

        self.assertNotIn(eg_name, self.s.get_event_generators())

    def test_ds_initializer(self):
        proj = self.s.create_project('project_01', pubsub='auto', n_threads='1',
                                     sas_command='@SAS_FOUNDATION_HOME@/sas -path @ESP_SASIO32_LIB@')
        out = proj.to_xml(pretty=True)
        self.assertTrue('<ds-initialize sas-command="@SAS_FOUNDATION_HOME@/sas -path @ESP_SASIO32_LIB@"' in out)

    #   def test_get_project_stats(self):
#       model_xml_path = os.path.join(DATA_DIR, 'model_sa.xml')
#       data_csv_path = os.path.join(DATA_DIR, 'data_sa.csv')
#       name = 'ESPUnitTestProjectSA'
#       eg_name = 'MyUnitTestEventGenerator'

#       with self.assertRaises(KeyError):
#           self.s.get_project(name)

#       proj = self.s.install_project(model_xml_path)

#       stats = self.s.get_project_stats(name=proj.name)
#       self.assertTrue(isinstance(stats, dict))
#       self.assertIn(proj.name, stats)

    
if __name__ == '__main__':
   tm.runtests()
