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

import os
import re
import six
import esppy
import unittest
from .. import utils as tm
from ..utils import elements_equal, normalize_xml
from ...utils import xml

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestWindowFeatures(tm.TestCase):

    def setUp(self):
        self.s = esppy.ESP(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

    def tearDown(self):
        self.s.delete_projects(filter="match(name, '^.*UnitTest.*$')")

    def _test_model_file(self, model):
        model_file = os.path.join(DATA_DIR, model)
        model_xml = normalize_xml(open(model_file, 'r').read())

        proj1 = self.s.Project.from_xml(model_xml)
        proj1_xml = normalize_xml(proj1.to_xml(pretty=True))

        proj2 = self.s.load_project(model_file, start=False)
        proj2_xml = normalize_xml(proj2.to_xml(pretty=True))

        elements_equal(model_xml, proj1_xml)
        elements_equal(model_xml, proj2_xml)

    def test_splitter(self):
        self._test_model_file('splitter_model.xml')

    def test_filter_exp(self):
        self._test_model_file('filter_exp_model.xml')

    def test_retention(self):
        self._test_model_file('retention_model.xml')

    def test_slot_exp(self):
        self._test_model_file('slot_exp_model.xml')

    def test_aggregate_exp(self):
        self._test_model_file('aggregate_exp_model.xml')

    @unittest.skip('Need way to install plugin')
    def test_aggregate_func(self):
        self._test_model_file('aggregate_func_model.xml')

    def test_aggregate_last(self):
        self._test_model_file('aggregate_last_model.xml')
    
    def test_aggregate_userdef(self):
        self._test_model_file('aggregate_userdef_model.xml')
    
    def test_analytics_dbscan(self):
        self._test_model_file('analytics_dbscan_model.xml')
    
    def test_analytics_kmeans(self):
        self._test_model_file('analytics_kmeans_model.xml')
    
    def test_broker_surv(self):
        self._test_model_file('broker_surv_model.xml')

    def test_calculate_img_proc(self):
        self._test_model_file('calculate_img_proc_model.xml')
    
    @unittest.skip('Need way to install plugin')
    def test_compute_context(self):
        self._test_model_file('compute_context_model.xml')
    
    def test_compute_exp_udf(self):
        self._test_model_file('compute_exp_udf_model.xml')
    
    def test_compute_exp(self):
        self._test_model_file('compute_exp_model.xml')
    
    @unittest.skip('Need way to install plugin')
    def test_compute_func(self):
        self._test_model_file('compute_func_model.xml')

    def test_compute_udf(self):
        self._test_model_file('compute_udf_model.xml')

    def test_copy_with_slots(self):
        self._test_model_file('copy_with_slots_model.xml')

    def test_db_connector_publisher(self):
        self._test_model_file('db_connector_publisher_model.xml')

    def test_db_connector_subscriber(self):
        self._test_model_file('db_connector_subscriber_model.xml')

    @unittest.skip('Need way to install plugin')
    def test_filter_func(self):
        self._test_model_file('filter_func_model.xml')

    def test_filter_on_flag(self):
        self._test_model_file('filter_on_flag_model.xml')

    def test_filter_on_opcode(self):
        self._test_model_file('filter_on_opcode_model.xml')

    @unittest.skip('Update to 5.2 specs')
    def test_ds2_procedural(self):
        self._test_model_file('ds2_procedural_model.xml')

    @unittest.skip('Crashes server')
    def test_ds_procedural(self):
        self._test_model_file('ds_procedural_model.xml')

    @unittest.skip('Update to 5.2 specs')
    def test_mas_composite(self):
        self._test_model_file('mas_composite_model.xml')

    @unittest.skip('Update to 5.2 specs')
    def test_mas_ds2_procedural(self):
        self._test_model_file('mas_ds2_procedural_model.xml')

    @unittest.skip('Update to 5.2 specs')
    def test_mas_python_procedural(self):
        self._test_model_file('mas_python_procedural_model.xml')

    def test_fs_adapter_publish(self):
        self._test_model_file('fs_adapter_publish_model.xml')

    @unittest.skip('Invalid resource')
    def test_functional_event_loop(self):
        self._test_model_file('functional_event_loop_model.xml')

    def test_geofence2(self):
        self._test_model_file('geofence2_model.xml')

    def test_geofence_proximity_analysis(self):
        self._test_model_file('geofence_proximity_analysis_model.xml')

    def test_geofence(self):
        self._test_model_file('geofence_model.xml')

    def test_join_exp(self):
        self._test_model_file('join_exp_model.xml')

    def test_join_select(self):
        self._test_model_file('join_select_model.xml')

    def test_log_config(self):
        self._test_model_file('log_config_model.xml')

    def test_orders(self):
        self._test_model_file('orders_model.xml')

    @unittest.skip('Need way to install plugin')
    def test_pattern2(self):
        self._test_model_file('pattern2_model.xml')

    def test_pattern_empty_index(self):
        self._test_model_file('pattern_empty_index_model.xml')

    def test_pattern(self):
        self._test_model_file('pattern_model.xml')

    @unittest.skip('Need way to install plugin')
    def test_procedural(self):
        self._test_model_file('procedural_model.xml')

    def test_regex(self):
        self._test_model_file('regex_model.xml')

    def test_splitter_initexp(self):
        self._test_model_file('splitter_initexp_model.xml')

    def test_splitter_udf(self):
        self._test_model_file('splitter_udf_model.xml')

    @unittest.skip('Needs mco file')
    def test_text_category(self):
        self._test_model_file('text_category_model.xml')

    @unittest.skip('Needs sam file')
    def test_text_sentiment(self):
        self._test_model_file('text_sentiment_model.xml')

    def test_textcat(self):
        self._test_model_file('textcat_model.xml')

    def test_trades(self):
        self._test_model_file('trades_model.xml')

    def test_twitter_adapter(self):
        self._test_model_file('twitter_adapter_model.xml')

    def test_union(self):
        self._test_model_file('union_model.xml')

    def test_url_connector_news(self):
        self._test_model_file('url_connector_news_model.xml')

    def test_url_connector_weather(self):
        self._test_model_file('url_connector_weather_model.xml')

    def test_vwap(self):
        self._test_model_file('vwap_model.xml')

    @unittest.skip('Update to 5.2 specs')
    def test_wind_turbine(self):
        self._test_model_file('wind_turbine_model.xml')

    def test_ws_connector_json(self):
        self._test_model_file('ws_connector_json_model.xml')

    def test_ws_connector_json2(self):
        self._test_model_file('ws_connector_json2_model.xml')

    def test_xml_connector_publisher(self):
        self._test_model_file('xml_connector_publisher_model.xml')

    def test_xml_connector_subscriber(self):
        self._test_model_file('xml_connector_subscriber_model.xml')


if __name__ == '__main__':
   tm.runtests()
