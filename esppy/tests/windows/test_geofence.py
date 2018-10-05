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
import unittest
import esppy
from . import utils as tm


class TestGeofenceWindow(tm.WindowTestCase):

    def test_xml(self):
        self._test_model_file('geofence_window.xml')

    def test_api(self):
        esp = self.s

        proj = esp.create_project('project_01_UnitTest', pubsub='auto',
                                  n_threads='1', index_type='empty')

        cq = esp.ContinuousQuery(name='cq_01')

        src1 = esp.SourceWindow(name='position_in',
                                schema=('vehicle:string','pt_id*:int64',
                                        'GPS_longitude:double','GPS_latitude:double',
                                        'speed:double','course:double','time:stamp'),
                                pubsub='true', insert_only=True,
                                collapse_updates=True, autogen_key=True)

        src2 = esp.SourceWindow(name='poly_in',
                                schema=('poly_id*:int64',
                                        'poly_desc:string',
                                        'poly_data:string'),
                                pubsub='true', insert_only=True,
                                collapse_updates=True)

        geofence_win = esp.GeofenceWindow(name='geofence_poly',
                                          index_type='empty', 
                                          output_insert_only=True,
                                          coordinate_type='geographic',
                                          meshfactor_x='-2', meshfactor_y='-2',
                                          log_invalid_geometry='false',
                                          output_multiple_results='false',
                                          output_sorted_results='true',
                                          max_meshcells_per_geometry='10',
                                          autosize_mesh='true',
                                          polygon_compute_distance_to='centroid')
        geofence_win.set_geometry(data_fieldname='poly_data', desc_fieldname='poly_desc')
        geofence_win.set_position(x_fieldname='GPS_longitude', y_fieldname='GPS_latitude')
        geofence_win.set_output(geoid_fieldname='poly_id',
                                geodesc_fieldname='poly_desc',
                                geodistance_fieldname='poly_dist')

        geofence_win.add_connector('fs',
                                   conn_name='sub', conn_type='subscribe',
                                   type='sub',
                                   fstype='csv',
                                   fsname='result.out',
                                   snapshot=True)

        proj.add_query(cq)
        cq.add_window(src1)
        cq.add_window(src2)
        cq.add_window(geofence_win)

        src1.add_target(geofence_win)
        src2.add_target(geofence_win)

        self._test_model_file('geofence_window.xml', proj)
