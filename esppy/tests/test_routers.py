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

import datetime
import os
import six
import esppy
import sys
import unittest
from . import utils as tm
from esppy.router import Router

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestRouter(tm.TestCase):

    pub_xml = r'''<esp-router name="router">
  <esp-engines>
    <esp-engine host="localhost" name="esp1" port="42002" />
    <esp-engine host="localhost" name="esp2" port="52002" />
  </esp-engines>
  <esp-destinations>
    <publish-destination name="src" opcode="upsert">
      <publish-target>
        <engine-func>esp2</engine-func>
        <project-func>proj2</project-func>
        <contquery-func>VERT_PEAK_FORCE</contquery-func>
        <window-func>READINGS_INPUT</window-func>
      </publish-target>
    </publish-destination>
  </esp-destinations>
  <esp-routes>
    <esp-route name="source" snapshot="false" to="src">
      <engine-expr>esp1</engine-expr>
      <project-expr>proj1</project-expr>
      <contquery-expr>VERT_PEAK_FORCE</contquery-expr>
      <window-expr>READINGS_INPUT</window-expr>
    </esp-route>
  </esp-routes>
</esp-router>'''

    write_xml = r'''<esp-router name="router">
  <esp-engines>
    <esp-engine host="localhost" name="esp1" port="42002" />
  </esp-engines>
  <esp-destinations>
    <writer-destination dateformat="%Y%m%dT%H:%M:%S.%f" format="csv" name="out1">
      <file-func>string('./','output1.csv')</file-func>
    </writer-destination>
    <writer-destination dateformat="%Y%m%dT%H:%M:%S.%f" format="csv" name="out2">
      <file-func>string('./','output2.csv')</file-func>
    </writer-destination>
    <publish-destination name="src" opcode="upsert">
      <publish-target>
        <engine-func>esp1</engine-func>
        <project-func>proj2</project-func>
        <contquery-func>VERT_PEAK_FORCE</contquery-func>
        <window-func>READINGS_INPUT</window-func>
      </publish-target>
    </publish-destination>
  </esp-destinations>
  <esp-routes>
    <esp-route name="output1" snapshot="false" to="out1">
      <engine-expr>esp1</engine-expr>
      <project-expr>proj1</project-expr>
      <contquery-expr>VERT_PEAK_FORCE</contquery-expr>
      <window-expr>READINGS_INPUT</window-expr>
    </esp-route>
    <esp-route name="output2" snapshot="false" to="out2">
      <engine-expr>esp1</engine-expr>
      <project-expr>proj2</project-expr>
      <contquery-expr>VERT_PEAK_FORCE</contquery-expr>
      <window-expr>READINGS_INPUT</window-expr>
    </esp-route>
    <esp-route name="source" snapshot="false" to="src">
      <engine-expr>esp1</engine-expr>
      <project-expr>proj1</project-expr>
      <contquery-expr>VERT_PEAK_FORCE</contquery-expr>
      <window-expr>READINGS_INPUT</window-expr>
    </esp-route>
  </esp-routes>
</esp-router>'''

    def setUp(self):
        self.s = esppy.ESP(HOST, PORT, USER, PASSWD, protocol=PROTOCOL)

    def test_xml(self):
        router = Router.from_xml(self.pub_xml)
        out_xml = router.to_xml(pretty=True).strip()

        self.assertEqual(self.pub_xml, out_xml)

    def test_pub_api(self):
        router = self.s.create_router('router')

        router.add_engine('localhost', 52002, name='esp2')
        router.add_engine('localhost', 42002, name='esp1')

        router.add_publish_destination('esp2.proj2.VERT_PEAK_FORCE.READINGS_INPUT', name='src', opcode='upsert')

        router.add_route('esp1.proj1.VERT_PEAK_FORCE.READINGS_INPUT', 'src', name='source', snapshot=False)

        xml = router.to_xml(pretty=True).strip()

        self.assertEqual(self.pub_xml, xml)

    def test_write_api(self):
        router = self.s.create_router(name='router')

        router.add_engine('localhost', '42002', name='esp1')

        router.add_publish_destination('esp1.proj2.VERT_PEAK_FORCE.READINGS_INPUT', name='src', opcode='upsert')
        router.add_writer_destination("string('./','output1.csv')", 'csv', name='out1')
        router.add_writer_destination("string('./','output2.csv')", 'csv', name='out2')

        router.add_route('esp1.proj1.VERT_PEAK_FORCE.READINGS_INPUT', 'src', name='source')
        router.add_route('esp1.proj1.VERT_PEAK_FORCE.READINGS_INPUT', 'out1', name='output1')
        router.add_route('esp1.proj2.VERT_PEAK_FORCE.READINGS_INPUT', 'out2', name='output2')
    
        xml = router.to_xml(pretty=True).strip()

        self.maxDiff = None
        self.assertEqual(self.write_xml, xml)


if __name__ == '__main__':
   tm.runtests()
