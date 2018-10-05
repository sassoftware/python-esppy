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
import numpy as np
import pandas as pd
import sys
import unittest
from PIL import Image
from esppy.plotting import split_chart_params, highlight_image
from . import utils as tm

USER, PASSWD = tm.get_user_pass()
HOST, PORT, PROTOCOL = tm.get_host_port_proto()
DATA_DIR = tm.get_data_dir()


class TestPlottingUtils(tm.TestCase):

    def test_split_chart_params(self):
        fig, mthd = split_chart_params(x_range=[0,1], y_range=[0,1], x='x_c', y='y_c')
        self.assertEqual(fig, dict(x_range=[0,1], y_range=[0,1]))
        self.assertEqual(mthd, dict(x='x_c', y='y_c'))

    def test_highlight_image(self):
        in_img = Image.open(os.path.join(DATA_DIR, 'scooter.jpg'))

        detections = pd.DataFrame([[1, 'Scooter', 1, 35, 35, 100, 100]],
                                  columns=['_nObjects_', '_Object0_', '_P_Object0_',
                                           '_Object0_x', '_Object0_y', '_Object0_width',
                                           '_Object0_height'])

        out_img = highlight_image(in_img, detections)

        bench_img = Image.open(os.path.join(DATA_DIR, 'scooter-hilite.jpg'))

#       TODO: Compare with benchmark
#       self.assertEqual(list(out_img.getdata()), list(bench_img.getdata()))


if __name__ == '__main__':
   tm.runtests()
