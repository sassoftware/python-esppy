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

import base64
import copy
import numpy as np
import os
import pandas as pd
import six
import unittest
from PIL import Image
from . import utils as tm
from ..transformers import bgr2rgb, rgb2bgr, bytes2image

DATA_DIR = tm.get_data_dir()


class TestTransformers(tm.TestCase):

    def test_rgb2bgr(self):
        # Images
        rgb1 = Image.fromarray(np.asarray(Image.open(os.path.join(DATA_DIR, 'scooter.jpg'))))
        rgb2 = bgr2rgb(rgb2bgr(Image.open(os.path.join(DATA_DIR, 'scooter.jpg'))))

        bgr1 = rgb2bgr(Image.open(os.path.join(DATA_DIR, 'scooter.jpg')))
        bgr2 = rgb2bgr(bgr2rgb(rgb2bgr(Image.open(os.path.join(DATA_DIR, 'scooter.jpg')))))

        self.assertEqual(list(rgb1.getdata()), list(rgb2.getdata()))
        self.assertEqual(list(bgr1.getdata()), list(bgr2.getdata()))
        self.assertNotEqual(list(bgr1.getdata()), list(rgb1.getdata()))

        # DataFrame
        df_rgb1 = pd.DataFrame([['a', rgb1], ['b', rgb2]], columns=['Name', 'Image'])
        df_rgb2 = bgr2rgb(rgb2bgr(df_rgb1))

        self.assertEqual(list(df_rgb1.columns), ['Name', 'Image'])
        self.assertEqual(list(df_rgb2.columns), ['Name', 'Image'])
        self.assertEqual(list(df_rgb1['Image'][0].getdata()),
                         list(df_rgb2['Image'][0].getdata()))
        self.assertEqual(list(df_rgb1['Image'][1].getdata()),
                         list(df_rgb2['Image'][1].getdata()))
        
        # DataFrame with columns
        df_rgb1 = pd.DataFrame([['a', rgb1], ['b', rgb2]], columns=['Name', 'Image'])
        df_rgb2 = bgr2rgb(rgb2bgr(df_rgb1, columns='Image'), columns='Image')

        self.assertEqual(list(df_rgb1.columns), ['Name', 'Image'])
        self.assertEqual(list(df_rgb2.columns), ['Name', 'Image'])
        self.assertEqual(list(df_rgb1['Image'][0].getdata()),
                         list(df_rgb2['Image'][0].getdata()))
        self.assertEqual(list(df_rgb1['Image'][1].getdata()),
                         list(df_rgb2['Image'][1].getdata()))
        
        # Unknown data
        self.assertEqual(bgr2rgb(10), 10)
        self.assertEqual(rgb2bgr(10), 10)

    def test_bytes2image(self):
        # Image
        jpg = open(os.path.join(DATA_DIR, 'scooter.jpg'), 'rb').read()
        img = bytes2image(jpg)
        self.assertTrue(Image.isImageType(img))

        # DataFrame
        df_jpg = pd.DataFrame([['a', jpg]], columns=['Name', 'Image'])
        df_jpg = bytes2image(df_jpg)
        self.assertEqual(list(df_jpg.columns), ['Name', 'Image'])
        self.assertTrue(Image.isImageType(df_jpg['Image'][0]))

        # DataFrame with columns
        df_jpg = pd.DataFrame([['a', jpg]], columns=['Name', 'Image'])
        df_jpg = bytes2image(df_jpg, columns='Image')
        self.assertEqual(list(df_jpg.columns), ['Name', 'Image'])
        self.assertTrue(Image.isImageType(df_jpg['Image'][0]))

        # Unknown data
        self.assertEqual(bytes2image(10), 10)


if __name__ == '__main__':
    tm.runtests()
