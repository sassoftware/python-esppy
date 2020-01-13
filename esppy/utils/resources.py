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

'''
Utility for handling message resources

'''

from rbtranslations import Translations
import rbtranslations

import os

class Resources(object):
    _instance = None

    @staticmethod
    def getInstance():
        if Resources._instance == None:
            Resources._instance = Resources()

        return(Resources._instance)

    def __init__(self):
        self._bundles = {}

        path = os.path.abspath (os.path.join(os.path.dirname(__file__), "../locales/pymsg.properties"))
        #with open(path,mode="r",encoding="utf-8") as fp:
        #with open(path,encoding="iso-8859-1") as fp:
        #with open(path,mode="rb") as fp:
        with open(path,mode="r") as fp:
            self._default = Translations(fp)

    def getText(self,key,locale = None):
        text = None
        bundle = None

        if locale != None:
            if locale in self._bundles:
                bundle = self._bundles[locale]
        else:
            bundle = self._default

        if bundle != None:
            text = bundle.gettext(key)

        return(text)
