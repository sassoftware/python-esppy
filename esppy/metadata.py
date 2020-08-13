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

''' ESP Metadata '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import six
from six.moves import urllib
from .base import RESTHelpers


class Metadata(collections.abc.MutableMapping, RESTHelpers):
    '''
    Base class for metadata dictionaries

    This is an abstract class that must be subclassed.  You must
    implement the ``__metadata__``, ``__setitem__``, and ``__delitem__``
    methods.

    Parameters
    ----------
    session : requests.Session
        The session object

    '''

    def __init__(self, session=None):
        RESTHelpers.__init__(self, session=session)

    def __metadata__(self):
        raise NotImplementedError

    def _set_metadata_from_xml(self, item, out):
        out[item.attrib['id']] = item.text

    def __getitem__(self, key):
        return self.__metadata__()[key]

    def __setitem__(self, key, value):
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError

    def __iter__(self):
        return iter(self.__metadata__())

    def __len__(self):
        return len(self.__metadata__())

    def __str__(self):
        return str(self.__metadata__())

    def __repr__(self):
        return repr(self.__metadata__())
