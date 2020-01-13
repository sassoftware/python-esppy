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
Utility for handling authentication info

'''

from base64 import b64encode

class Authorization(object):
    _instance = None

    @staticmethod
    def getInstance(session):
        if Authorization._instance == None:
            Authorization._instance = Authorization(session)

        return(Authorization._instance)

    def __init__(self,session):
        self._session = session
        self._authorization = None
        self._hostname = None

    def setBasic(self,username,password):
        basic = username + ":" + password
        encoded = b64encode(basic.encode())
        self._authorization = "Basic " + encoded.decode()

    def setBearer(self,token):
        self._authorization = "Bearer " + token

    def setKerberos(self,hostname):
        self._hostname = hostname

    @property
    def isEnabled(self):
        return(self._authorization != None or self._hostname != None)

    @property
    def authorization(self):
        authorization = None

        if self._hostname != None:
            authorization = self._session.auth.generate_request_header(None,self._hostname,True)
        else:
            authorization = self._authorization

        return(authorization)

