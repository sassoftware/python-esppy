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

''' Utilities '''

from __future__ import print_function, division, absolute_import, unicode_literals

import json
import re
import requests
import logging
import sys
import xml.etree.ElementTree as ET
from six.moves import urllib
from ..config import get_option
from ..exceptions import ESPError

try:
    from json.decoder import JSONDecodeError
except ImportError:
    JSONDecodeError = ValueError


def to_camel(val):
    '''
    Convert underscore-delimited names to camel-case

    Parameters
    ----------
    val : string
        The string to convert

    Returns
    -------
    string

    '''
    return re.sub(r'_([A-Za-z])', lambda x: x.group(1).upper(), val)


def to_underscore(val):
    '''
    Convert camel-case names to underscore-delimited

    Parameters
    ----------
    val : string
        The string to convert

    Returns
    -------
    string

    '''
    out = re.sub(r'([A-Z])', lambda x: '_' + x.group(1).lower(), val)
    if re.match(r'[A-Z]', val):
        return out[1:]
    return out


def _to_string(value, ESPObject):
    if value is True:
        return 'true'
    elif value is False:
        return 'false'
    elif isinstance(value, ESPObject):
        return value.name
    if isinstance(value, (list, set, tuple)):
        out = []
        for item in value:
            out.append(_to_string(item, ESPObject).replace(' ', '%20'))
        return '|'.join(out)
    else:
        return '%s' % value


def get_params(**kwargs):
    '''
    Convert keyword arguments into URL parameters

    This function does the following conversions:

        * Underscore-delimited names to camel-case
        * True / False values to 'true' / 'false'
        * ['a', 'b', 'c'] to 'a|b|c'
        * ESPObject to ESPObject.name

    Parameters
    ----------
    **kwargs : keyword arguments, optional
       Arbitrary Python arguments to convert to URL parameters

    '''
    try:
        ESPObject    # noqa: F821
    except:
        from ..base import ESPObject

    out = {}
    for key, value in kwargs.items():
        if value is not None:
            out[to_camel(key)] = _to_string(value, ESPObject)
    return out


class RESTHelpers(object):
    '''
    Helper methods for running REST requests against an ESP server

    Attributes
    ----------
    session : requests.Session
        The session object
    base_url : string
        The base server URL
    url : string
        The URL of the object

    '''

    def __init__(self, session=None):
        self.session = session

    def _insert_params(self, url, **kwargs):
        # The REST server is a bit particular about what can and
        # can't be URL encoded.  Handle that here.
        params = kwargs.pop('params', {})
        params = '&'.join(['%s=%s' % (key, ('%s' % value).replace(' ', '%20'))
                           for key, value in params.items()])
        if '?' in url:
            params = '&%s' % params
        else:
            params = '?%s' % params
        return (url + params, kwargs)

    def _verify_session(self):
        if self.session is None:
            raise ValueError('There is no session associated with this object.')

    @property
    def session(self):
        ''' The server session object '''
        return getattr(self, '_session', None)

    @session.setter
    def session(self, session):
        setattr(self, '_session', session)

    @property
    def base_url(self):
        ''' The base server URL '''
        self._verify_session()
        return self.session.base_url

    @property
    def url(self):
        ''' The URL of the object '''
        self._verify_session()
        return self.session.base_url

    def _get(self, path=None, format='xml', raw=False, **kwargs):
        '''
        GET the specified path

        Parameters
        ----------
        path : string, optional
            Absolute or relative URL
        format : string, optional
            Output format: 'xml' or 'json'
        raw : boolean, optional
            Should the raw content be returned rather than an Element?
        **kwargs : keyword parameters, optional
            URL parameters

        Returns
        -------
        dict

        '''
        self._verify_session()

        if path and (path.startswith('http:') or path.startswith('https:')):
            url = path
        elif path:
            url = urllib.parse.urljoin(self.url, path)
        else:
            url = self.url

        if format == 'json':
            try:
                out = self.session.get(url, **kwargs)
                out = out.json()
                return out
            except JSONDecodeError:
                sys.stderr.write('%s\n' % out.content.decode('utf-8'))
                raise

        url, kwargs = self._insert_params(url, **kwargs)

        if get_option('debug.requests'):
            sys.stderr.write('GET %s\n' % url)

        if get_option('debug.request_bodies') and kwargs.get('data'):
            sys.stderr.write('%s\n' % kwargs['data'])

        content = self._error_check(self.session.get(url,
                                                     **kwargs)).content.decode('utf-8')

        if get_option('debug.responses'):
            sys.stderr.write('%s\n' % content)

        if raw:
            return content

        return ET.fromstring(content)

    def _post(self, path=None, **kwargs):
        '''
        POST the specified path

        Parameters
        ----------
        path : string, optional
            Absolute or relative URL
        **kwargs : keyword parameters, optional
            URL parameters

        Returns
        -------
        dict

        '''
        self._verify_session()

        if path and (path.startswith('http:') or path.startswith('https:')):
            url = path
        elif path:
            url = urllib.parse.urljoin(self.url, path)
        else:
            url = self.url

        url, kwargs = self._insert_params(url, **kwargs)

        if get_option('debug.requests'):
            sys.stderr.write('POST %s\n' % url)

        content = self._error_check(self.session.post(url, **kwargs)).content.decode('utf-8')

        if get_option('debug.responses'):
            sys.stderr.write('%s\n' % content)

        return ET.fromstring(content)

    def _put(self, path=None, **kwargs):
        '''
        PUT the specified path

        Parameters
        ----------
        path : string, optional
            Absolute or relative URL
        **kwargs : keyword parameters, optional
            URL parameters

        Returns
        -------
        dict

        '''
        self._verify_session()

        if path and (path.startswith('http:') or path.startswith('https:')):
            url = path
        elif path:
            url = urllib.parse.urljoin(self.url, path)
        else:
            url = self.url

        url, kwargs = self._insert_params(url, **kwargs)

        if get_option('debug.requests'):
            sys.stderr.write('PUT %s\n' % url)

        content = self._error_check(self.session.put(url, **kwargs)).content.decode('utf-8')

        if get_option('debug.responses'):
            sys.stderr.write('%s\n' % content)

        return ET.fromstring(content)

    def _delete(self, path=None, **kwargs):
        '''
        DELETE the specified path

        Parameters
        ----------
        path : string, optional
            Absolute or relative URL
        **kwargs : keyword parameters, optional
            URL parameters

        Returns
        -------
        dict

        '''
        self._verify_session()

        if path and (path.startswith('http:') or path.startswith('https:')):
            url = path
        elif path:
            url = urllib.parse.urljoin(self.url, path)
        else:
            url = self.url

        url, kwargs = self._insert_params(url, **kwargs)

        if get_option('debug.requests'):
            sys.stderr.write('DELETE %s' % url)

        content = self._error_check(self.session.delete(url, **kwargs)).content.decode('utf-8')

        if get_option('debug.responses'):
            sys.stderr.write('%s\n' % content)

        return ET.fromstring(content)

    def _error_check(self, resp):
        '''
        Check the server response for errors

        Parameters
        ----------
        resp : dict
            Data from server

        Returns
        -------
        ``resp``

        '''

        error = None

        if resp.status_code >= 400:
            try:
                elem = ET.fromstring(resp.content)
            except ET.ParseError:
                sys.stderr.write('%s\n' % resp.content)
                raise

            msg = elem.find('./message/response/message')
            if msg is not None:
                error = msg.text
                if get_option('debug.responses'):
                    sys.stderr.write('%s\n' % resp.content)

            msg = elem.find('./message')
            if msg is not None:
                error = msg.text
                if get_option('debug.responses'):
                    sys.stderr.write('%s\n' % resp.content)

            if get_option('debug.responses'):
                sys.stderr.write('%s\n' % resp.content)

            if error != None:
                details = elem.findall('./details/detail')
                for detail in details:
                    error += "\n";
                    error += detail.text;
            else:
                error = resp.content

        if error != None:
            raise ESPError(error)

        return resp
