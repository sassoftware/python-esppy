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

''' ESP Connection '''

from __future__ import print_function, division, absolute_import, unicode_literals

import base64
import collections
import os
import pandas as pd
import re
import requests
import six
import sys
import textwrap
import threading
import warnings
import xml.etree.ElementTree as ET
from numpy import nan
from six.moves import urllib
from urllib.parse import urlparse
from .base import RESTHelpers, ESPObject
from .algorithm import Algorithm
from .config import get_option, ESP_ROOT, CONCAT_OPTIONS
from .connectorinfo import ConnectorInfo
from .mas import MASModule
from .router import Router
from .templates import template
from . import contquery
from . import project
from . import windows
from .evtgen import EventGenerator
from .exceptions import ESPError
from .logger import Logger
from .metadata import Metadata
from .utils import xml
from .utils.authorization import Authorization
from .utils.authinfo import query_authinfo
from .utils.rest import get_params
from .utils.data import get_project_data, gen_name, get_server_info
from .utils.events import get_events
from .utils.keyword import dekeywordify
from .utils.project import expand_path
from .websocket import WebSocketClient
from .windows import BaseWindow, get_window_class
from .espapi import api


class ProjectStats(object):
    '''
    Project statistics subscriber

    Parameters
    ----------
    session : requests.Session or ESP or Project
        The object that supplies the ESP session
    filter : string, optional
        Functional filter to match projects.  If a project is given as
        the first argument, the filter is set to only view that project.
    interval : int, optional
        The interval in seconds between updates
    min_cpu : int, optional
        The minimum CPU value you want included
    limit : int, optional
        The maximum number of rows to retain in the DataFrame

    '''

    def __init__(self, session, filter=None, interval=None, min_cpu=None,
                 limit=20):
        self._ws = None
        self.filter = filter
        self.interval = interval
        self.min_cpu = min_cpu
        self.limit = limit

        if isinstance(session, project.Project):
            self.filter = "in(name,'%s')" % session.name
            self.session = session.session
        elif isinstance(session, ESPObject):
            self.session = session.session
        else:
            self.session = session

        stats = pd.DataFrame(columns=['project', 'contquery',
                                      'window', 'interval', 'cpu'],
                             data=[['', '', '', 0, 0.0]])
        stats = stats.set_index(['project', 'contquery', 'window'])
        self.stats = stats.iloc[0:0]

    def __getitem__(self, key):
        return self.stats[key]

    def __getattr__(self, name):
        return getattr(self.stats, name)

    def __len__(self):
        return len(self.stats)

    @property
    def url(self):
        '''
        Return the URL of the project statitics

        Returns
        -------
        string

        '''
        url_params = get_params(**{'format': 'xml',
                                   'interval': self.interval,
                                   'filter': self.filter,
                                   'minCpu': self.min_cpu})
        url_params = '&'.join(['%s=%s' % (k, v) for k, v in sorted(url_params.items())])
        s = re.findall("^\w+:", self.session.base_url)
        wsproto = (s[0] == "https:") and "wss" or "ws"
        value = re.sub(r'^\w+:', wsproto + ':', self.session.base_url) + 'projectStats?' + url_params
        return(value)

    @property
    def is_active(self):
        '''
        Is the web socket active?

        Returns
        -------
        bool

        '''
        return self._ws is not None

    def start(self):
        ''' Initialize the web socket and start it in its own thread '''
        if self._ws is not None:
            return

        def on_message(sock, message):
            if re.match(r'^\s*\w+\s*:\s*\d+\s*$', message):
                return

            if get_option('debug.events'):
                sys.stderr.write('%s\n' % message)

            rows = []

            elem = xml.from_xml(message)
            for proj in elem.findall('./project'):
                proj_name = proj.attrib['name']
                for cq in proj.findall('./contquery'):
                    cq_name = cq.attrib['name']
                    for win in cq.findall('./window'):
                        win_name = win.attrib['name']
                        attrib = dict(win.attrib)
                        attrib.pop('name')
                        row = dict(project=proj_name,
                                   contquery=cq_name,
                                   window=win_name)
                        for key, value in six.iteritems(attrib):
                            if re.match(r'^\s*\d+\s*$', value):
                                value = int(value)
                            elif re.match(r'^\s*\d*\.\d*\s*$', value):
                                value = float(value)
                            row[key] = value
                        rows.append(row)

            if rows:
                data = pd.DataFrame(columns=rows[0].keys(), data=rows)
                data = data.set_index(['project', 'contquery', 'window'])
                data = data[['interval'] + list(sorted(x for x in data.columns
                                                       if x != 'interval'))]
                data = pd.concat([self.stats, data], **CONCAT_OPTIONS)
                data = data.sort_index().sort_values(['interval']).tail(self.limit)
                self.stats = data

        if get_option('debug.requests'):
            sys.stderr.write('WEBSOCKET %s\n' % self.url)

        self._ws = WebSocketClient(self.url, on_message=on_message)
        self._ws.connect()

        ws_thread = threading.Thread(name='%s-%s' % (id(self), self.url),
                                     target=self._ws.run_forever)
        ws_thread.daemon = True
        ws_thread.start()

    def stop(self):
        ''' Stop processing events and close the web socket '''
        if self._ws is not None:
            self._ws.close()
            self._ws = None

    close = stop


class EngineMetadata(Metadata):
    ''' Metadata for ESP Engine '''

    def __metadata__(self):
        out = {}
        for item in self._get('engineMetadata').findall('.//meta'):
            self._set_metadata_from_xml(item, out)
        return out

    def __setitem__(self, key, value):
        self._put('engineMetadata/%s' % key, data=str(value))

    def __delitem__(self, key):
        self._delete('engineMetadata/%s' % key)


class ESP(RESTHelpers):
    '''
    ESP Connection

    Parameters
    ----------
    hostname : string, optional
        Hostname of ESP server or ESP connection URL
    port : int, optional
        Port number of ESP server.  This is not needed if
        a URL is used in the first parameter.
    username : string, optional
        Username for authentication
    password : string, optional
        Password for authentication.  If a password is specified,
        without a username, this is assumed to be an OAUTH token.
    protocol : string, optional
        The protocol to use: http or https.  This is not
        needed if a URL is used in the first parameter.
    ca_bundle : string, optional
        Path to the certificate bundle if using SSL
    authinfo : string, optional
        Path to the authinfo file containing authentication
        information.

    Attributes
    ----------
    metadata : dict
        ESP engine metadata
    server_info : dict
        ESP server information
    connector_info : dict
        Information about ESP connectors
    api_docs : dict
        Swagger JSON documentation

    Examples
    --------
    Start ESP connection

    >>> esp = esppy.ESP('http://esp-host.com:8080')
    >>> esp.server_info
    { ... }

    Create a project from a definition file

    >>> proj = esp.load_project('/path/to/model.xml')
    >>> proj.start()

    '''

    Project = project.Project
    Query = ContinuousQuery = contquery.ContinuousQuery
    Template = template.Template

    def __init__(self, hostname=None, port=None, username=None,
                 password=None, protocol=None, ca_bundle=None,
                 authinfo=None, auth_obj=None):
        # Use environment variables as needed
        if hostname is None and get_option('hostname'):
            hostname = get_option('hostname')
        if port is None and get_option('port'):
            port = int(get_option('port'))
        if username is None and os.environ.get('ESPUSER'):
            username = os.environ['ESPUSER']
        if password is None and os.environ.get('ESPPASSWORD'):
            password = os.environ['ESPPASSWORD']
        if protocol is None and get_option('protocol'):
            protocol = get_option('protocol')

        # Set default protocol
        if not protocol:
            if ca_bundle:
                protocol = 'https'
            else:
                protocol = 'http'

        # Construct base URL
        if re.match('^https?://', hostname):
            base_url = hostname
            if not base_url.endswith('/'):
                base_url = '%s/' % base_url
            if not re.search(r'%s/' % ESP_ROOT, base_url):
                conn_url = base_url
                base_url = urllib.parse.urljoin(base_url, '%s/' % ESP_ROOT)
        else:
            conn_url = '%s://%s:%s' % (protocol, hostname, port)
            base_url = '%s://%s:%s/%s/' % (protocol, hostname, port, ESP_ROOT)

        session = requests.Session()
        session.conn_url = conn_url
        session.base_url = base_url

        u = urlparse(session.conn_url)

        s = u[1].split(":")

        self._hostname = s[0]
        self._port = s[1]

        auth = Authorization.getInstance(session)

        self._kerberos = False
        self._authorization = None

        enable_kerberos = False

        # Set certificate verification
        if ca_bundle:
            session.verify = ca_bundle

        # Use specified auth object
        if auth_obj is not None:
            session.auth = auth_obj

        else:
            enable_kerberos = username is None and password is None

            # Check for authinfo file
            if password is None:
                authinfo = query_authinfo(host=hostname, user=username,
                                          protocol=port, path=authinfo)
                if authinfo is not None:
                    username = authinfo.get('user', username)
                    password = authinfo.get('password')

            # Set authorization headers
            if username and password:
                auth.setBasic(username,password)
                basic = username + ":" + password
                encoded = base64.b64encode(basic.encode())
                self._authorization = "Basic " + encoded.decode()
                session.headers.update({'Authorization':self._authorization.encode("utf-8")})
            elif password:
                auth.setBearer(password)
                self._authorization = "Bearer " + password
                session.headers.update({'Authorization':self._authorization.encode("utf-8")})

            # Verify authentication
            res = session.head(session.base_url)
            if res.status_code == 401 and enable_kerberos:
                try:
                    self._kerberos = True
                    from requests_kerberos import HTTPKerberosAuth, OPTIONAL
                    auth.setKerberos(self._hostname)
                    session.auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
                    res = session.head(session.base_url)
                    if res.status_code == 401:
                        raise ESPError(res.reason)

                except ImportError:
                    warnings.warn('Kerberos authentication attempted, but requests_kerberos '
                                  'module is not installed ', RuntimeWarning)
                    raise ESPError(res.reason)

        RESTHelpers.__init__(self, session=session)

        if float(self.server_info['version']) < 5.2:
            raise RuntimeError('This package requires an ESP server '
                               'version 5.2 or greater')

        self._populate_algorithms()

    def __str__(self):
        return '%s(%s)' % (type(self).__name__,
                           repr(self.base_url.replace('/%s/' % ESP_ROOT, '')))

    def __repr__(self):
        return str(self)

    def _enable_http_debugging(self):
        import logging
        import http.client
        http.client.HTTPConnection.debuglevel = 1
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger('requests.packages.urllib3')
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True

    def createServerConnection(self,**kwargs):
        return(api.connect(self.session,**kwargs))

    @property
    def authorization(self):
        authorization = None

        if self._kerberos:
            authorization = self._session.auth.generate_request_header(None,self._hostname,True)
        else:
            authorization = self._authorization

        return(authorization)

    @property
    def metadata(self):
        ''' Engine metadata '''
        return EngineMetadata(session=self.session)

    def create_project(self, proj, **kwargs):
        '''
        Create a new Project object

        Notes
        -----
        This method does not add the project to the server.  The
        returned object is client-side only until you install the project
        using :meth:`load_project`.

        Parameters
        ----------
        proj : string
            Either the name of the project, or a filename / URL
            pointing to a project definition
        kwargs : keyword arguments
            Optional parameters to the Project constructor.
            These are only used in the `project` argument is a
            project name.

        See Also
        --------
        :meth:`load_project`

        Returns
        -------
        :class:`Project`

        '''
        if os.path.isfile(proj) or re.match(r'\w+://', proj):
            out = project.Project.from_xml(get_project_data(proj))
        elif proj.startswith('<') and proj.endswith('>'):
            out = project.Project.from_xml(proj)
        else:
            out = project.Project(proj, **kwargs)
        out.session = self.session
        return out

    def _populate_algorithms(self):
        ''' Generate algorithm classes '''
        for alg in ['train', 'calculate', 'score']:
            doc = []
            doc.append('%s Algorithms' % alg.title())
            doc.append('-' * len(doc[-1]))
            members = {}
            algorithms = []
            for key, value in sorted(self.get_algorithms(alg, properties=True).items()):
                algorithms.append(key)
                members[str(key)] = self._new_algorithm(value,
                                                        get_window_class('window-%s' %
                                                                         alg))
                doc.append(key)
            members['__doc__'] = '\n'.join(doc)
            members['supported_algorithms'] = tuple(algorithms)
            setattr(self, alg,
                    type(str('%s%s' % (alg.title(), 'Algorithms')),
                         (object,), members))

    def _new_algorithm(self, data, wcls):
        '''
        Create a new algorithm window class

        Parameters
        ----------
        data : Algorithm
            The algorithm class populated with information from the server
        wcls : Window class
            The window class to use as the super class

        Returns
        -------
        :class:`Window` subclass

        '''
        _globals = globals()
        _locals = locals()

        has_input_map = bool(getattr(data, 'input_map', None))
        has_output_map = bool(getattr(data, 'output_map', None))

        params = ', '.join(['%s=%s' % (dekeywordify(name),
                                       repr(value.get('default', None)))
                            for name, value in sorted(data.parameters.items())])
        setters = [('self.parameters["%s"] = '
                    'self._cast_parameter(%s, dtype=%s, vartype=%s)')
                   % (dekeywordify(name),
                      dekeywordify(name),
                      repr(value.get('type')), repr(value.get('vartype')))
                   for name, value in sorted(data.parameters.items())]

        doc = [re.sub(r'([A-Z]\w+)', lambda x: ' ' + x.group(1), data.name).strip()]

        doc.append('')

        doc.append('Parameters')
        doc.append('----------')
        doc.append('name : string, optional')
        doc.append('    The name of the window')

        for name, value in sorted(data.parameters.items()):
            name = dekeywordify(name)
            desc = value.get('description', '').capitalize().strip()

            dtype = value.get('type', '')
            if dtype.endswith('-list'):
                dtype = 'list-of-%ss' % dtype.replace('-list', '')

            doc.append('%s : %s, optional' % (name, value['type']))

            if desc:
                doc.append('    %s' % '\n    '.join(textwrap.wrap(desc, 76)))

        if has_input_map:
            doc.append('input_map : dict, optional')
            doc.append('    The input variable map entries')

        if has_output_map:
            doc.append('output_map : dict, optional')
            doc.append('    The output variable map entries')

        doc.append('')

        doc.append('See Also')
        doc.append('--------')
        doc.append('set_inputs')
        doc.append('set_outputs')

        doc.append('')

        input_map_types = {}
        input_map_doc = []

        if wcls.__name__.lower().startswith('calc'):
            if params:
                params += ', index_type=None, produces_only_inserts=None'
            else:
                params = 'index_type=None, produces_only_inserts=None'

        if has_input_map:

            doc.append('Notes')
            doc.append('-----')
            doc.append('** Input Map Entries **')
            doc.append('')

            input_map_doc.extend(['Set the input map entries', '',
                                  'Parameters', '----------'])

            for key, value in sorted(data.input_map.items()):
                key = dekeywordify(key)
                desc = value.get('description', '').capitalize().strip()
                dtype = value.get('type', '')
                dtype = dtype.replace('varlist', 'list-of-strings')
                dtype = dtype.replace('var', 'string')
                vartype = value.get('vartype', '')

                if vartype:
                    input_map_types[key] = vartype

                docstring = '%s : %s' % (key, dtype)
                if value.get('default', '').strip():
                    docstring = '%s, optional (default: %s)' % \
                                (docstring, value['default'])

                doc.append(docstring)
                input_map_doc.append(docstring)

                if desc:
                    doc.append(
                        '    %s' % '\n    '.join(textwrap.wrap(desc, 76)))
                    input_map_doc.append(
                        '    %s' % '\n        '.join(textwrap.wrap(desc, 72)))

            doc.append('')
            input_map_doc.append('')

            if params:
                params += ', input_map=None'
            else:
                params = 'input_map=None'

        output_map_types = {}
        output_map_doc = []

        if has_output_map:

            if 'Notes' not in doc:
                doc.append('Notes')
                doc.append('-----')

            doc.append('** Output Map Entries **')
            doc.append('')

            output_map_doc.extend(['Set the output map entries', '',
                                   'Parameters', '----------'])

            for key, value in sorted(data.output_map.items()):
                key = dekeywordify(key)
                desc = value.get('description', '').capitalize().strip()
                dtype = value.get('type', '')
                dtype = dtype.replace('varlist', 'list-of-strings')
                dtype = dtype.replace('var', 'string')
                vartype = value.get('vartype', '')

                if vartype:
                    output_map_types[key] = vartype

                docstring = '%s : %s' % (key, dtype)
                if value.get('default', '').strip():
                    docstring = '%s, optional (default: %s)' % \
                                (docstring, value['default'])

                doc.append(docstring)
                output_map_doc.append(docstring)

                if desc:
                    doc.append(
                        '    %s' % '\n    '.join(textwrap.wrap(desc, 76)))
                    output_map_doc.append(
                        '    %s' % '\n        '.join(textwrap.wrap(desc, 72)))

            doc.append('')
            output_map_doc.append('')

            if params:
                params += ', output_map=None'
            else:
                params = 'output_map=None'

        schema = ''
        if not wcls.__name__.lower().startswith('train'):
            schema = ', schema=schema'

        copyvars = ''
        if wcls.__name__.lower().startswith('score'):
            copyvars = ', copyvars=copyvars'

        six.exec_('\n'.join([
            '''def __init__(self, name=None%s%s, pubsub=None, %s):''' %
                (schema.replace('=schema', '=None'),
                 copyvars.replace('=copyvars', '=None'), params),
            '''    from .windows import %s''' % wcls.__name__,
            '''    %s.__init__(self, name=name%s%s, pubsub=pubsub)''' %
                (wcls.__name__, schema, copyvars),
            '''    self.algorithm = %s''' % repr(data.name),
            has_input_map and '''    self.set_inputs(**(input_map or {}))''' or '',
            has_output_map and '''    self.set_outputs(**(output_map or {}))''' or '',
            '''    %s''' % '\n    '.join(setters),
        ]), _globals, _locals)

        # Setup input_map methods
        input_map = ', '.join(['%s=%s' % (dekeywordify(x),
                                          dekeywordify(x))
                               for x in sorted(data.input_map.keys())])
        if input_map:
            input_map = ', ' + input_map

        alg = ''
        if wcls.__name__.lower().startswith('score'):
            alg = ', self.algorithm'

        six.exec_('\n'.join([
            '''def set_inputs(self%s):''' % re.sub(r'=\w+', r'=None', input_map),
            '''    """''',
            '    ' + '\n    '.join(input_map_doc),
            '''    """''',
            '''    from .windows import %s''' % wcls.__name__,
            '''    return %s.set_inputs(self%s%s)''' % (wcls.__name__, alg, input_map),
        ]), _globals, _locals)

        # Setup output_map methods
        output_map = ', '.join(['%s=%s' % (dekeywordify(x),
                                           dekeywordify(x))
                                for x in sorted(data.output_map.keys())])
        if output_map:
            output_map = ', ' + output_map

        six.exec_('\n'.join([
            '''def set_outputs(self%s):''' % re.sub(r'=\w+', r'=None', output_map),
            '''    """''',
            '    ' + '\n    '.join(output_map_doc),
            '''    """''',
            '''    from .windows import %s''' % wcls.__name__,
            '''    return %s.set_outputs(self%s%s)''' % (wcls.__name__, alg, output_map),
        ]), _globals, _locals)

        cls_members = {
            '__doc__': '\n'.join(doc),
            '__init__': _locals['__init__'],
            'output_map_types': output_map_types,
            'input_map_types': input_map_types,
            'set_inputs': _locals['set_inputs'],
            'set_outputs': _locals['set_outputs'],
        }

        if wcls.__name__.lower().startswith('train'):
            cls_members.pop('set_outputs')

        return type(str(data.name), (wcls,), cls_members)

    def _projects_from_xml(self, data):
        ''' Create projects from XML content '''
        projects = dict()
        for item in data.findall('./project'):
            proj = project.Project.from_xml(item, session=self.session)
            projects[proj.name] = proj
        return projects

    def get_projects(self, name=None, filter=None):
        '''
        Retrieve projects from the server

        Parameters
        ----------
        name : string or list-of-strings, optional
            Names of the projects to return
        filter : string or list-of-strings, optional
            Functional filter which specifies projects to return

        Returns
        -------
        dict of :class:`Project`

        '''
        out = self._get('projectXml', params=get_params(name=name, filter=filter))
        return self._projects_from_xml(out)

    def get_project(self, name):
        '''
        Retrieve the specified project

        Parameters
        ----------
        name : string
            Name of the project

        Returns
        -------
        :class:`Project`

        '''
        out = self.get_projects(name=name)
        try:
            out = out[name]
            out._load_metadata()
            return out
        except KeyError:
            raise KeyError("No project with name '%s' found." % name)

    def load_project(self, project, name=None, overwrite=True, start=True,
                     start_connectors=True):
        '''
        Load a project from a project definition

        Parameters
        ----------
        project : string or Project or file-like
            If a Project object, the project is exported to an XML definition
            then loaded.  If it is a string containing a URL, the URL must
            contain an XML definiton of a project.  Any other string or
            file-like object, must contain XML defining a project.
        name : string, optional
            Name of the project.  By default, the name is retrieved from
            the XML definition.
        overwrite : bool, optional
            Should an existing project with the same name be overwritten?
        start : bool, optional
            Should the project be started?
        start_connectors : bool, optional
            Should the connectors be started?

        Returns
        -------
        :class:`Project`

        '''
        if isinstance(project, six.string_types) and re.match(r'\w+://', project):
            data = ''
            project_url = project
        else:
            data = get_project_data(project)
            project_url = None

        if name is None:
            if data:
                proj = xml.from_xml(data)
                if proj.tag == 'project':
                    name = proj.attrib.get('name')
                else:
                    name = proj.findall('.//project')[0].attrib.get('name')
            if name is None:
                name = gen_name(prefix='p_')

        #if sys.platform != "win32":
            #data = data.encode("utf-8")
        data = data.encode("utf-8")

        self._put('projects/%s' % name,
                  params=get_params(overwrite=overwrite,
                                    connectors=start_connectors,
                                    projectUrl=project_url,
                                    start=start,
                                    log=True),
                  data=data)

        return self.get_project(name)

    install_project = load_project

    def delete_projects(self, *name, **kwargs):
        '''
        Delete projects

        Parameters
        ----------
        *name : zero-or-more-strings, optional
            Names of the projects to delete
        filter : string or list-of-strings, optional
            Functional filter indicating the projects to delete

        '''
        self._delete('projects', params=get_params(name=list(name),
                                                   filter=kwargs.get('filter')))

    def delete_project(self, name):
        '''
        Delete specified project

        Parameters
        ----------
        name : string
            Name of the project to delete

        '''
        self._delete('projects/%s' % name)

    def get_running_projects(self, name=None, filter=None):
        '''
        Retrieve running project information

        Parameters
        ----------
        name : string or list-of-strings, optional
            Names of the running projects to return
        filter : string or list-of-strings, optional
            Functional filters indicating the running projects to return

        Returns
        -------
        dict of :class:`Project`

        '''
        out = self._get('runningProjects', params=get_params(name=name,
                                                             filter=filter,
                                                             schema=False))
        names = list(self._projects_from_xml(out).keys())
        if names:
            return self.get_projects(name=names)

        return {}

    def get_running_project(self, name):
        '''
        Retrieve the specified running project

        Parameters
        ----------
        name : string
            Name of the project

        Returns
        -------
        :class:`Project`

        '''
        out = self.get_running_projects(name=name)
        if out:
            return out.popitem()[-1]
        raise KeyError("No running project with name '%s' found." % name)

    def start_projects(self, name=None, filter=None):
        '''
        Start projects

        Parameters
        ----------
        name : string or list-of-strings, optional
            Names of projects to start
        filter : string or list-of-strings, optional
            Functional filters indicating which projects to start

        '''
        self._post('runningProjects', params=get_params(name=name, filter=filter))

    def start_project(self, name):
        '''
        Start specified project

        Parameters
        ----------
        name : string
            Name of the project to start

        '''
        self._post('runningProjects/%s' % name, params=get_params(name=name))

    def get_stopped_projects(self, name=None, filter=None):
        '''
        Retrieve stopped projects

        Parameters
        ----------
        name : string or list-of-strings, optional
            Names of stopped projects to return
        filter : string or list-of-strings, optional
            Functional filters indicating stopped projects to return

        Returns
        -------
        dict of :class:`Project`

        '''
        out = self._get('stoppedProjects', params=get_params(name=name,
                                                             filter=filter,
                                                             schema=False))
        names = list(self._projects_from_xml(out).keys())
        if names:
            return self.get_projects(name=names)

        return {}

    def get_stopped_project(self, name):
        '''
        Retrieve specified stopped project

        Parameters
        ----------
        name : string
            Name of the stopped project

        Returns
        -------
        :class:`Project`

        '''
        out = self.get_stopped_projects(name=name)
        if out:
            return out.popitem()[-1]
        raise KeyError("No stopped project with name '%s' found." % name)

    def stop_projects(self, name=None, filter=None):
        '''
        Stop projects

        Parameters
        ----------
        name : string or list-of-strings, optional
            Names of the projects to stop
        filter : string or list-of-strings, optional
            Functional filters indicating which projects to stop

        '''
        self._post('stoppedProjects', params=get_params(name=name, filter=filter))

    def stop_project(self, name):
        '''
        Stop specified project

        Parameters
        ----------
        name : string
            Name of the project to stop

        '''
        self._post('stoppedProjects/%s' % name, params=get_params(name=name))

    def get_windows(self, path='*.*.*', type=None, filter=None):
        '''
        Retrieve windows from the server

        Parameters
        ----------
        path : string, optional
            '.' delimited path to windows to retrieve.  '*' can be used as
            a wildcard for any component.  Multiple component names can
            be delimited by '|'.
        type : string or list-of-strings, optional
            Types of windows you want to retrieve
        filter : string or list-of-strings, optional
            Function filter indicating which windows to retrieve

        Returns
        -------
        dict of :class:`Window`

        '''
        path = [None, None, None] + expand_path(path)
        res = self._get('windowXml', params=get_params(project=path[-3],
                                                       contquery=path[-2],
                                                       name=path[-1], type=type,
                                                       filter=filter))
        windows = dict()
        for item in res.findall('./*'):
            try:
                wcls = get_window_class(item.tag)
            except KeyError:
                raise TypeError('Unknown window type: %s' % item.tag)
            window = wcls.from_xml(item, session=self.session)
            windows[window.fullname] = window
        return windows

    def get_window(self, path):
        '''
        Retrieve specified window

        Parameters
        ----------
        path : string, optional
            '.' delimited path to windows to retrieve.  '*' can be used as
            a wildcard for any component.  Multiple component names can
            be delimited by '|'.

        Returns
        -------
        :class:`Window`

        '''
        out = self.get_windows(path)
        if len(out) > 1:
            raise ValueError(("More than one window with the path '%s' exists. " % path) +
                             'Use a more explicit path or use the get_windows method.')
        if out:
            return out.popitem()[1]
        raise KeyError("No window with the path '%s'" % path)

    def get_events(self, window_filter=None, event_filter=None, sort_by=None, limit=None):
        '''
        Retrieve events from the server

        Parameters
        ----------
        window_filter : string or list-of-strings, optional
            Functional filter indicating windows of which you want the
            events for
        event_filter : string or list-of-strings, optional
            Functional filter indicating events to retrieve
        sort_by : string, optional
            The field to sort results on.  It should be of the
            form: ``sortfield:ascending``.  Where the ordering can be
            ``ascending`` or ``descending``.
        limit : int, optional
            Maximum number of events to return

        Returns
        -------
        dict of :class:`pandas.DataFrame`

        '''
        return get_events(self, self._get('events',
                                          params=get_params(window_filter=window_filter,
                                                            event_filter=event_filter,
                                                            sort_by=sort_by,
                                                            limit=limit)))

    def get_pattern_events(self, window_filter=None, event_filter=None,
                           sort_by=None, limit=None):
        '''
        Retrieve events residing in open patterns in the server

        Parameters
        ----------
        window_filter : string or list-of-strings, optional
            Functional filter indicating windows of which you want the
            events for
        event_filter : string or list-of-strings, optional
            Functional filter indicating events to retrieve
        sort_by : string, optional
            The field to sort results on.  It should be of the
            form: ``sortfield:ascending``.  Where the ordering can be
            ``ascending`` or ``descending``.
        limit : int, optional
            Maximum number of events to return

        Returns
        -------
        dict of :class:`pandas.DataFrame`

        '''
        return get_events(self, self._get('patternEvents',
                                          params=get_params(window_filter=window_filter,
                                                            event_filter=event_filter,
                                                            sort_by=sort_by,
                                                            limit=limit)))

    @property
    def server_info(self):
        ''' Retrive current status of the server '''
        return get_server_info(self)

    def save(self, path=None):
        '''
        Save the server state

        Parameters
        ----------
        path : string, optional
            Location of the server data

        '''
        self._put('server/state', params=get_params(value='persisted', path=path))

    def reload(self):
        ''' Reset the server model to the model used at startup '''
        self._put('server/state', params=get_params(value='reloaded'))

    def shutdown(self):
        ''' Shutdown the server '''
        self._put('server/state', params=get_params(value='stopped'))

    def get_loggers(self):
        '''
        Retrieve all loggers in the system

        Returns
        -------
        dict of :class:`Logger`

        '''
        loggers = dict()
        out = self._get('loggers')
        for item in out.findall('./logger'):
            logger = Logger(**item.attrib)
            logger.session = self.session
            loggers[logger.name] = logger
        return loggers

    def get_logger(self, name):
        '''
        Retrieve the specified logger

        Parameters
        ----------
        name : string
            Name of the logger

        Returns
        -------
        :class:`Logger`

        '''
        try:
            logger = None

            out = self._get('loggers/%s' % name)
            node = out.find("logger")
            if node != None:
                logger = Logger(**node.attrib)
                logger.session = self.session

            return logger

        except ESPError:
            raise KeyError("No logger with the name '%s' exists." % name)

    @property
    def connector_info(self):
        '''
        Return the connection metadata for all connectors

        Returns
        -------
        dict

        '''
        out = {}
        info = self._get('connectorInfo')
        if info.tag == 'connector':
            conn = ConnectorInfo.from_xml(info, session=self.session)
            out[conn.label] = conn
        else:
            for connector in info.findall('./connector'):
                conn = ConnectorInfo.from_xml(connector, session=self.session)
                out[conn.label] = conn
        return out

    def _run_project(self, project, windows=None):
        '''
        Run a project and send back window contents

        Parameters
        ----------
        project : Project or string
            The project definition, project XML, or URL of project definition
        windows : string or list-of-strings, optional
            Names of the windows whose contents to include

        Returns
        -------
        dict

        '''
        if isinstance(project, six.string_types) and re.match(r'\w+://', project):
            data = ''
            project_url = project
        else:
            data = get_project_data(project)
            project_url = None

        return get_events(self, self._post('projectResults',
                                            params=get_params(windows=windows,
                                                              projectUrl=project_url),
                                            data=data))

    def validate_project(self, project):
        '''
        Validate project definition

        Parameters
        ----------
        project : string
            XML content or location of the project definition

        Returns
        -------
        True
            If project is valid
        False
            If project is not valid

        '''
        try:
            res = self._post('projectValidationResults', data=get_project_data(project).encode("utf-8"))
            if res.tag != 'project-validation-success':
                return False
        except ESPError:
            return False
        return True

    def get_mas_modules(self, expandcode=False):
        '''
        Retrieve all MAS modules

        Parameters
        ----------
        expandcode : bool, optional
            Should code-file elements be returned as code elements containing
            the contents of the file?

        Returns
        -------
        dict of :class:`MASModule`

        '''
        res = self._get('masModules', params=get_params(expandcode=expandcode))
        out = {}
        for elem in res.findall('./project'):
            proj = elem.attrib['name']
            modules = []
            for mod in elem.findall('./mas-module'):
                mod = MASModule.from_xml(mod, session=self.session)
                mod.project = proj
                modules.append(mod)
            out[proj] = modules
        return out

    def get_routers(self):
        '''
        Retrieve all routers in the server

        Returns
        -------
        dict of :class:`Router`

        '''
        res = self._get('routers')
        out = {}
        for elem in res.findall('./esp-router'):
            rte = Router.from_xml(elem, session=self.session)
            out[rte.name] = rte
        return out

    def get_router(self, name):
        '''
        Retrieve the specified router

        Parameters
        ----------
        name : string
            Name of the router

        Returns
        -------
        :class:`Router`

        '''
        return Router.from_xml(self._get('routers/%s' % name),
                               session=self.session)

    def get_router_stats(self, name=None):
        '''
        Retrieve router statistics

        Parameters
        ----------
        name : string, optional
            Name of the router to retrieve statistics for

        Returns
        -------
        dict

        '''
        if name:
            stats = self._get('routerStats/%s' % name)
            elems = [stats]
        else:
            stats = self._get('routerStats')
            elems = stats.findall('./esp-router')

        def cast_numeric(val):
            ''' Cast numeric looking values to numerics '''
            if re.match(r'^\d+$', val):
                return int(val)
            elif re.match(r'^[\d\.]+$', val):
                return float(val)
            return val

        out = {}

        for elem in elems:
            routes = {}
            for route in elem.findall('./route'):
                rte = {}
                for stat in route.findall('./stats/*'):
                    rte[stat.tag] = cast_numeric(stat.text)
                routes[route.attrib['name']] = rte
            out[elem.attrib['name']] = routes

        if name:
            return out[name]

        return out

    def create_router(self, name=None, overwrite=True):
        '''
        Create a new router

        Parameters
        ----------
        router : string
            The router definition
        name : string, optional
            The name of the router to create
        overwrite : bool, optional
            Should an existing router of the same name be overwritten?

        Returns
        -------
        :class:`Router`

        '''
        rte = Router(name=name)
        rte.session = self.session
        return rte

    def get_event_generators(self):
        '''
        Retrieve all event generators in the server

        Returns
        -------
        dict of :class:`EventGenerator`

        '''
        res = self._get('eventGenerators')
        out = {}
        for elem in res.findall('./event-generator'):
            evtg = EventGenerator.from_xml(elem, session=self.session)
            out[evtg.name] = evtg
        return out

    def get_event_generator_state(self, name):
        '''
        Retrieve the state of the specified event generator

        Parameters
        ----------
        name : string
            Name of the event generator

        Returns
        -------
        string

        '''
        return self._get('eventGenerators/%s' % name).attrib['state']

    def create_event_generator(self, window, data=None, name=None, overwrite=False):
        '''
        Create an event generator

        Parameters
        ----------
        window : Window
            The window to inject into
        data : CSV or URL or DataFrame, optional
            The events to inject
        name : string, optional
            The name of the event generator.  If no name is given,
            a name will be generated.
        overwrite : bool, optional
            Should an existing event generator with the same name
            be overwritten?

        Returns
        -------
        :class:`EventGenerator`

        '''
        gen = EventGenerator(name=name)
        gen.session = self.session
        gen.publish_target = window
        if data is not None:
            gen.event_data = data
            gen.save(overwrite=overwrite)
        return gen

    def delete_event_generators(self):
        ''' Delete all event generators in the server '''
        for key in self.get_event_generators():
            self.delete_event_generator(key)

    def delete_event_generator(self, name):
        '''
        Delete the specified event generator

        Parameters
        ----------
        name : string
            Name of the event generator to delete

        '''
        self._delete('eventGenerators/%s' % name)

    @property
    def api_docs(self):
        ''' Swagger JSON documentation '''
        return self._get('api-docs?format=json', format='json', headers={'accept':'application/json'})

    def get_algorithms(self, atype, properties=False, type=None, reference=None):
        '''
        Retrieve information for all algorithms of the specified type

        Parameters
        ----------
        atype : string
            Type of the algorithm: train, calculate, or score
        properties : bool, optional
            Should algorithm properties be returned?
        type : string, optional
            Set to 'astore' if astore information is desired
        reference : string, optional
            Set to the value of an astore reference file

        Returns
        -------
        dict of :class:`Algorithm`

        '''
        res = self._get('algorithms/%s' % atype,
                        params=get_params(properties=properties,
                                          type=type,
                                          reference=reference))
        out = {}
        if res.attrib.get('reference', None):
            alg = Algorithm.from_xml(res)
            out[alg.reference] = alg
        else:
            for item in res.findall('./algorithm'):
                alg = Algorithm.from_xml(item)
                out[alg.name] = alg
        return out

    def get_algorithm(self, atype, name):
        '''
        Retrieve information for the specified algorithm

        Parameters
        ----------
        atype : string
            Type of the algorithm: train, calculate, or score
        name : string
            Name of the algorithm

        Returns
        -------
        :class:`Algorithm`

        '''
        return Algorithm.from_xml(self._get('algorithms/%s/%s' %
                                            (atype, name)).find('./algorithm'))

    def get_project_stats(self, filter=None, interval=None, min_cpu=None, limit=20):
        '''
        Project statistics subscriber

        Parameters
        ----------
        filter : string, optional
            Functional filter to match projects.
        interval : int, optional
            The interval in seconds between updates
        min_cpu : int, optional
            The minimum CPU value you want included
        limit : int, optional
            The maximum number of rows to retain in the DataFrame

        Returns
        -------
        :class:`ProjectStats`

        '''
        proj_stats = ProjectStats(self, filter=filter, interval=interval,
                                  min_cpu=min_cpu, limit=limit)
        proj_stats.start()
        return proj_stats

    def enable_server_log_capture(self):
        ''' Enable capturing of server log messages '''
        self._put('logCapture/state', params=get_params(value='on'))

    def disable_server_log_capture(self):
        ''' Disable capturing of server log messages '''
        self._put('logCapture/state', params=get_params(value='off'))

    def get_server_log_state(self):
        '''
        Return the current state of the server log capture

        Returns
        -------
        string

        '''
        return self._get('logCapture').attrib['logcapture'] == 'on'

    def get_server_log(self):
        '''
        Return all stored server log messages

        Returns
        -------
        list of strings

        '''
        return self._get('logs', raw=True).rstrip().split('\n')


def get_subclasses(cls):
    for subclass in cls.__subclasses__():
        for subcls in get_subclasses(subclass):
            yield subcls
        yield subclass


for cls in get_subclasses(windows.BaseWindow):
    setattr(ESP, cls.__name__, cls)
