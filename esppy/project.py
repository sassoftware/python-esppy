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

''' ESP Project '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import six
import threading
import xml.etree.ElementTree as ET
from six.moves import urllib
from .base import ESPObject, attribute
from .config import get_option
from .contquery import ContinuousQuery
from .exceptions import ESPError
from .mas import MASModule
from .windows import get_window_class
from .utils.rest import get_params
from .utils.data import get_project_data, gen_name
from .utils.events import get_events
from .utils.notebook import scale_svg
from .utils.project import expand_path
from .utils import xml


class ContinuousQueryDict(collections.abc.MutableMapping):
    '''
    Dictionary for holding continuous query objects

    Attributes
    ----------
    project : string
        The name of the project
    session : requests.Session
        The session for the continuous queries

    Parameters
    ----------
    *args : one-or-more arguments, optional
        Positional arguments to MutableMapping
    **kwargs : keyword arguments, optional
        Keyword arguments to MutableMapping

    '''

    def __init__(self, *args, **kwargs):
        collections.abc.MutableMapping.__init__(self, *args, **kwargs)
        self._data = dict()
        self.project = None
        self.session = None

    @property
    def project(self):
        '''
        The project that queries are associated with

        Returns
        -------
        string

        '''
        return self._project

    @project.setter
    def project(self, value):
        self._project = getattr(value, 'name', value)
        for item in self._data.values():
            item.project = self._project

    @property
    def session(self):
        '''
        The session for the queries

        Returns
        -------
        string

        '''
        return self._session

    @session.setter
    def session(self, value):
        self._session = value
        for item in self._data.values():
            item.session = self._session

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        if not isinstance(value, ContinuousQuery):
            raise TypeError('Only ContinuousQuery objects can be '
                            'values in a Project')
        value.name = key
        value.project = self.project
        value.session = self.session
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return repr(self._data)


class ConnectorGroup(object):
    '''
    Project connector group

    Attributes
    ----------
    entries : dict
        Collection of connectors

    Parameters
    ----------
    name : string
        The name of the group
    description : string, optional
        A description of the group

    Returns
    -------
    :class:`ConnectorGroup`

    '''

    def __init__(self, name, description=None):
        self.name = name
        self.description = description
        self.entries = {}

    def add_entry(self, connector, state):
        self.entries[connector] = state

    @classmethod
    def from_element(cls, data, session=None):
        if isinstance(data, six.string_types):
            data = xml.from_xml(data)

        out = cls(data.attrib['name'])

        for item in data.findall('./description'):
            out.description = item.text

        for item in data.findall('./connector-entry'):
            out.entries[item.attrib['connector']] = item.attrib['state']

        return out

    from_xml = from_element

    def to_element(self):
        out = xml.new_elem('connector-group', attrib=dict(name=self.name))

        if self.description:
            xml.add_elem(out, 'description', text_content=self.description)

        for key, value in six.iteritems(self.entries):
            xml.add_elem(out, 'connector-entry',
                         attrib=dict(connector=key, state=value))

        return out

    def to_xml(self, pretty=False):
        return xml.to_xml(self.to_element(), pretty=pretty)


class Edge(object):

    def __init__(self, source, target):
        self.source = source
        if isinstance(target, six.string_types):
            self.target = re.split(r'\s*,\s*', target)
        else:
            self.target = list(target)

    @classmethod
    def from_element(cls, data, session=None):
        return cls(data.attrib['source'], data.attrib['target'])

    def to_element(self):
        return xml.new_elem('edge',
                            attrib=dict(source=self.source,
                                        target=','.join(self.target)))


class Project(ESPObject, collections.abc.MutableMapping):
    '''
    ESP Project

    Parameters
    ----------
    name : string
        Name of the project
    n_threads : int, optional
        A positive integer for the number of threads to use from the
        available thread pool
    pubsub : string, optional
        Publish/subscribe mode for the project
    compress_open_patterns : bool, optional
        When True, compress events stored in unresolved pattern instances
    port : int, optional
        The project-level publish/subscribe port for auto or manual mode
    index_type : string, optional
        A default index type for all windows in the project that do not
        explicitly specify an index type
        Valid values: 'rbtree', 'hash', 'ln_hash', 'cl_hash', 'fw_hash', 'empty'
    use_tagged_token : bool, optional
        Specify whether tagged token data flow semantics should be used
        for continuous queries
    use_retention_tracking : bool, optional
        Specify whether to use retention tracking for the project.
        This allows two events per key in output event blocks.
    disk_store_path : string, optional
        Specify the path for on-disk event storage when using hleveldb indices
    heartbeat_interval : int, optional
        Specify how frequently the system sends timer heartbeats to the project.
        The default value is 1 second. This heartbeat drives wall-clock-based
        retention and pattern time outs. When a model has an exceptionally large
        number of unresolved pattern instances, increasing the value of the
        heartbeat interval can optimize how the model runs.
    restore : string, optional
        Specify a directory from which to restore the project from a
        previously-performed persist operation.
    description : string, optional
        Description of the project
    metadata : dict, optional
        Metadata associated with the project

    Attributes
    ----------
    queries : dict
        Dictionary of the queries
    metadata : dict
        Metadata associated with the project
    url : string
        The URL of the project

    Notes
    -----
    All parameters to the constructor are also available as attributes.

    Returns
    -------
    :class:`Project`

    '''

    default_query = 'contquery'

    n_threads = attribute('threads', dtype='int')
    pubsub = attribute('pubsub', dtype='string', values=['none', 'auto', 'manual'])
    port = attribute('port', dtype='int')
    index_type = attribute('index', dtype='string',
                           values={'rbtree': 'pi_RBTREE', 'hash': 'pi_HASH',
                                   'ln_hash': 'pi_LN_HASH', 'cl_hash': 'pi_CL_HASH',
                                   'fw_hash': 'pi_FW_HASH', 'empty': 'pi_EMPTY'})
    name = attribute('name', dtype='string')
    use_tagged_token = attribute('use-tagged-token', dtype='bool')
    use_retention_tracking = attribute('retention-tracking', dtype='bool')
    restore = attribute('restore', dtype='string')
    disk_store_path = attribute('disk-store-path', dtype='string')
    heartbeat_interval = attribute('heartbeat-interval', dtype='int')
    compress_open_patterns = attribute('compress-open-patterns', dtype='bool')

    def __init__(self, name=None, n_threads=1, pubsub='auto',
                 compress_open_patterns=None, port=None, index_type=None,
                 use_tagged_token=None, use_retention_tracking=None,
                 disk_store_path=None, heartbeat_interval=None, restore=None,
                 description=None, sas_log_location=None, sas_connection_key=None,
                 sas_command=None, metadata=None):
        self.queries = ContinuousQueryDict()
        ESPObject.__init__(self, attrs=locals())
        self.name = name or gen_name(prefix='p_')
        self.description = description
        self.sas_log_location = sas_log_location
        self.sas_connection_key = sas_connection_key
        self.sas_command = sas_command
        self.mas_modules = []
        self.connector_groups = {}
        self.edges = []
        self.metadata = dict(metadata or {})

    @property
    def session(self):
        '''
        The requests.Session object for the project

        Returns
        -------
        string

        '''
        return ESPObject.session.fget(self)

    @session.setter
    def session(self, value):
        ESPObject.session.fset(self, value)
        self.queries.session = value

    @property
    def name(self):
        '''
        The name of the project

        Returns
        -------
        string

        '''
        return self._name

    @name.setter
    def name(self, value):
        '''
        Set the name of the project and propagate to sub-objects

        Parameters
        ----------
        value : string
            The name of the project

        '''
        self._name = value
        self.queries.project = self.name

    @property
    def windows(self):
        '''
        Return collection of windows from the default query

        Returns
        -------
        dict

        '''
        if self.default_query not in self.queries:
            self.queries[self.default_query] = ContinuousQuery()
            self.queries[self.default_query].windows.project_handle = self
        return self.queries[self.default_query].windows

    @property
    def templates(self):
        '''
        Return collection of templates from the default query

        Returns
        -------
        dict

        '''
        if self.default_query not in self.queries:
            self.queries[self.default_query] = ContinuousQuery()
            self.queries[self.default_query].templates.project_handle = self
        return self.queries[self.default_query].templates

    @property
    def trace(self):
        '''
        Set the trace property on the default continuous query

        Returns
        -------
        string

        '''
        if self.default_query not in self.queries:
            self.queries[self.default_query] = ContinuousQuery()
            self.queries[self.default_query].windows.project_handle = self
        return self.queries[self.default_query].trace

    @trace.setter
    def trace(self, value):
        '''
        Set the trace property on the default continuous query

        Parameters
        ----------
        value : string
            The names of the windows to trace

        '''
        if self.default_query not in self.queries:
            self.queries[self.default_query] = ContinuousQuery()
            self.queries[self.default_query].windows.project_handle = self
        self.queries[self.default_query].trace = value

    def sync(self, overwrite=True, start=True, start_connectors=True):
        '''
        Sync the project definition to the server

        Notes
        -----
        This will overwrite the project definition in the server.

        Parameters
        ----------
        overwrite : bool, optional
            Should an existing project with the same name be overwritten?
        start : bool, optional
            Should the project be started?
        start_connectors : bool, optional
            Should the connectors be started?

        '''
        self._put(params=get_params(overwrite=overwrite,
                                    connectors=start_connectors,
                                    start=start),
                  data=get_project_data(self).encode())

    def copy(self, deep=False):
        '''
        Return a copy of the object

        Parameters
        ----------
        deep : bool, optional
            Should sub-objects be copied as well?

        Returns
        -------
        :class:`Project`

        '''
        out = type(self)()

        out.session = self.session

        for key, value in self._get_attributes(use_xml_values=False).items():
            setattr(out, key, value)

        if deep:
            out.queries = dict([(k, v.copy(deep=True))
                                for k, v in self.queries.items()])
        else:
            out.queries.update(self.queries)

        return out

    def __copy__(self):
        return self.copy(deep=False)

    def __deepcopy__(self, memo):
        return self.copy(deep=True)

    @property
    def fullname(self):
        return self.name

    @property
    def url(self):
        '''
        URL of project

        Returns
        -------
        string

        '''
        return urllib.parse.urljoin(self.base_url, 'projects/%s/' % self.name)

    @classmethod
    def from_element(cls, data, session=None):
        '''
        Create project from XML definition

        Parameters
        ----------
        data : xml-string or ElementTree.Element
            XML project definition
        session : requests.Session, optional
            Session that the project is associated with

        Returns
        -------
        :class:`Project`

        '''
        out = cls()
        out.session = session

        if isinstance(data, six.string_types):
            data = xml.from_xml(get_project_data(data))

        if data.tag != 'project':
            data = data.find('.//project')

        if data is None:
            raise ValueError('No project found in input.')

        out._set_attributes(data.attrib)

        for desc in data.findall('./description'):
            out.description = desc.text

        for item in data.findall('./ds-initialize'):
            out.sas_log_location = item.attrib.get('sas-log-location')
            out.sas_connection_key = item.attrib.get('sas-connection-key')
            out.sas_command = item.attrib.get('sas-command')

        for item in data.findall('./mas-modules/mas-module'):
            out.mas_modules.append(MASModule.from_xml(item, session=session))

        for item in data.findall('./project-connectors/connector-groups/connector-group'):
            grp = ConnectorGroup.from_element(item, session=session)
            out.connector_groups[grp.name] = grp

        for item in data.findall('./project-connectors/edges/edge'):
            out.edges.append(Edge.from_element(item, session=session))

        for contquery in data.findall('.//contquery'):
            query = ContinuousQuery.from_xml(contquery, project=out,
                                             session=session)
            out.queries[query.name] = query

        for item in data.findall('./metadata/meta'):
            if 'id' in item.attrib.keys():
                out.metadata[item.attrib['id']] = item.text
            elif 'name' in item.attrib.keys():
                out.metadata[item.attrib['name']] = item.text

        return out

    from_xml = from_element

    def _load_metadata(self):
        data = self._get(urllib.parse.urljoin(self.base_url,
                                              'projectMetadata/%s' % self.name))
        for item in data.findall('./project/metadata/meta'):
            self.metadata[item.attrib['id']] = item.text
        for cq in data.findall('./project/contquery'):
            name = cq.attrib['id']
            if name in self.queries:
                for item in cq.findall('./metadata/meta'):
                    self.queries[name].metadata[item.attrib['id']] = item.text

    def _persist_metadata(self):
        if self.metadata:
            self._set_metadata(self.metadata)
        for cq in six.itervalues(self.queries):
            if cq.metadata:
                cq._set_metadata(cq.metadata)

    def _clear_metadata(self):
        self.metadata.clear()
        for cq in six.itervalues(self.queries):
            cq._clear_metadata()

    def _get_metadata(self):
        out = {}
        for item in self._get(urllib.parse.urljoin(self.base_url,
                                                   'projectMetadata/%s' % self.name)
                             ).findall('./project/metadata/meta'):
            out[item.attrib['id']] = item.text
        return out

    def _set_metadata(self, data):
        for key, value in six.iteritems(data):
            self._put(urllib.parse.urljoin(self.base_url,
                                           'projectMetadata/%s/%s' % (self.name, key)),
                      data='%s' % value)

    def _del_metadata(self, *data):
        for key in data:
            self._delete(urllib.parse.urljoin(self.base_url,
                                              'projectMetadata/%s/%s' % (self.name, key)))

    def to_element(self):
        '''
        Export project definition to an ElementTree.Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('engine')
        projects = xml.add_elem(out, 'projects')

        proj = xml.add_elem(projects, 'project', xml.get_attrs(self, extra='name'))

        if self.description:
            xml.add_elem(proj, 'description', text_content=self.description)

        if self.metadata:
            metadata = xml.add_elem(proj, 'metadata')
            for key, value in sorted(six.iteritems(self.metadata)):
                xml.add_elem(metadata, 'meta', attrib=dict(id=key),
                             text_content=value)

        if self.sas_log_location or self.sas_connection_key or self.sas_command:
            xml.add_elem(proj, 'ds-initialize',
                         attrib=dict(sas_log_location=self.sas_log_location,
                                     sas_connection_key=self.sas_connection_key,
                                     sas_command=self.sas_command))

        if self.mas_modules:
            mods = xml.add_elem(proj, 'mas-modules')
            for item in self.mas_modules:
                xml.add_elem(mods, item.to_element())

        queries = xml.add_elem(proj, 'contqueries')

        for name, query in sorted(six.iteritems(self.queries)):
            xml.add_elem(queries, query.to_element())

        if self.connector_groups or self.edges:
            conns = xml.add_elem(proj, 'project-connectors')

            if self.connector_groups:
                grps = xml.add_elem(conns, 'connector-groups')
                for name, value in sorted(six.iteritems(self.connector_groups)):
                    xml.add_elem(grps, value.to_element())

            if self.edges:
                edges = xml.add_elem(conns, 'edges')
                for item in self.edges:
                    xml.add_elem(edges, item.to_element())

        return out

    def to_xml(self, pretty=False):
        '''
        Export project definition to XML

        Parameters
        ----------
        pretty : bool, optional
            Should the XML include whitespace for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)

    def save_xml(self, dest, mode='w', pretty=True, **kwargs):
        '''
        Save the project XML to a file

        Parameters
        ----------
        dest : string or file-like
            The destination of the XML content
        mode : string, optional
            The write mode for the output file (only used if `dest` is a string)
        pretty : boolean, optional
            Should the XML include whitespace for readability?

        '''
        if isinstance(dest, six.string_types):
            with open(dest, mode=mode, **kwargs) as output:
                output.write(self.to_xml(pretty=pretty))
        else:
            dest.write(self.to_xml(pretty=pretty))

    def to_graph(self, graph=None, schema=False, template_detail=False):
        '''
        Export project definition to graphviz.Digraph

        Parameters
        ----------
        graph : graphviz.Digraph, optional
            The parent graph to add to
        schema : bool, optional
            Should window schemas be included?
        template_detail : bool, optional
            Should template detail be shown?

        Returns
        -------
        :class:`graphviz.Digraph`

        '''
        try:
            import graphviz as gv
        except ImportError:
            raise ImportError('The graphviz module is required for exporting to graphs.')

        if graph is None:
            graph = gv.Digraph(format='svg')
            graph.attr('node', shape='rect', fontname='helvetica')
            graph.attr('graph', rankdir='LR', center='false')
            graph.attr('edge', fontname='times-italic')

        if self.queries:
            pgraph = gv.Digraph(format='svg',
                                name='cluster_%s' % self.fullname.replace('.', '_'))
            pgraph.attr('graph', fontname='helvetica')
            pgraph.attr('edge', fontname='times-italic')
            pgraph.attr(label=self.name, labeljust='l',
                        style='filled,bold,rounded', color='#c0c0c0',
                        fillcolor='#dadada', fontcolor='black')

            if list(self.queries.keys()) == [self.default_query]:
                if self.templates:
                    for tkey, template in sorted(self.templates.items()):
                        pgraph.subgraph(template.to_graph(schema=schema, detail=template_detail))

                for wkey, win in sorted(self.windows.items()):
                    if not win.template:
                        pgraph.subgraph(win.to_graph(schema=schema))
                    for target in win.targets:
                        if target.name not in self.windows:
                            continue
                        tail_name = win.template.fullname if win.template and not template_detail else win.fullname
                        head_name = target.template.fullname if target.template and not template_detail \
                            else self.windows[target.name].fullname
                        if win.template and target.template and win.template == target.template:
                            continue
                        graph.edge(tail_name, head_name, label=target.role or '')
            else:
                for qkey, query in sorted(self.queries.items()):
                    pgraph.subgraph(query.to_graph(schema=schema, template_detail=template_detail))

            graph.subgraph(pgraph)

        else:
            graph.node(self.fullname, label=self.name, labeljust='l',
                       style='filled,bold,rounded', color='#c0c0c0',
                       fillcolor='#dadada', fontcolor='black')

        return graph

    def _repr_svg_(self):
        try:
            return scale_svg(self.to_graph()._repr_svg_())
        except ImportError:
            raise AttributeError('_repr_svg_')

    def __str__(self):
        return '%s(name=%s)' % (type(self).__name__, repr(self.name))

    def __repr__(self):
        return str(self)

    def start(self):
        ''' Start the project '''
        self._put('state', params=dict(value='running'))

    def stop(self):
        ''' Stop the project '''
        self._put('state', params=dict(value='stopped'))

    def start_connectors(self):
        ''' Start the connectors for the project '''
        self._put('state', params=dict(value='connectorsStarted'))

    def add_connectors(self, group_name, connectors, group_description=None):
        '''
        Add connectors to a group

        Parameters
        ----------
        group_name : string
            The name of the connector group
        connectors : dict
            Key / value pairs of connector names and states
            ('finished', 'running', 'stopped')
        group_description : string, optional
            A description of the group

        Notes
        -----
        If the connector group does not exist yet, one is created.
        If the connector group does exist, the connectors will be
        added to it.

        '''
        if group_name not in self.connector_groups:
            self.connector_groups[group_name] = ConnectorGroup(group_name,
                                                    description=group_description)

        group = self.connector_groups[group_name]

        for key, value in six.iteritems(connectors):
            group.add_entry(key, value)

    def add_edge(self, source, targets):
        '''
        Add connector edges

        Parameters
        ----------
        source : string
           Name of the source
        targets : string or list-of-strings
           If a string, then it is a single target.  If a list, then
           it is a list of target names.

        '''
        if isinstance(targets, six.string_types):
            targets = [targets]
        self.edges.append(Edge(source, targets)) 

    def save(self, path=None):
        '''
        Save the project

        Parameters
        ----------
        path : string
            Directory to save the project in

        '''
        self._put('state', params=get_params(value='persisted', path=path))

    def restore(self, path=None):
        '''
        Restore the project

        Parameters
        ----------
        path : string
            Directory where project was saved

        '''
        self._put('state', params=get_params(value='restored', path=path))

    def update(self, project):
        '''
        Update project definition

        Parameters
        ----------
        project : string
            Location of the project data

        '''
        self._put('state', params=get_params(value='modified'),
                  data=get_project_data(project).encode())

    def delete(self):
        ''' Delete the project '''
        self._delete()

    def create_mas_module(self, language, module, func_names, mas_store=None,
                          mas_store_version=None, description=None,
                          code_file=None, code=None):
        '''
        Create a MAS module object

        Parameters
        ----------
        language : string
            The name of the programming language
        module : string, optional
            Name of the MAS module
        func_names : string or list-of-strings, optional
            The function names exported by the module

        Returns
        -------
        :class:`MASModule`

        '''
        out = MASModule(language, module, func_names, mas_store=mas_store,
                        mas_store_version=mas_store_version,
                        description=description, code_file=code_file,
                        code=code)
        out.project = self.name
        out.session = self.session
        return out

    def add_mas_module(self,module):
        self.mas_modules.append(module)
        
    def get_mas_modules(self, expandcode=False):
        '''
        Retrieve all MAS modules

        Parameters
        ----------
        expandcode : bool, optional
            If True, return code-file elements as code elements containing
            the contents of the file.

        Returns
        -------
        list of :class:`MASModule`s

        '''
        res = self._get(urllib.parse.urljoin(self.base_url,
                                             'masModules/%s' % self.name),
                        params=get_params(expandcode=expandcode))
        modules = []
        for elem in res.findall('./project'):
            for mod in elem.findall('./mas-module'):
                mod = MASModule.from_xml(mod, session=self.session)
                mod.project = self.name
                modules.append(mod)
        return modules

    def get_mas_module(self, name, expandcode=False):
        '''
        Retrieve specified MAS modules

        Parameters
        ----------
        name : string
            Name of the MAS module
        expandcode : bool, optional
            If True, return code-file elements as code elements containing
            the contents of the file.

        Returns
        -------
        :class:`MASModule`

        '''
        res = self._get(urllib.parse.urljoin(self.base_url,
                                             'masModules/%s/%s' %
                                             (self.name, name)),
                        params=get_params(expandcode=expandcode))
        mod = MASModule.from_xml(res, session=self.session)
        mod.project = self.name
        return mod

    def replace_mas_module(self, name, module):
        '''
        Replace the specified MAS module

        Parameters
        ----------
        name : string
            Name of the MAS module
        module : string or MASModule
            MAS module configuration

        '''
        self._put(urllib.parse.urljoin(self.base_url,
                                       'masModules/%s/%s' % (self.name, name)),
                  data=get_project_data(module).encode("utf-8"))

    def get_stats(self, interval=None, min_cpu=None, limit=20):
        '''
        Retrieve project statistics subscriber

        Parameters
        ----------
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
        from .connection import ProjectStats
        proj_stats = ProjectStats(self, interval=interval, min_cpu=min_cpu,
                                  limit=limit)
        proj_stats.start()
        return proj_stats

    def validate(self):
        '''
        Validate the project definition

        Returns
        -------
        True
            If project is valid
        False
            If project is not valid

        '''
        try:
            xml = self.to_xml().encode()
            res = self._post(urllib.parse.urljoin(self.base_url,'projectValidationResults'),data=xml)
            if res.tag != 'project-validation-success':
                return False
        except ESPError:
            return False
        return True

    def add_window(self, window, contquery=None):
        '''
        Add a window to the project

        Parameters
        ----------
        window : Window
            The Window object to add
        contquery : string or ContinuousQuery, optional
            The name of the continuous query the window belongs to.
            If not specified, the default continuous query will
            be used.

        Returns
        -------
        :class:`Window`

        '''
        if contquery is None:
            contquery = self.default_query
        else:
            contquery = getattr(contquery, 'name', contquery)

        if contquery not in self.queries:
            self.queries[contquery] = ContinuousQuery()
            self.queries[self.default_query].windows.project_handle = self

        self.queries[contquery].add_window(window)

        return window

    def add_template(self, template, contquery=None):
        '''
        Add a template object

        Parameters
        ----------
        template : Template
        a Template object to add to the project

        Returns
        -------
        :class:`Template`

        '''
        if contquery is None:
            contquery = self.default_query
        else:
            contquery = getattr(contquery, 'name', contquery)

        if contquery not in self.queries:
            self.queries[contquery] = ContinuousQuery()
            self.queries[self.default_query].windows.project_handle = self

        self.queries[contquery].add_template(template)

        return template

    def add_templates(self, *templates):
        '''
        Add one or more templates to the project

        Parameters
        ----------
        templates : one-or-more-Templates
            The Template objects to add

        Returns
        -------
        tuple of :class:`Templates`s

        '''
        for item in templates:
            self.add_template(item)
        return templates

    def add_query(self, contquery):
        '''
        Add a continuous query object

        Parameters
        ----------
        contquery : string or ContinuousQuery
            The name of the continuous query to create, or a
            ContinuousQuery object to add to the project

        Returns
        -------
        :class:`ContinuousQuery`

        '''
        if isinstance(contquery, six.string_types):
            self.queries[contquery] = ContinuousQuery()
            self.queries[contquery].windows.project_handle = self
            return self.queries[contquery]
        else:
            if not contquery.name:
                contquery.name = gen_name(prefix='cq_')
            self.queries[contquery.name] = contquery
            self.queries[contquery.name].windows.project_handle = self
            return self.queries[contquery.name]

    add_continuous_query = add_query

    add_contquery = add_query

    def get_windows(self, path='*.*', type=None, filter=None):
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

        Notes
        -----
        This method retrieves window definitions from the server, not
        from the collection of windows held locally in this projects
        variables.

        Returns
        -------
        list of :class:`Window`

        '''
        path = [None, None] + expand_path(path)
        res = self._get(urllib.parse.urljoin(self.base_url, 'windowXml'),
                        params=get_params(project=self.name,
                                          contquery=path[-2],
                                          name=path[-1], type=type,
                                          filter=filter))
        windows = {}
        for item in res.findall('./*'):
            try:
                wcls = get_window_class(item.tag)
            except KeyError:
                raise TypeError('Unknown window type: %s' % item.tag)
            window = wcls.from_xml(item, session=self.session)
            windows[window.name] = window
        return windows

    def get_window(self, path):
        '''
        Retrieve specified window

        Parameters
        ----------
        path : string
            '.' delimited path to the window including continuous query
            name and window name.

        Notes
        -----
        This method retrieves window definitions from the server, not
        from the collection of windows held locally in this projects
        variables.

        Returns
        -------
        :class:`Window`

        '''
        out = self.get_windows(path)
        if out and len(out) == 1:
            return out[list(out.keys())[0]]
        raise KeyError("No window with the path '%s'." % path)

#
# MutableMapping methods
#

    def __getitem__(self, key):
        return self.queries[key]

    def __setitem__(self, key, value):
        value.project = self.name
        self.queries[key] = value

    def __delitem__(self, key):
        del self.queries[key]

    def __iter__(self):
        return iter(self.queries)

    def __len__(self):
        return len(self.queries)

    def __contains__(self, value):
        return value in self.queries
