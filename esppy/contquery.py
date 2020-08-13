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

''' ESP Continuous Query '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import six
import xml.etree.ElementTree as ET
from six.moves import urllib
from .base import ESPObject, attribute
from .config import get_option
from .metadata import Metadata
from .windows import BaseWindow, get_window_class
from .templates.template import Template
from .utils import xml
from .utils.rest import get_params
from .utils.data import gen_name
from .utils.events import get_events
from .utils.notebook import scale_svg
from .utils.project import expand_path


class WindowDict(collections.abc.MutableMapping):
    '''
    Dictionary for holding window objects

    Attributes
    ----------
    project : string
        The name of the project
    contquery : string
        The name of the continuous query
    session : requests.Session
        The session for the windows

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
        self.project_handle = None
        self.contquery = None
        self.session = None

    @property
    def session(self):
        '''
        The session for the windows

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

    @property
    def project(self):
        '''
        The project that windows are associated with

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
    def contquery(self):
        '''
        The continuous query that windows are associated with

        Returns
        -------
        string

        '''
        return self._contquery

    @contquery.setter
    def contquery(self, value):
        self._contquery = getattr(value, 'name', value)
        for item in self._data.values():
            item.contquery = self._contquery
        if hasattr(value, 'project'):
            self.project = value.project

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        if not isinstance(value, BaseWindow):
            raise TypeError('Only Window objects can be values '
                            'in a ContinuousQuery')
        value._register_to_project(self.project_handle)

        oldname = value.name
        if not value.template:
            value.base_name = key
        value.project = self.project
        value.contquery = self.contquery
        value.session = self.session
        self._data[key] = value

        # Make sure targets get updated with new name
        if oldname != key:
            for window in self._data.values():
                for target in set(window.targets):
                    if target.name == oldname:
                        role = target.role
                        window.targets.discard(target)
                        window.add_target(key, role=role)

    def __delitem__(self, key):
        del self._data[key]
        for window in self._data.values():
            try:
                window.delete_target(key)
            except ValueError:
                pass

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return str(self._data)

    def __repr__(self):
        return repr(self._data)


class TemplateDict(WindowDict):
    '''
    Dictionary for holding template objects

    Attributes
    ----------
    project : string
        The name of the project
    contquery : string
        The name of the continuous query
    session : requests.Session
        The session for the windows

    Parameters
    ----------
    *args : one-or-more arguments, optional
        Positional arguments to MutableMapping
    **kwargs : keyword arguments, optional
        Keyword arguments to MutableMapping

    '''

    def __init__(self, *args, **kwargs):
        WindowDict.__init__(self, *args, **kwargs)

    def __setitem__(self, key, value):
        if not isinstance(value, Template):
            raise TypeError('Only Template objects are valid values')

        value.name = key
        value.project = self.project
        value.contquery = self.contquery
        value.session = self.session
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]


class ContinuousQuery(ESPObject, collections.abc.MutableMapping):
    '''
    Continuous Query

    Parameters
    ----------
    name : string
        Name of the continuous query
    trace : string, optional
        One or more space-separated window names or IDs
    index_type : string, optional
        A default index type for all windows in the continuous query that
        do not explicitly specify an index type
        Valid values: 'rbtree', 'hash', 'ln_hash', 'cl_hash', 'fw_hash', 'empty'
    timing_threshold : int, optional
        When a window in the query takes more than value microseconds to
        compute for a given event or event block, a warning message is logged
    include_singletons : bool, optional
        Specify whether to add unattached source windows
    description : string, optional
        Description of the continuous query

    Attributes
    ----------
    project : string or Project
        Name of the project the query is associated with
    windows : dict
        Collection of windows in the continuous query
    metadata : dict
        Metadata dictionary
    url : string
        URL of the continuous query

    Notes
    -----
    All parameters are also available as instance attributes.

    Returns
    -------
    :class:`ContinuousQuery`

    '''
    trace = attribute('trace', dtype='string')
    index_type = attribute('index', dtype='string',
                           values={'rbtree': 'pi_RBTREE', 'hash': 'pi_HASH',
                                   'ln_hash': 'pi_LN_HASH', 'cl_hash': 'pi_CL_HASH',
                                   'fw_hash': 'pi_FW_HASH', 'empty': 'pi_EMPTY'})
    timing_threshold = attribute('timing-threshold', dtype='int')
    include_singletons = attribute('include-singletons', dtype='bool')

    def __init__(self, name=None, trace=None, index_type=None,
                 timing_threshold=None, include_singletons=None, description=None):
        self.windows = WindowDict()
        self.templates = TemplateDict()
        ESPObject.__init__(self, attrs=locals())
        self.project = None
        self.name = name or gen_name(prefix='cq_')
        self.description = description
        self.metadata = {}

    @property
    def session(self):
        '''
        The requests.Session object for the continuous query

        Returns
        -------
        string

        '''
        return ESPObject.session.fget(self)

    @session.setter
    def session(self, value):
        ESPObject.session.fset(self, value)
        self.windows.session = value

    @property
    def name(self):
        '''
        The name of the continuous query

        Returns
        -------
        string

        '''
        return self._name

    @name.setter
    def name(self, value):
        self._name = value
        self.windows.contquery = value
        self.templates.contquery = value

    @property
    def project(self):
        '''
        The name of the project

        Returns
        -------
        string

        '''
        return self._project

    @project.setter
    def project(self, value):
        self._project = getattr(value, 'name', value)
        self.windows.project = self._project
        self.templates.project = self._project

    def add_window(self, window):
        '''
        Add a window to the continuous query

        Parameters
        ----------
        window : Window
            The Window object to add

        Returns
        -------
        :class:`Window`

        '''
        if not window.name:
            window.name = gen_name(prefix='w_')

        self.windows[window.name] = window

        return window

    def add_windows(self, *windows):
        '''
        Add one or more windows to the continuous query

        Parameters
        ----------
        windows : one-or-more-Windows
            The Window objects to add

        Returns
        -------
        tuple of :class:`Window`s

        '''
        for item in windows:
            self.add_window(item)
        return windows

    def add_template(self, template):
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
        if not template.name:
            template.name = gen_name(prefix='t_')

        self.templates[template.name] = template

        for key, window in sorted(six.iteritems(template.windows)):
            self.add_window(window)
        return template

    def delete_templates(self, *templates):
        '''
        Delete templates and related windows

        Parameters
        ----------
        templates : one-or-more strings or Template objects
            The template to delete

        '''
        for item in templates:
            template_key = getattr(item, 'name', item)
            template = self.templates[template_key]
            self.delete_windows(*six.itervalues(template.windows))
            del self.templates[template_key]

        return self.templates

    delete_template = delete_templates

    # def get_template(self, template):
    #     return

    def copy(self, deep=False):
        '''
        Return a copy of the object

        Parameters
        ----------
        deep : bool, optional
            Should sub-objects be copied as well?

        Returns
        -------
        :class:`ContinuousQuery`

        '''
        out = type(self)()

        out.session = self.session
        out.project = self.project

        for key, value in self._get_attributes(use_xml_values=False).items():
            setattr(out, key, value)

        if deep:
            out.windows = dict([(k, v.copy(deep=True))
                                for k, v in self.windows.items()])
        else:
            out.windows.update(self.windows)

        return out

    def __copy__(self):
        return self.copy(deep=False)

    def __deepcopy__(self, memo):
        return self.copy(deep=True)

    @property
    def fullname(self):
        return '%s.%s' % (self.project, self.name)

    @property
    def url(self):
        '''
        URL of the continuous query

        Returns
        -------
        string

        '''
        if not self.project:
            raise ValueError('This continuous query is not associated with a project.')
        return urllib.parse.urljoin(self.base_url, '%s/%s/' % (self.project, self.name))

    @classmethod
    def from_xml(cls, data, project=None, session=None):
        '''
        Create continous query from XML definition

        Parameters
        ----------
        data : xml-string or ElementTree.Element
            XML continuous query definition
        session : requests.Session, optional
            The session object

        Returns
        -------
        :class:`ContinuousQuery`

        '''
        out = cls()
        out.session = session
        out.project = project

        if isinstance(data, six.string_types):
            data = xml.from_xml(data)

        out._set_attributes(data.attrib)

        for desc in data.findall('./description'):
            out.description = desc.text

        for item in data.findall('./windows/*'):
            try:
                wcls = get_window_class(item.tag)
            except KeyError:
                raise TypeError('Unknown window type: %s' % item.tag)

            window = wcls.from_xml(item, session=session)
            out.windows[window.name] = window

        for item in data.findall('./edges/*'):
            for target in re.split(r'\s+', item.attrib.get('target', '').strip()):
                if not target:
                    continue
                for source in re.split(r'\s+', item.attrib.get('source', '').strip()):
                    if not source:
                        continue
                    out.windows[source].add_target(out.windows[target], role=item.get('role'),
                                                   slot=item.get('slot'))

        for item in data.findall('./metadata/meta'):
            if 'id' in item.attrib.keys():
                out.metadata[item.attrib['id']] = item.text
            elif 'name' in item.attrib.keys():
                out.metadata[item.attrib['name']] = item.text

        return out

    from_element = from_xml

    def to_element(self):
        '''
        Export continuous query definition to ElementTree.Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('contquery', xml.get_attrs(self, exclude='project'))

        if self.description:
            xml.add_elem(out, 'description', text_content=self.description)

        if self.metadata:
            metadata = xml.add_elem(out, 'metadata')
            for key, value in sorted(six.iteritems(self.metadata)):
                xml.add_elem(metadata, 'meta', attrib=dict(id=key),
                             text_content=value)

        windows = xml.add_elem(out, 'windows')

        sources = {}
        if self.windows:
            edges = []
            for name, window in sorted(six.iteritems(self.windows)):
                xml.add_elem(windows, window.to_element(query=self))
                for target in window.targets:
                    sources.setdefault(target.name, []).append(window.name)
                    attrib = dict(source=window.name, target=target.name)
                    if target.role:
                        attrib['role'] = target.role
                    if target.slot:
                        attrib['slot'] = target.slot
                    edges.append((target._index, attrib))
            if edges:
                elem = xml.add_elem(out, 'edges')
                for i, attrib in sorted(edges):
                    xml.add_elem(elem, 'edge', attrib=attrib)

        else:
            xml.add_elem(windows,
                get_window_class('window-source')().to_element(query=self))

        # Replace "inherit" data types with the real data type
        n_inherit = -1
        while True:
            inherit = out.findall('./windows/*/schema/fields/field[@type="inherit"]')
            if len(inherit) == n_inherit:
                break
            n_inherit = len(inherit)
            for window in out.findall('./windows/*'):
                for field in window.findall('./schema/fields/field[@type="inherit"]'):
                    for source in sources[window.attrib['name']]:
                        fname = field.attrib['name']
                        if source not in self.windows:
                            raise ValueError("Could not determine data type of "
                                             "field '%s' on window '%s'" % (fname, source))
                        win = self.windows[source]
                        if hasattr(win, 'schema') and fname in win.schema:
                            dtype = win.schema[fname].type
                            field.set('type', dtype)

        return out

    def to_xml(self, pretty=False):
        '''
        Export continuous query definition to XML

        Parameters
        ----------
        pretty : bool, optional
            Should the output embed whitespaced for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)

    def _persist_metadata(self):
        if self.metadata:
            self._set_metadata(self.metadata)

    def _clear_metadata(self):
        self.metadata.clear()

    def _set_metadata(self, data):
        for key, value in six.iteritems(data):
            self._put(urllib.parse.urljoin(self.base_url,
                                           'projectMetadata/%s/%s/%s' %
                                           (self.project, self.name, key)),
                      data='%s' % value)

    def _del_metadata(self, *data):
        for key in data:
            self._delete(urllib.parse.urljoin(self.base_url,
                                              'projectMetadata/%s/%s/%s' %
                                              (self.project, self.name, key)))

    def save_xml(self, dest, mode='w', pretty=True, **kwargs):
        '''
        Save the continuous query XML to a file

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
        Export continuous query definition to graphviz.Digraph

        Parameters
        ----------
        graph : graphviz.Graph, optional
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
            graph.attr('node', shape='rect')
            graph.attr('graph', rankdir='LR', center='false')
            graph.attr('edge', fontname='times-italic')

        if self.windows:
            qgraph = gv.Digraph(format='svg', name='cluster_%s' % self.fullname.replace('.', '_'))
            qgraph.attr('node', shape='rect')
            qgraph.attr('graph', fontname='helvetica')
            qgraph.attr('edge', fontname='times-italic')
            qgraph.attr(label=self.name, labeljust='l',
                        style='filled,rounded', color='#a0a0a0', fillcolor='#f0f0f0',
                        fontcolor='black')

            if self.templates:
                for tkey, template in sorted(self.templates.items()):
                    qgraph.subgraph(template.to_graph(schema=schema, detail=template_detail))

            for wkey, win in sorted(self.windows.items()):
                if not win.template:
                    win.to_graph(graph=qgraph, schema=schema or get_option('display.show_schema'))
                for target in win.targets:
                    if target.name not in self.windows:
                        continue
                    tail_name = win.template.fullname if win.template and not template_detail else win.fullname
                    head_name = target.template.fullname if target.template and not template_detail \
                        else self.windows[target.name].fullname
                    if win.template and target.template and win.template == target.template:
                        continue
                    graph.edge(tail_name, head_name, label=target.role or '')

            graph.subgraph(qgraph)

        else:
            graph.node(self.fullname, label=self.name, labeljust='l',
                       style='filled,rounded', color='#a0a0a0', fillcolor='#f0f0f0',
                       fontcolor='black')

        return graph

    def _repr_svg_(self):
        try:
            return scale_svg(self.to_graph()._repr_svg_())
        except ImportError:
            raise AttributeError('_repr_svg_')

    def __str__(self):
        return '%s(name=%s, project=%s)' % (type(self).__name__,
                                            repr(self.name),
                                            repr(self.project))

    def __repr__(self):
        return str(self)

    def rename_window(self, window, newname):
        '''
        Rename a window and update targets

        Parameters
        ----------
        window : string or Window object
            The window to rename
        newname : string
            The new name of the window

        '''
        self.windows[newname] = self.windows[getattr(window, 'name', window)]
        self.delete_window(window)

    def delete_windows(self, *windows):
        '''
        Delete windows and update targets

        Parameters
        ----------
        windows : one-or-more strings or Window objects
            The window to delete

        '''
        for item in windows:
            del self.windows[getattr(item, 'name', item)]

    delete_window = delete_windows

    def get_windows(self, name=None, type=None, filter=None):
        '''
        Retrieve windows from the server

        Parameters
        ----------
        name : string or list-of-strings, optional
            Names of the windows which you want to retrieve
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
        dict of :class:`Window`

        '''
        path = [None] + expand_path(name)
        res = self._get(urllib.parse.urljoin(self.base_url, 'windowXml'),
                        params=get_params(project=self.project,
                                          contquery=self.name,
                                          name=path[-1], type=type,
                                          filter=filter))
        windows = dict()
        for item in res.findall('./*'):
            try:
                wcls = get_window_class(item.tag)
            except KeyError:
                raise TypeError('Unknown window type: %s' % item.tag)
            window = wcls.from_xml(item, session=self.session)
            windows[window.fullname.split('.', 1)[-1]] = window
        return windows

    def get_window(self, name):
        '''
        Retrieve specified window

        Parameters
        ----------
        name : string
            Name of the window

        Notes
        -----
        This method retrieves window definitions from the server, not
        from the collection of windows held locally in this projects
        variables.

        Returns
        -------
        :class:`Window`

        '''
        out = self.get_windows(name)
        if out:
            return list(out.values())[0]
        raise KeyError("No window with the name '%s'." % name)

#
# MutableMapping methods
#

    def __getitem__(self, key):
        return self.windows[key]

    def __setitem__(self, key, value):
        self.windows[key] = value

    def __delitem__(self, key):
        del self.windows[key]

    def __iter__(self):
        return iter(self.windows)

    def __len__(self):
        return len(self.windows)

    def __contains__(self, value):
        return value in self.windows
