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

''' ESP Routers '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import six
from six.moves import urllib
from .base import ESPObject
from .utils import xml
from .utils.rest import get_params
from .utils.data import get_project_data, gen_name


class Engine(object):
    '''
    ESP Engine Configuration

    Parameters
    ----------
    name : string
        Name of the ESP engine
    host : string
        Hostname of the ESP server
    port : string
        Port number of the ESP server
    auth_token : string, optional
        Auth token
    auth_token_url : string, optional
        Auth token URL

    Attributes
    ----------
    name : string
        Name of the ESP engine
    host : string
        Hostname of the ESP server
    port : string
        Port number of the ESP server
    auth_token : string
        Auth token
    auth_token_url : string
        Auth token URL
    router : string
        The name of the router the engine definiton belongs to

    Returns
    -------
    :class:`Engine`

    '''

    def __init__(self, host, port, name=None, auth_token=None, auth_token_url=None):
        self.name = name or gen_name(prefix='eng_')
        self.host = host
        self.port = int(port)
        self.auth_token = auth_token
        self.auth_token_url = auth_token_url
        self.router = None

    def to_element(self):
        eng = xml.new_elem('esp-engine',
                           attrib=dict(name=self.name, host=self.host,
                                       port=self.port))
        if self.auth_token is not None:
            xml.add_elem(eng, 'auth_token', self.auth_token)
        if self.auth_token_url is not None:
            xml.add_elem(eng, 'auth_token_url', self.auth_token_url)
        return eng

    def to_xml(self, pretty=False):
        return xml.to_xml(self.to_element(), pretty=pretty)


class PublishDestination(ESPObject):
    '''
    Router Publish Destination

    Parameters
    ----------
    target : string
        The target path in the form "<engine>.<project>.<contquery>.<window>"
    name : string, optional
        The name of the destination
    opcode : string, optional
        The opcode to apply to each event
    filter_func : string, optional
        The function used to filter the events that are published. The function
        is run for each event. Its Boolean value is evaluated to determine
        whether to publish the event to the appropriate target Source window.
    event_fields_init : dict, optional
        Container for functions to be run in order to initialize variables.
        These variables can be used in any subsequent functions.
    event_fields : dict, optional
        Container for functions to be run in order to add or modify event fields.

    Attributes
    ----------
    target : string
        The target path in the form "<engine>.<project>.<contquery>.<window>"
    name : string
        The name of the destination
    opcode : string
        The opcode to apply to each event
    filter_func : string
        The function used to filter the events that are published. The function
        is run for each event. Its Boolean value is evaluated to determine
        whether to publish the event to the appropriate target Source window.
    event_fields_init : dict
        Container for functions to be run in order to initialize variables.
        These variables can be used in any subsequent functions.
    event_fields : dict
        Container for functions to be run in order to add or modify event fields.
    engine_func : string
        Function used to resolve the target engine. It must resolve to the name
        of one of the engines that are defined in the router context.
    project_func : string
        Function used to resolve the target project. It must resolve to the name
        of a project in the engine that is resolved in engine_func.
    contquery_func : string
        Function used to resolve the target continuous query. It must resolve
        to the name of a continuous query in the project that is resolved in
        project-func.
    window_func : string
        Function used to resolve the target Source window. It must resolve to the
        name of a source window in the continuous query that is resolved in
        contquery_func.
    router : string
        The name of the router the destination belongs to

    Returns
    -------
    :class:`PublishDestination`

    '''

    def __init__(self, target, name=None, opcode=None, filter_func=None,
                 event_fields_init=None, event_fields=None):
        ESPObject.__init__(self)
        self.name = name or gen_name(prefix='pd_')
        self.opcode = opcode
        self.filter_func = filter_func
        self.event_fields_init = dict(event_fields_init or {})
        self.event_fields = dict(event_fields or {})
        self.target = target
        self.router = None

    @property
    def target(self):
        '''
        Target path

        Returns
        -------
        string

        '''
        return '.'.join(['%s' % x for x in [self.engine_func,
                                            self.project_func,
                                            self.contquery_func,
                                            self.window_func]])

    @target.setter
    def target(self, value):
        '''
        Set the target path

        Parameters
        ----------
        value : string
            The target path in the form "<engine>.<project>.<contquery>.<window>"

        '''
        if value.count('.') < 3:
            raise ValueError('Target does not contain enough levels')
        value = list(value.split('.'))
        self.engine_func = value[0]
        self.project_func = value[1]
        self.contquery_func = value[2]
        self.window_func = value[3]

    def initialize(self):
        ''' Initialize the destination '''
        self._put(urllib.parse.urljoin(self.base_url,
                                       'routerDestinations/%s/%s/state' %
                                       (self.router, self.name)),
                  params=get_params(value='initialized'))

    def to_element(self):
        '''
        Convert destination to Element definition

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        dest = xml.new_elem('publish-destination',
                            attrib=dict(name=self.name,
                                        opcode=self.opcode))
        if self.filter_func is not None:
            xml.add_elem(dest, 'filter-func',
                         text_content=self.filter_func)

        tgt = xml.add_elem(dest, 'publish-target')
        if self.engine_func is not None:
            xml.add_elem(tgt, 'engine-func', text_content=self.engine_func)
        if self.project_func is not None:
            xml.add_elem(tgt, 'project-func', text_content=self.project_func)
        if self.contquery_func is not None:
            xml.add_elem(tgt, 'contquery-func', text_content=self.contquery_func)
        if self.window_func is not None:
            xml.add_elem(tgt, 'window-func', text_content=self.window_func)

        if self.event_fields_init or self.event_fields:
            efields = xml.add_elem(dest, 'event-fields')
            if self.event_fields_init:
                init = xml.add_elem(efields, 'init')
                for key, value in six.iteritems(self.event_fields_init):
                    xml.add_elem(init, 'value', attrib=dict(name=key),
                                 text_content=value)
            if self.event_fields:
                flds = xml.add_elem(efields, 'fields')
                for key, value in six.iteritems(self.event_fields):
                    xml.add_elem(flds, 'field', attrib=dict(name=key),
                                 text_content=value)

        return dest

    def to_xml(self, pretty=False):
        '''
        Convert destination to XML definition

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class WriterDestination(ESPObject):
    '''
    Route Writer Destination

    Parameters
    ----------
    file_func : string
        Function used to resolve the name of the file into which the events
        are written.
    format : string
        Format of event output. Valid values are XML, JSON, and CSV.
        The default is XML.
    name : string, optional
        The name of the destination
    dateformat : string, optional
        The format for datetime strings in the data

    Attributes
    ----------
    file_func : string
        Function used to resolve the name of the file into which the events
        are written.
    format : string
        Format of event output. Valid values are XML, JSON, and CSV.
        The default is XML.
    name : string
        The name of the destination
    dateformat : string
        The format for datetime strings in the data
    router : string
        The name of the router the destination belongs to

    Returns
    -------
    :class:`WriterDestination`

    '''

    def __init__(self, file_func, format, name=None, dateformat='%Y%m%dT%H:%M:%S.%f'):
        ESPObject.__init__(self)
        self.name = name or gen_name(prefix='wd_')
        self.format = format
        self.dateformat = dateformat
        self.file_func = file_func
        self.router = None

    def initialize(self):
        ''' Initialize the destination '''
        self._put(urllib.parse.urljoin(self.base_url,
                                       'routerDestinations/%s/%s/state' %
                                       (self.router, self.name)),
                  params=get_params(value='initialized'))

    def to_element(self):
        '''
        Convert the destination to an Element definition

        Returns
        -------
        :class:`WriterDestination`

        '''
        dest = xml.new_elem('writer-destination',
                            attrib=dict(name=self.name,
                                        format=self.format,
                                        dateformat=self.dateformat))
        xml.add_elem(dest, 'file-func', text_content=self.file_func)
        return dest

    def to_xml(self, pretty=False):
        '''
        Convert the destination to an XML definition

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class Route(object):
    '''
    Route

    Parameters
    ----------
    route : string
        The route path in the form "<engine>.<project>.<contquery>.<window>.<type>"
        where <type> is optional.
    to : string
        Comma-separated list of destinations that receive all of the events coming
        in from the subscriptions contained within the route.
    name : string, optional
        The name of the route
    snapshot : bool, optional
        Specify a value of true when you want to grab a snapshot of the initial
        window contents.

    Attributes
    ----------
    route : string
        The route path in the form "<engine>.<project>.<contquery>.<window>.<type>"
        where <type> is optional.
    to : string
        Comma-separated list of destinations that receive all of the events coming
        in from the subscriptions contained within the route.
    name : string, optional
        The name of the route
    snapshot : bool, optional
        Specify a value of true when you want to grab a snapshot of the initial
        window contents.
    engine_expr : string
        Regular expression used to resolve the engine(s) to which the route should
        subscribe. If it is not specified, the route subscribes to all engines.
    project_expr : string
        Regular expression used to resolve the project(s) to which the route should
        subscribe. If it is not specified, the route subscribes to all projects.
    contquery_expr : string
        Regular expression used to resolve the continuous queries to which the route
        should subscribe. If it is not specified, the route subscribes to all
        continuous queries.
    window_expr : string
        Regular expression used to resolve the window(s) to which the route
        should subscribe. If it is not specified, the route subscribes to
        all windows.
    type_expr : string
        Regular expression used to resolve the window type(s) to which the
        route should subscribe. If it is not specified, the route subscribes
        to all window types.
    router : string
        The name of the router the route belongs to

    Returns
    -------
    :class:`Route`

    '''

    def __init__(self, route, to, name=None, snapshot=False):
        self.name = name or gen_name('r_')
        self.to = to
        self.snapshot = snapshot
        self.route = route
        self.router = None

    @property
    def route(self):
        '''
        Route path

        Returns
        -------
        string

        '''
        route = '.'.join(['%s' % x for x in [self.engine_expr,
                                             self.project_expr,
                                             self.contquery_expr,
                                             self.window_expr,
                                             self.type_expr]])
        while route.endswith('.None'):
            route = route[:-5]
        return route

    @route.setter
    def route(self, value):
        '''
        Set route path

        Parameters
        ----------
        value : string
            The route path in the form "<engine>.<project>.<contquery>.<window>.<type>"
            where <type> is optional.

        '''
        if value.count('.') < 3:
            raise ValueError('Route does not contain enough levels')
        value = list(value.split('.')) + [None]
        self.engine_expr = value[0]
        self.project_expr = value[1]
        self.contquery_expr = value[2]
        self.window_expr = value[3]
        self.type_expr = value[4]

    def to_element(self):
        '''
        Convert Route to an Element definition

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        rte = xml.new_elem('esp-route',
                           attrib=dict(name=self.name, to=self.to,
                                       snapshot=self.snapshot))

        if self.engine_expr is not None:
            xml.add_elem(rte, 'engine-expr', text_content=self.engine_expr)
        if self.project_expr is not None:
                    xml.add_elem(rte, 'project-expr', text_content=self.project_expr)
        if self.contquery_expr is not None:
            xml.add_elem(rte, 'contquery-expr', text_content=self.contquery_expr)
        if self.window_expr is not None:
            xml.add_elem(rte, 'window-expr', text_content=self.window_expr)
        if self.type_expr is not None:
            xml.add_elem(rte, 'type-expr', text_content=self.type_expr)

        return rte

    def to_xml(self, pretty=False):
        '''
        Convert Route to an XML definition

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class Router(ESPObject):
    '''
    Router definition

    Parameters
    ----------
    name : string
        Name of the router

    Attributes
    ----------
    name : string
        Name of the router
    engines : dict-of-Engines
        The ESP engines in the router definition
    destinations : dict-of-PublishDestinations/WriterDestinations
        The destinations for the router
    routes : dict-of-Routes
        The routes defined in the Router

    '''

    def __init__(self, name=None):
        ESPObject.__init__(self)
        self.name = name or gen_name(prefix='r_')
        self.engines = {}
        self.destinations = {}
        self.routes = {}

    @classmethod
    def from_xml(cls, data, session=None):
        '''
        Create router from XML definition

        Parameters
        ----------
        data : xml-string or ElementTree.Element
            XML router definition
        session : requests.Session, optional
            Session that the router is associated with

        Returns
        -------
        :class:`Router`

        '''
        out = cls()
        out.session = session

        if isinstance(data, six.string_types):
            data = xml.from_xml(data)

        if data.tag != 'esp-router':
            data = data.find('.//esp-router')
            if data is None:
                raise ValueError('No router definition was found')

        out.name = data.attrib['name']

        for eng in data.findall('./esp-engines/esp-engine'):
            args = dict(eng.attrib)
            for item in eng.findall('./auth_token'):
                args['auth_token'] = item.text
            for item in eng.findall('./auth_token_url'):
                args['auth_token_url'] = item.text
            eng = Engine(**args)
            eng.session = session
            eng.router = out.name
            out.engines[eng.name] = eng

        for pdest in data.findall('./esp-destinations/publish-destination'):
            path = ['None', 'None', 'None', 'None']
            for item in pdest.findall('./publish-target/engine-func'):
                path[0] = item.text
            for item in pdest.findall('./publish-target/project-func'):
                path[1] = item.text
            for item in pdest.findall('./publish-target/contquery-func'):
                path[2] = item.text
            for item in pdest.findall('./publish-target/window-func'):
                path[3] = item.text

            filter_func = None
            for item in pdest.findall('./filter-func'):
                filter_func = item.text

            einit = {}
            for evt in data.findall('./event-fields/init/value'):
                name = evt.attrib.get('name', gen_name(prefix='ei_'))
                einit[name] = evt.text

            efields = {}
            for evt in data.findall('./event-fields/fields/field'):
                name = evt.attrib.get('name', gen_name(prefix='ef_'))
                efields[name] = evt.text

            dst = PublishDestination('.'.join(path), filter_func=filter_func,
                                     event_fields_init=einit, event_fields=efields,
                                     **pdest.attrib)
            dst.session = session
            dst.router = out.name
            out.destinations[dst.name] = dst

        for wdest in data.findall('./esp-destinations/writer-destination'):
            args = dict(wdest.attrib)
            for func in wdest.findall('./file-func'):
                args['file_func'] = func.text
            dst = WriterDestination(**args)
            dst.session = session
            dst.router = out.name
            out.destinations[dst.name] = dst

        for rte in data.findall('./esp-routes/esp-route'):
            path = ['None', 'None', 'None', 'None']
            for item in rte.findall('./engine-expr'):
                path[0] = item.text
            for item in rte.findall('./project-expr'):
                path[1] = item.text
            for item in rte.findall('./contquery-expr'):
                path[2] = item.text
            for item in rte.findall('./window-expr'):
                path[3] = item.text
            for item in rte.findall('./type-expr'):
                path.append(item.text)
            path = '.'.join(path)

            route = Route(path, rte.attrib['to'],
                          name=rte.attrib.get('name'),
                          snapshot=rte.attrib.get('snapshot',
                                                  'f').lower().startswith('t'))
            route.session = session
            route.router = out.name
            out.routes[route.name] = route

        return out

    from_element = from_xml

    def to_element(self):
        '''
        Convert Router to Element definition

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('esp-router', attrib=dict(name=self.name))

        if self.engines:
            engs = xml.add_elem(out, 'esp-engines')
            for item in sorted(self.engines.values(), key=lambda x: x.name):
                xml.add_elem(engs, item.to_element())

        if self.destinations:
            dests = xml.add_elem(out, 'esp-destinations')
            for item in sorted(self.destinations.values(), key=lambda x: x.name):
                xml.add_elem(dests, item.to_element())

        if self.routes:
            routes = xml.add_elem(out, 'esp-routes')
            for item in sorted(self.routes.values(), key=lambda x: x.name):
                xml.add_elem(routes, item.to_element())

        return out

    def to_xml(self, pretty=False):
        '''
        Convert Router to XML definition

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)

    def add_engine(self, host, port, name=None, auth_token=None,
                   auth_token_url=None):
        '''
        Add a new router engine

        Parameters
        ----------
        name : string
            Name of the router engine
        host : string
            Hostname of the server
        port : int
            Port number of the server
        auth_token : string, optional
            Auth token
        auth_token_url : string, optional
            URL to auth token

        Returns
        -------
        :class:`Engine`

        '''
        eng = Engine(host, port, name=name, auth_token=auth_token,
                     auth_token_url=auth_token_url)
        self.engines[eng.name] = eng
        return eng

    def add_publish_destination(self, target, name=None, opcode=None, filter_func=None,
                                event_fields_init=None, event_fields=None):
        '''
        Add a new router publish destination

        Parameters
        ----------
        target : string
            The target path in the form "<engine>.<project>.<contquery>.<window>"
        name : string, optional
            Name of the router destination
        opcode : string, optional
            Opcode for each event
        filter_func : string, optional
            The function used to filter the events that are published. The
            function is run for each event. Its Boolean value is evaluated
            to determine whether to publish the event to the appropriate
            target Source window.
        event_fields_init : dict, optional
            Container for functions to be run in order to initialize
            variables. These variables can be used in any subsequent
            functions.
        event_fields : dict, optional
            Container for functions to be run in order to add or modify
            event fields.

        '''
        dst = PublishDestination(target, name=name, opcode=opcode, filter_func=filter_func,
                                 event_fields_init=event_fields_init,
                                 event_fields=event_fields)
        dst.session = self.session
        dst.router = self.name
        self.destinations[dst.name] = dst

    def add_writer_destination(self, file_func, format, name=None,
                               dateformat='%Y%m%dT%H:%M:%S.%f'):
        '''
        Add a new router writer destination

        Parameters
        ----------
        file_func : string
            Function used to resolve the name of the file into which the
            events are written.
        format : string
            Format of event output. Valid values are XML, JSON, and CSV.
            The default is XML.
        name : string, optional
            Name of the router destination
        dateformat : string, optional
            Format of date strings

        Returns
        -------
        :class:`WriterDestination`

        '''
        dst = WriterDestination(file_func, format, name=name, dateformat=dateformat)
        dst.session = self.session
        dst.router = self.name
        self.destinations[dst.name] = dst
        return dst

    def initialize_destination(self, name):
        '''
        Initalize router destination

        Parameters
        ----------
        name : string
            Name of the destination to initialize

        '''
        self.destinations[name].initialize()

    def add_route(self, route, to, name=None, snapshot=False):
        '''
        Add a new route

        Parameters
        ----------
        route : string
            The route path in the form
            "<engine>.<project>.<contquery>.<window>.<type>", where <type>
            is optional.
        to : string
            Comma-separated list of destinations that receive all of the
            events coming in from the subscriptions contained within the route.
        name : string, optional
            Name of the route to create
        snapshot : bool, optional
            Specify a value of true when you want to grab a snapshot of
            the initial window contents.

        Returns
        -------
        :class:`Route`

        '''
        rte = Route(route, to, name=name, snapshot=snapshot)
        rte.session = self.session
        rte.router = self.name
        self.routes[rte.name] = rte
        return rte

    def save(self, overwrite=True):
        '''
        Save the router definition to the server

        Parameters
        ----------
        overwrite : bool, optional
            Should an existing router of the same name be overwritten?

        '''
        data=get_project_data(self).encode()
        self._put(urllib.parse.urljoin(self.base_url,
                                       'routers/%s' % self.name),
                  params=get_params(overwrite=overwrite),
                  data=data)

    def delete(self):
        ''' Delete the router '''
        self._delete(urllib.parse.urljoin(self.base_url,
                                          'routers/%s' % self.name))
