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

''' ESP Event Generators '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import pandas as pd
import re
import requests
import six
from six.moves import urllib
from .base import ESPObject, attribute
from .utils.rest import get_params
from .utils.data import get_project_data, gen_name
from .utils import xml
from .windows import BaseWindow

MapURL = collections.namedtuple('MapURL', ['url'])
ListURL = collections.namedtuple('ListURL', ['url'])
SetURL = collections.namedtuple('SetURL', ['url'])


DELIMITERS = ' ,;@&'
INNER = '=:%#!'


def get_list_delimiter(value):
    '''
    Determine good list delimiter for value

    Parameters
    ----------
    value : list or tuple or set
        The list values

    Returns
    -------
    string

    '''
    value = ''.join('%s' % x for x in value)
    for char in DELIMITERS:
        if char not in value:
            return char
    raise ValueError('Could not determine a good delimiter character') 


def get_map_delimiters(value):
    '''
    Determine good map delimiter for value

    Parameters
    ----------
    value : dict
        The map values

    Returns
    -------
    (inner-delim, outer-delim)

    '''
    key = ''.join('%s' % x for x in value.keys())
    value = ''.join('%s' % x for x in value.values())
    outer = inner = None
    for char in DELIMITERS:
        if char not in key and char not in value:
            outer = char 
            break
    for char in INNER:
        if char not in key and char not in value:
            inner = char 
            break
    if outer is None or inner is None:
        raise ValueError('Could not determine a good delimiter character')
    return inner, outer


class Resources(collections.abc.MutableMapping):
    ''' Event Generator Resource Manager '''

    def __init__(self):
        self._data = {}

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        if isinstance(value, (MapURL, ListURL, SetURL)):
            self._data[key] = value
        elif isinstance(value, (dict, collections.abc.MutableMapping)):
            self._data[key] = dict(value)
        elif isinstance(value, (list, tuple)):
            self._data[key] = list(value)
        elif isinstance(value, set):
            self._data[key] = set(value)
        elif isinstance(value, six.string_types):
            raise ValueError('Use add_map_urls, add_list_urls, or '
                             'add_set_urls to add URL resources')
        else:
            raise TypeError('Unknown type for resource: %s' % value)

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

    def add_map_urls(self, **kwargs):
        '''
        Add URLs to map resources

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of URL resources.  The key is the
            resource name.  The value is the URL.

        '''
        for key, value in six.iteritems(kwargs):
            self._data[key] = MapURL(value)

    def add_list_urls(self, **kwargs):
        '''
        Add URLs to a list resources

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of URL resources.  The key is the
            resource name.  The value is the URL.

        '''
        for key, value in six.iteritems(kwargs):
            self._data[key] = ListURL(value)

    def add_set_urls(self, **kwargs):
        '''
        Add URLs to set resources

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of URL resources.  The key is the
            resource name.  The value is the URL.

        '''
        for key, value in six.iteritems(kwargs):
            self._data[key] = SetURL(value)


class EventGenerator(ESPObject):
    '''
    Event generator

    Attributes
    ----------
    autogen_key : bool, optional
        Should keys be generated for events?
    data : CSV or URL or DataFrame, optional
        Event data
    insert_only : bool, optional
        Should duplicate keys be removed?
    name : string
        Name of the event generator
    publish_target : URL-string or Window, optional
        The URL to publish the events to
    resources : Resources dict
        The resources for the event generator
    init : dict
        Initialization parameters
    fields : dict
        Event data fields
    exists_opcode : string
        Code to determine upsert or delete for an event
    event_data : string
        Either a URL to the data, or CSV data

    Parameters
    ----------
    name : string, optional
        Name of the event generator
    insert_only : bool, optional
        Should duplicate keys be removed?
    autogen_key : bool, optional
        Should keys be generated for events?
    publish_target : URL-string or Window, optional
        The URL to publish the events to
    data : CSV or URL or DataFrame, optional
        Event data

    '''

    insert_only = attribute('insert-only', dtype='bool')
    autogen_key = attribute('autogen-key', dtype='bool')
    publish_target = attribute('publish-target', dtype='string')
    
    def __init__(self, name=None, insert_only=True, autogen_key=True,
                 publish_target=None, data=None):
        ESPObject.__init__(self)
        self.name = name or gen_name(prefix='eg_')
        self.insert_only = insert_only
        self.autogen_key = autogen_key
        self.publish_target = publish_target
        self.resources = Resources()
        self.init = collections.OrderedDict()
        self.fields = collections.OrderedDict()
        self.exists_opcode = None
        self.event_data = data

    def __str__(self):
        if self.publish_target:
            return '%s(name=%s, publish_target=%s)' % \
                   (type(self).__name__, repr(self.name), repr(self.publish_target))
        return '%s(name=%s)' % (type(self).__name__, repr(self.name))

    def __repr__(self):
        return str(self)

    def add_initializers(self, **kwargs):
        '''
        Add values to initialize when event generator starts

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of initializers.  The key is the field
            name.  The value is the initialization expression.

        '''
        self.init.update(kwargs)

    def add_fields(self, **kwargs):
        '''
        Add fields to generate

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of field definitions.  The key is the
            field name.  The value is the field expression.

        '''
        self.fields.update(kwargs)

    def add_map_resources(self, **kwargs):
        '''
        Add map resources

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of map resources.  The key is the name
            of the resource.  The value is a dictionary of value mappings.

        '''
        for key, value in six.iteritems(kwargs):
            if not isinstance(value, (dict, collections.abc.MutableMapping)):
                raise TypeError('Map resources must be a dictionary')
            self.resources[key] = dict(value)

    def add_list_resources(self, **kwargs):
        '''
        Add list resources

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of list resources.  The key is the name
            of the resource.  The value is a list of values.

        '''
        for key, value in six.iteritems(kwargs):
            if not isinstance(value, (list, tuple, set)):
                raise TypeError('List resources must be either a list, tuple, or set')
            self.resources[key] = list(value)

    def add_set_resources(self, **kwargs):
        '''
        Add set resources

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of set resources.  The key is the name
            of the resource.  The value is a set of values.

        '''
        for key, value in six.iteritems(kwargs):
            if not isinstance(value, (list, tuple, set)):
                raise TypeError('Set resources must be either a list, tuple, or set')
            self.resources[key] = set(value)

    def add_map_url_resources(self, **kwargs):
        '''
        Add map URL resources

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of map URL resources.  The key is the name
            of the resource.  The value is a URL.

        '''
        for key, value in six.iteritems(kwargs):
            self.resources[key] = MapURL(value)

    def add_list_url_resources(self, **kwargs):
        '''
        Add list URL resources

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of list URL resources.  The key is the name
            of the resource.  The value is a URL.

        '''
        for key, value in six.iteritems(kwargs):
            self.resources[key] = ListURL(value)

    def add_set_url_resources(self, **kwargs):
        '''
        Add set URL resources

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Key / value pairs of set URL resources.  The key is the name
            of the resource.  The value is a URL.

        '''
        for key, value in six.iteritems(kwargs):
            self.resources[key] = SetURL(value)

    @property
    def event_data(self):
        if self._event_data is not None:
            return self._event_data
        return self._event_data_url

    @event_data.setter
    def event_data(self, value):
        if isinstance(value, six.string_types):
            if os.path.isfile(value):
                self._event_data_url = None
                with open(value, 'r') as infile:
                    self._event_data = infile.read()
            elif re.match(r'^(https?|ftp|file):', value) and '\n' not in value:
                self._event_data_url = value
                self._event_data = None
            else:
                self._event_data_url = None
                self._event_data = value

        elif isinstance(value, pd.DataFrame):
            self._event_data_url = None
            self._event_data = value.to_csv(header=False, index=False)

        elif hasattr(value, 'read'):
            self._event_data_url = None
            self._event_data = value.read()

        else:
            self._event_data_url = None
            self._event_data = value

    @property
    def publish_target(self):
        '''
        The URL to publish the events to

        Returns
        -------
        string

        '''
        return self._publish_target

    @publish_target.setter
    def publish_target(self, value):
        '''
        Set the URL for publishing events to

        Parameters
        ----------
        value : string or Window
            If a string, it should be the URL to publish to.
            If a Window, the URL will be derived from that object.

        '''
        if isinstance(value, BaseWindow):
            window = value.fullname.replace('.', '/')

            # Get pubsub port
            port = self._get(urllib.parse.urljoin(self.base_url, 'server'),
                             params=get_params(config=True)
                             ).get('pubsub')
            # Get hostname
            hostname = urllib.parse.urlparse(self.base_url).hostname

            value = 'dfESP://%s:%s/%s' % (hostname, port, window)

        self._publish_target = value

    @property
    def is_running(self):
        ''' Is the event generator current running? '''
        out = self._get(urllib.parse.urljoin(self.base_url,
                                             'eventGenerators/%s' % self.name))
        return 'stop' not in out.attrib['state']

    @property
    def url(self):
        '''
        URL of event generator

        Returns
        -------
        string

        '''
        return urllib.parse.urljoin(self.base_url, 'eventGenerators/%s' % self.name)

    @classmethod
    def from_xml(cls, data, session=None):
        '''
        Create event generator from XML definition

        Parameters
        ----------
        data : xml-string or ElementTree.Element
            XML event generator definition
        session : requests.Session, optional
            Session that the event generator is associated with

        Returns
        -------
        :class:`EventGenerator`

        '''
        out = cls()
        out.session = session

        if isinstance(data, six.string_types):
            data = xml.from_xml(data)

        out._set_attributes(data.attrib)

        publish_target = data.find('./publish-target')
        if publish_target is not None:
            out.publish_target = publish_target.text

        for item in data.findall('./resources/*'):
            if item.tag == 'list-url':
                out.resources.add_list_urls(**{item.attrib['name']: item.text})
            elif item.tag == 'map-url':
                out.resources.add_map_urls(**{item.attrib['name']: item.text})
            elif item.tag == 'set-url':
                out.resources.add_set_urls(**{item.attrib['name']: item.text})
            elif item.tag == 'list':
                out.resources[item.attrib['name']] = \
                    item.text.split(item.attrib.get('delimiter', ' '))
            elif item.tag == 'map':
                outer = item.attrib.get('outer', ' ')
                inner = item.attrib.get('inner', '=')
                out.resources[item.attrib['name']] = \
                    dict([tuple(x.split(inner, 1)) for x in item.text.split(outer)])
            elif item.tag == 'set':
                out.resources[item.attrib['name']] = \
                    set(item.text.split(item.attrib.get('delimiter', ' ')))
            else:
                raise TypeError('Unknown resource type: %s' % item.tag)

        for item in data.findall('./init/value'):
            out.init[item.attrib['name']] = item.text

        for item in data.findall('./fields/field'):
            out.fields[item.attrib['name']] = item.text

        exists_opcode = data.find('./exists-opcode')
        if exists_opcode is not None:
            out.exists_opcode = exists_opcode

        event_source = data.find('./event-source')
        if event_source is not None:
            event_data_url = event_source.find('./event-data-url')
            if event_data_url is not None:
                out.event_data = event_data_url.text
            event_data = event_source.find('./event-data')
            if event_data is not None:
                out.event_data = event_data.text

        return out

    from_element = from_xml

    def to_element(self):
        '''
        Export the event generator definition to an ElementTree.Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('event-generator',
                           xml.get_attrs(self, exclude=['publish_target',
                                                        'exists_opcode',
                                                        'event_data_url',
                                                        'event_data']))

        if self.publish_target:
            xml.add_elem(out, 'publish-target',
                         text_content=self.publish_target)

        if self._event_data_url is not None:
            data = xml.add_elem(out, 'event-source')
            xml.add_elem(data, 'event-data-url', text_content=self._event_data_url)

            fields = xml.add_elem(out, 'fields')
            xml.add_elem(fields, 'field',
                         attrib=dict(name='dummy-id'), text_content=0)

            return out

        if self._event_data is not None:
            data = xml.add_elem(out, 'event-source')
            if isinstance(self._event_data, pd.DataFrame):
                event_data = self._event_data.to_csv(header=False, index=False)
            else:
                event_data = self._event_data
            xml.add_elem(data, 'event-data', text_content=event_data)

            fields = xml.add_elem(out, 'fields')
            xml.add_elem(fields, 'field',
                         attrib=dict(name='dummy-id'), text_content=0)

            return out

        if self.resources:
            resources = xml.add_elem(out, 'resources')
            for key, value in self.resources.items():

                if isinstance(value, list):
                    delim = get_list_delimiter(value)
                    xml.add_elem(resources, 'list',
                                 attrib=dict(name=key, delimiter=delim),
                                 text_content=' '.join(['%s' % x for x in value]))

                elif isinstance(value, set):
                    delim = get_list_delimiter(value)
                    xml.add_elem(resources, 'set',
                                 attrib=dict(name=key, delimiter=delim),
                                 text_content=' '.join(['%s' % x for x in value]))

                elif isinstance(value, dict):
                    inner, outer = get_map_delimiters(value)
                    xml.add_elem(resources, 'map',
                                 attrib=dict(name=key, outer=outer, inner=inner),
                                 text_content=outer.join(['%s%s%s' % (k, inner, v)
                                                          for k, v in value.items()]))

                elif isinstance(value, ListURL):
                    xml.add_elem(resources, 'list-url',
                                 attrib=dict(name=key),
                                 text_content=value.url)

                elif isinstance(value, SetURL):
                    xml.add_elem(resources, 'set-url',
                                 attrib=dict(name=key),
                                 text_content=value.url)

                elif isinstance(value, MapURL):
                    xml.add_elem(resources, 'map-url',
                                 attrib=dict(name=key),
                                 text_content=value.url)

        if self.init:
            init = xml.add_elem(out, 'init')
            for key, value in self.init.items():
                xml.add_elem(init, 'value',
                             attrib=dict(name=key), text_content=value)

        if self.exists_opcode:
            xml.add_elem(out, 'exists-opcode',
                         text_content=self.exists_opcode)

        if self.fields:
            fields = xml.add_elem(out, 'fields')
            for key, value in self.fields.items():
                xml.add_elem(fields, 'field',
                             attrib=dict(name=key), text_content=value)
        else:
            fields = xml.add_elem(out, 'fields')
            xml.add_elem(fields, 'field',
                         attrib=dict(name='dummy-id'), text_content=0)

        return out

    def to_xml(self, pretty=False):
        '''
        Export the event generator definition to XML

        Parameters
        ----------
        pretty : bool, optional
            Should the XML include whitespace for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)

    def start(self, events=None, blocksize=None, rate=None, pause=None):
        '''
        Start the generator

        Parameters
        ----------
        events : int, optional
            The number of events to inject
        blocksize : int, optional
            The block size of events to inject
        rate : int, optional
            The maximum number of events per second to inject
        pause : int, optional
            The number of milliseconds to pause betwen each event injection

        '''
        self._put(urllib.parse.urljoin(self.base_url,
                                       'eventGenerators/%s/state' % self.name),
                  params=get_params(value='started',
                                    events=events,
                                    blocksize=blocksize,
                                    rate=rate,
                                    pause=pause))

    def stop(self):
        ''' Stop the event generator '''
        self._put(urllib.parse.urljoin(self.base_url,
                                       'eventGenerators/%s/state' % self.name),
                  params=get_params(value='stopped'))

    def initialize(self):
        ''' Initialize the event generator '''
        self._put(urllib.parse.urljoin(self.base_url,
                                       'eventGenerators/%s/state' % self.name),
                  params=get_params(value='initialized'))

    def delete(self):
        ''' Delete the event generator '''
        self._delete(urllib.parse.urljoin(self.base_url,
                                          'eventGenerators/%s' % self.name))

    def save(self, overwrite=True, name=None):
        '''
        Save the event generator

        Parameters
        ----------
        overwrite : bool, optional
            Save the event generator configuration to the server
        name : string, optional
            Optional name override

        '''
        xml = self.to_xml().encode()
        self._put(urllib.parse.urljoin(self.base_url,
                                       'eventGenerators/%s' % (name or self.name)),
                  params=get_params(overwrite=overwrite),
                  data=xml)
