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

from __future__ import print_function, division, absolute_import, unicode_literals

import six
from .base import Window, attribute
from .features import InitializeExpressionFeature, SchemaFeature
from .utils import get_args, ensure_element, connectors_to_end
from ..utils import xml


class FieldPlugin(object):
    '''
    Compute field plugin

    Parameters
    ----------
    plugin : string
       The shared library that contains the specified function
    function : string
       The specified function

    Returns
    -------
    :class:`FieldPlugin`

    '''

    def __init__(self, plugin, function):
        self.plugin = plugin
        self.function = function

    def copy(self, deep=False):
        return type(self)(self.plugin, self.function)


class ContextPlugin(object):
    '''
    Compute context plugin

    Parameters
    ----------
    name : string
        The shared library that contains the context–generation function
    function : string
        The function that, when called, returns a new derived context
        for the window’s handler routines.

    Returns
    -------
    :class:`ContextPlugin`

    '''

    def __init__(self, name, function):
        self.name = name
        self.function = function

    def copy(self, deep=False):
        return type(self)(self.name, self.function)


class ComputeWindow(Window, InitializeExpressionFeature, SchemaFeature):
    '''
    Compute window

    Parameters
    ----------
    name : string, optional
        The name of the window
    schema : Schema
        The schema of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for
        the window and false disables it.
    description : string, optional
        Description of the window
    output_insert_only : bool, optional
        When true, prevents the window from passing non-insert events to
        other windows.
    collapse_updates : bool, optional
        When true, multiple update blocks are collapsed into a single
        update block
    pulse_interval : string, optional
        Output a canonical batch of updates at the specified interval
    exp_max_string : int, optional
        Specifies the maximum size of strings that the expression engine
        uses for the window. Default value is 1024.
    index_type : string, optional
        Index type for the window
    pubsub_index_type : string, optional
        Publish/subscribe index type.  Valid values are the same as the
        `index_type` parameter.

    Attributes
    ----------
    context_plugin : ContextPlugin
        Function that returns context for use in plugins
    field_expressions : list-of-strings
        Field expressions
    field_plugins : list-of-FieldPlugins
        Field plugins

    Returns
    -------
    :class:`ComputeWindow`

    '''

    window_type = 'compute'

    def __init__(self, name=None, schema=None, pubsub=None, description=None,
                 output_insert_only=None, collapse_updates=None,
                 pulse_interval=None, exp_max_string=None, index_type=None,
                 pubsub_index_type=None):
        Window.__init__(self, **get_args(locals()))
        self.context_plugin = None
        self.field_expressions = []
        self.field_plugins = []

    def copy(self, deep=False):
        out = Window.copy(self, deep=deep)
        out.context_plugin = self.context_plugin.copy(deep=deep)
        if deep:
            out.field_expressions = [x.copy(deep=deep) for x in self.field_expressions]
            out.field_plugins = [x.copy(deep=deep) for x in self.field_plugins]
        else: 
            out.field_expressions = list(self.field_expressions)
            out.field_plugins = list(self.field_plugins)
        return out

    @classmethod
    def from_element(cls, data, session=None):
        '''
        Convert XML / Element to object

        Parameters
        ----------
        data : xml-string or Element
            The element to convert
        session : Session, optional
            The requests.Session object

        Returns
        -------
        :class:`ComputeWindow`

        '''
        data = ensure_element(data)
        out = super(ComputeWindow, cls).from_element(data, session=session)

        for item in data.findall('./context-plugin'):
            out.context_plugin = ContextPlugin(item.attrib['name'],
                                               item.attrib['function'])

        for item in data.findall('./output/field-expr'):
            out.field_expressions.append(item.text)

        for item in data.findall('./output/field-plug'):
            out.field_plugins.append(FieldPlugin(item.attrib['plugin'],
                                                 item.attrib['function']))

        return out

    from_xml = from_element

    def to_element(self, query=None):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = Window.to_element(self, query=query)

        if self.context_plugin is not None:
            xml.add_elem(out, 'context-plugin',
                         attrib=dict(name=self.context_plugin.name,
                                     function=self.context_plugin.function))

        schema = out.find('./schema')
        if schema is not None:
            out.remove(schema)
            out.append(schema)

        output = xml.add_elem(out, 'output')

        for item in self.field_expressions:
            xml.add_elem(output, 'field-expr', text_content=item)

        for item in self.field_plugins:
            xml.add_elem(output, 'field-plug',
                         attrib=dict(plugin=item.plugin, function=item.function))

        connectors_to_end(out)

        return out

    def set_context_plugin(self, name, function):
        '''
        Set a context plugin

        Parameters
        ----------
        name : string
            The shared library that contains the context–generation function
        function : string
            The function that, when called, returns a new derived context
            for the window’s handler routines.

        '''
        self.context_plugin = ContextPlugin(name, function)

    def add_field_expressions(self, *expr):
        '''
        Add new field expressions

        Parameters
        ----------
        *expr : one-or-more-strings
            The expressions to add

        '''
        for exp in expr:
            self.field_expressions.append(exp)

    add_field_expression = add_field_expressions

    add_field_expr = add_field_expressions

    add_field_exprs = add_field_expressions

    def add_field_plugin(self, plugin, function):
        '''
        Add a field plugin

        Parameters
        ----------
        plugin : string
            The name of the plugin
        function : string or list-of-strings
            The name(s) of the function

        '''
        if isinstance(function, six.string_types):
            function = [function]
        for func in function:
            self.field_plugins.append(FieldPlugin(plugin, func))
