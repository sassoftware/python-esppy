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

from .base import Window, attribute
from .features import SchemaFeature
from .utils import get_args, ensure_element, connectors_to_end
from ..utils import xml


class FieldPlugin(object):
    '''
    Aggregate field plugin

    Parameters
    ----------
    plugin : string
        Name of the shared library.
    function : string
        Name of the function in the shared library.
    additive : bool, optional
        Specify for Aggregate windows only. Defaults to false.
    additive_insert_only : bool, optional
        Specify for Aggregate windows only. Defaults to false.

    Returns
    -------
    :class:`FieldPlugin`

    '''

    def __init__(self, plugin, function, additive=False, additive_insert_only=False):
        self.plugin = plugin
        self.function = function
        self.additive = additive
        self.additive_insert_only = additive_insert_only

    def copy(self, deep=False):
        return type(self)(self.plugin, self.function, additive=self.additive,
                          additive_insert_only=self.additive_insert_only)


class AggregateWindow(Window, SchemaFeature):
    '''
    Aggregate window

    Parameters
    ----------
    name : string
        The name of the window
    schema : Schema
        The schema of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for the
        window and false disables it.
    description : string, optional
        Description of the window
    output_insert_only : bool, optional
        When true, prevents the window from passing non-insert events to
        other windows.
    collapse_updates : bool, optional
        When true, multiple update blocks are collapsed into a single
        update block
    pulse_interval : string, optional
        Output a canonical batch of updates at the specified interval.
    exp_max_string : int, optional
        Specifies the maximum size of strings that the expression engine
        uses for the window. Default value is 1024.
    index_type : string, optional
        Index type for the window.
        Valid values: 'rbtree', 'hash', 'ln_hash', 'cl_hash', 'fw_hash', 'empty'
    pubsub_index_type : int, optional
        Publish/subscribe index type.  Valid values are the same as for the
        `index_type` parameter.

    Attributes
    ----------
    field_expressions : list-of-FieldExpressions
        Specifies Expression Engine Language (EEL) expressions assigned
        to a field.
    field_plugins : list-of-FieldPlugins
        Functions in a shared library whose returned value is assigned
        to a field.

    Returns
    -------
    :class:`AggregateWindow`

    '''

    window_type = 'aggregate'

    def __init__(self, name=None, schema=None, pubsub=None, description=None,
                 output_insert_only=None, collapse_updates=None,
                 pulse_interval=None, exp_max_string=None, index_type=None,
                 pubsub_index_type=None):
        Window.__init__(self, **get_args(locals()))
        self.field_expressions = []
        self.field_plugins = []

    def copy(self, deep=False):
        out = Window.copy(self, deep=deep)
        out.field_expressions = list(self.field_expressions)
        if deep:
            out.field_plugins = []
            for item in self.field_plugins:
                out.field_plugins.append(item.copy(deep=deep))
        else:
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
        :class:`AggregateWindow`

        '''
        out = super(AggregateWindow, cls).from_element(data, session=session)

        for item in data.findall('./output/field-expr'):
            out.field_expressions.append(item.text)
        for item in data.findall('./output/field-plug'):
            attrs = item.attrib
            out.field_plugins.append(
                FieldPlugin(attrs['plugin'],
                            attrs['function'],
                            additive=attrs.get('additive', 'f').startswith('t'),
                            additive_insert_only=attrs.get('additive_insert_only',
                                                           'f').startswith('t')))
        return out

    from_xml = from_element

    def to_element(self, query=None):
        '''
        Convert object to Element

        Parameters
        ----------
        query : string, optional
            Name of the continuous query

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = Window.to_element(self, query=None)

        if self.field_expressions or self.field_plugins:
            output = xml.add_elem(out, 'output')

            for item in self.field_expressions:
                xml.add_elem(output, 'field-expr', text_content=item)

            for item in self.field_plugins:
                xml.add_elem(output, 'field-plug',
                             attrib=dict(plugin=item.plugin,
                                         function=item.function,
                                         additive=item.additive,
                                         additive_insert_only=item.additive_insert_only))

        connectors_to_end(out)

        return out

    def to_xml(self, pretty=False):
        '''
        Convert object to XML

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)

    def add_field_expressions(self, *expr):
        '''
        Add aggregate field expression

        Parameters
        ----------
        expr : string
            The aggregation expression

        '''
        for item in expr:
            self.field_expressions.append(item)

    add_field_expr = add_field_expressions

    add_field_exprs = add_field_expressions

    add_field_expression = add_field_expressions

    def add_field_plugin(self, plugin, function, additive=False,
                         additive_insert_only=False):
        '''
        Add aggregate field plugin

        Parameters
        ----------
        plugin : string
            Name of the shared library.
        function : string
            Name of the function in the shared library.
        additive : bool, optional
            Specify for Aggregate windows only. Defaults to false.
        additive_insert_only : bool, optional
            Specify for Aggregate windows only. Defaults to false.

        '''
        self.field_plugins.append(FieldPlugin(plugin, function, additive=additive,
                                              additive_insert_only=additive_insert_only))
