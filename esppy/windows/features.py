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

import collections
import copy
import re
import six
from .utils import ensure_element
from ..connectors import Connector
from ..connectors.base import get_connector_class
from ..schema import Schema
from ..utils.data import gen_name
from ..utils import xml


class InitializeExpression(object):
    '''
    Initialization expression

    Parameters
    ----------
    expr : string
        The initializer expression
    type : string, optional
        The return type of the initializer
    funcs : dict, optional
        User-defined functions to create.  The format of the dictionary
        should be {'func-name:return-type': 'code'}.

    Returns
    -------
    :class:`InitializeExpression`

    '''

    def __init__(self, expr=None, type=None, funcs=None):
        self.expr = expr
        self.type = type
        if not funcs:
            self.funcs = {}
        else:
            self.funcs = dict(funcs)

    def copy(self, deep=False):
        return type(self)(expr=self.expr, type=self.type, funcs=self.funcs)

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
        :class:`InitializeExpression`

        '''
        data = ensure_element(data)

        init_type = 'double'
        init_expr = None
        for init in data.findall('./initializer'):
            init_type = init.attrib['type']
            init_expr = init.text

        funcs = {}
        for func in data.findall('./udfs/udf'):
            funcs['%s:%s' % (func.attrib['name'], func.attrib['type'])] = func.text

        return cls(expr=init_expr, type=init_type, funcs=funcs)

    from_xml = from_element

    def to_element(self):
        '''
        Convert InitializeExpression to Element definition

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('expr-initialize')

        if self.expr and self.type:
            xml.add_elem(out, 'initializer',
                         attrib=dict(type=self.type),
                         text_content=self.expr)

        if self.funcs:
            udfs = xml.add_elem(out, 'udfs')
            for key, value in six.iteritems(self.funcs):
                name, dtype = key.split(':')
                xml.add_elem(udfs, 'udf', attrib=dict(name=name, type=dtype),
                             text_content=value)

        return out

    def to_xml(self, pretty=False):
        '''
        Convert InitializeExpression to XML definition

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class SplitterExpression(object):
    '''
    Add an expression to direct events to one of n different output slots

    Parameters
    ----------
    expr : string
        The splitter expression
    init_expr : string, optional
        An expression used to initialize variables
    init_type : string, optional
        The return type of the initializer
    init_funcs : dict, optional
        User-defined functions to create.  The format of the dictionary
        should be {'func-name:return-type': 'code'}.

    Returns
    -------
    :class:`SplitterExpression`

    '''

    def __init__(self, expr, init_expr=None, init_type=None, init_funcs=None):
        self.expr = expr
        if init_expr or init_type or init_funcs:
            self.initializer = InitializeExpression(expr=init_expr,
                                                    type=init_type,
                                                    funcs=init_funcs)
        else:
            self.initializer = None

    def copy(self, deep=False):
        out = type(self)(self.expr)
        if self.initializer is not None:
            out.initializer = self.initializer.copy(deep=deep)
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
        :class:`SplitterExpression`

        '''
        data = ensure_element(data)

        expr = None

        for exp in data.findall('./expression'):
            expr = exp.text

        init_type = None
        init_expr = None
        for init in data.findall('./expr-initialize/initializer'):
            init_type = init.attrib['type']
            init_expr = init.text

        funcs = {}
        for func in data.findall('./expr-initialize/udfs/udf'):
            funcs['%s:%s' % (func.attrib['name'], func.attrib['type'])] = func.text

        return cls(expr, init_type=init_type, init_expr=init_expr, init_funcs=funcs)

    from_xml = from_element

    def to_element(self):
        '''
        Convert the SplitterExpression to an Element definition

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('splitter-expr')

        if self.initializer is not None:
            xml.add_elem(out, self.initializer.to_element())

        xml.add_elem(out, 'expression', text_content=self.expr)

        return out

    def to_xml(self, pretty=False):
        '''
        Convert SplitterExpression to XML definition

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class SplitterPlugin(object):
    '''
    Create a splitter function using a shared library and function name

    Parameters
    ----------
    name : string
        Name of the shared library that contains the function
    function : string
        Name of the function in the shared library

    Returns
    -------
    :class:`SplitterPlugin`

    '''

    def __init__(self, name, function):
        self.name = name
        self.function = function

    def copy(self, deep=False):
        return type(self)(self.name, self.function)

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
        :class:`SplitterPlugin`

        '''
        data = ensure_element(data)
        return cls(data.attrib['name'], data.attrib['function'])

    from_xml = from_element

    def to_element(self):
        '''
        Convert SplitterPlugin to Element definition

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('splitter-plug',
                            attrib=dict(name=self.name, function=self.function))

    def to_xml(self, pretty=False):
        '''
        Convert SplitterPlugin to XML definition

        Parameters
        ----------
        pretty : bool, optional
            Should whitespace be added for readability?

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(), pretty=pretty)


class FinalizedCallback(object):
    '''
    Finalized Callback

    Parameters
    ----------
    name : string
        Path to the library with the callback function
    function : string
        Name of the callback function

    Returns
    -------
    :class:`FinalizedCallback`

    '''

    def __init__(self, name, function):
        self.name = name
        self.function = function

    def copy(self, deep=False):
        return type(self)(self.name, self.function)

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
        :class:`FinalizedCallback`

        '''
        data = ensure_element(data)

        out = None
        for item in data.findall('./finalized-callback'):
            out = cls(item.attrib['name'], item.attrib['function'])
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('finalized-callback',
                            attrib=dict(name=self.name, function=self.function))

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


class Retention(object):
    '''
    Retention

    Parameters
    ----------
    type : string
        Retention type.
        Valid Values: bytime_jumping, bytime_jumping_lookback,
        bytime_sliding, bycount_jumping, bycount_sliding
    value : string
        Retention value
    field : string, optional
        Specifies the name of a field of type datetime or timestamp
    unit : string, optional
        Specifies the unit of the lookback period for
        bytime_jumping_lookback retention policies.

    Returns
    -------
    :class:`Retention`

    '''

    def __init__(self, type, value, field=None, unit=None):
        self.type = type
        self.value = value
        self.field = field
        self.unit = unit

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
        :class:`Retention`

        '''
        data = ensure_element(data)
        return cls(data.attrib['type'], data.text,
                   field=data.attrib.get('field'),
                   unit=data.attrib.get('unit'))

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('retention', attrib=dict(type=self.type,
                            field=self.field, unit=self.unit),
                            text_content=self.value)

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

    def copy(self, deep=False):
        return type(self)(self.type, self.value, field=self.field, unit=self.unit)


class MASWindowMap(object):
    '''
    MAS Window Map

    Parameters
    ----------
    module : string
        Module name
    source : string
        Input window
    revision : string, optional
        Module revision number
    function : string, optional
        Function in the module

    Returns
    -------
    :class:`MASWindowMap`

    '''

    def __init__(self, module, source, revision='0', function=None):
        self.module = module
        self.source = source
        self.revision = revision
        self.function = function

    def copy(self, deep=False):
        return type(self)(self.module, self.source, revision=self.revision,
                          function=self.function)

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
        :class:`FieldExpression`

        '''
        data = ensure_element(data)
        return cls(data.attrib['module'], data.attrib['source'],
                   revision=data.attrib.get('revision', '0'),
                   function=data.attrib.get('function', None))

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('window-map', attrib=dict(module=self.module,
                                                      source=self.source,
                                                      revision=self.revision,
                                                      function=self.function))

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


class Model(object):
    '''
    Model

    Parameters
    ----------
    parameters : dict, optional
        Parameters
    input_map : dict, optional
        Input mappings
    output_map : dict, optional
        Output mappings

    Returns
    -------
    :class:`Model`

    '''

    def __init__(self, parameters=None, input_map=None, output_map=None):
        self.parameters = dict(parameters or {})
        self.input_map = dict(input_map or {})
        self.output_map = dict(output_map or {})

    def __repr__(self):
        return str(self)

    def set_inputs(self, **kwargs):
        '''
        Set input map fields

        Parameters
        ----------
        **kwargs : keyword arguments
            The key / value pairs of input names and values

        '''
        self.input_map.update({k: v for k, v in kwargs.items() if v is not None})

    def set_outputs(self, **kwargs):
        '''
        Set output map fields

        Parameters
        ----------
        **kwargs : keyword arguments
            The key / value pairs of input names and values

        '''
        self.output_map.update({k: v for k, v in kwargs.items() if v is not None})


class OnlineModel(Model):
    '''
    Online model

    Parameters
    ----------
    algorithm : string
        The name of the algorithm
    parameters : dict, optional
        Parameters
    input_map : dict, optional
        Input mappings
    output_map : dict, optional
        Output mappings

    Returns
    -------
    :class:`OnlineModel`

    '''

    def __init__(self, algorithm, parameters=None, input_map=None, output_map=None):
        Model.__init__(self, parameters=parameters, input_map=input_map, output_map=output_map)
        self.algorithm = algorithm

    def copy(self, deep=False):
        return type(self)(self.algorithm, 
                          parameters=self.parameters,
                          input_map=self.input_map,
                          output_map=self.output_map)

    def __str__(self):
        maps = []
        if self.parameters:
            maps.append('parameters=%s' % self.parameters)
        if self.input_map:
            maps.append('input_map=%s' % self.input_map)
        if self.output_map:
            maps.append('output_map=%s' % self.output_map)
        return '%s(%s, %s)' % (type(self).__name__, self.algorithm, ', '.join(maps))

    def __repr__(self):
        return str(self)

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
        :class:`FieldExpression`

        '''
        data = ensure_element(data)

        out = cls(data.attrib['algorithm'])

        for prop in data.findall('./parameters/properties/property'):
            out.parameters[prop.attrib['name']] = prop.text
        for prop in data.findall('./input-map/properties/property'):
            out.input_map[prop.attrib['name']] = prop.text
        for prop in data.findall('./output-map/properties/property'):
            out.output_map[prop.attrib['name']] = prop.text

        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('online', attrib=dict(algorithm=self.algorithm))

        if self.parameters != None and len(self.parameters) > 0:
            parms = xml.add_elem(out,'parameters')
            xml.add_properties(parms, self.parameters, verbatim=True, bool_as_int=True)

        if self.input_map:
            imap = xml.add_elem(out, 'input-map')
            xml.add_properties(imap, self.input_map, verbatim=True, bool_as_int=True)

        if self.output_map:
            omap = xml.add_elem(out, 'output-map')
            xml.add_properties(omap, self.output_map, verbatim=True, bool_as_int=True)

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


class OfflineModel(Model):
    '''
    Offline model

    Parameters
    ----------
    model_type : string, optional
        Model type
    parameters : dict, optional
        Parameters
    input_map : dict, optional
        Input mappings
    output_map : dict, optional
        Output mappings

    Returns
    -------
    :class:`OfflineModel`

    '''

    def __init__(self, model_type='astore', parameters=None, input_map=None, output_map=None):
        Model.__init__(self, parameters=parameters, input_map=input_map, output_map=output_map)
        self.model_type = model_type

    def copy(self, deep=False):
        return type(self)(model_type=self.model_type,
                          parameters=self.parameters,
                          input_map=self.input_map,
                          output_map=self.output_map)

    def __str__(self):
        maps = []
        if self.parameters:
            maps.append('parameters=%s' % self.parameters)
        if self.input_map:
            maps.append('input_map=%s' % self.input_map)
        if self.output_map:
            maps.append('output_map=%s' % self.output_map)
        return '%s(%s, %s, model_type=%s, %s)' % (type(self).__name__,
                                                  repr(self.model_type),
                                                  ', '.join(maps))

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
        :class:`FieldExpression`

        '''
        data = ensure_element(data)

        out = cls(model_type=data.attrib['model-type'])

        for prop in data.findall('./parameters/properties/property'):
            out.parameters[prop.attrib['name']] = prop.text
        for prop in data.findall('./input-map/properties/property'):
            out.input_map[prop.attrib['name']] = prop.text
        for prop in data.findall('./output-map/properties/property'):
            out.output_map[prop.attrib['name']] = prop.text

        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('offline', attrib=dict(model_type=self.model_type))

        if self.parameters != None and len(self.parameters) > 0:
            parms = xml.add_elem(out,'parameters')
            xml.add_properties(parms, self.parameters, verbatim=True, bool_as_int=True)

        if self.input_map:
            imap = xml.add_elem(out, 'input-map')
            xml.add_properties(imap, self.input_map, verbatim=True, bool_as_int=True)

        if self.output_map:
            omap = xml.add_elem(out, 'output-map')
            xml.add_properties(omap, self.output_map, verbatim=True, bool_as_int=True)

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


class Plugin(object):
    '''
    Plugin

    Parameters
    ----------
    name : string
        Path to .so / .dll
    function : string
        Name of the function in the library
    context_name : string, optional
        The shared library that contains the context–generation function.
    context_function : string, optional
        The function that, when called, returns a new derived context
        for the window’s handler routines.

    Returns
    -------
    :class:`Plugin`

    '''

    def __init__(self, name, function, context_name=None,
                 context_function=None):
        self.name = name
        self.function = function
        self.context_name = context_name
        self.context_function = context_function

    def copy(self, deep=False):
        return type(self)(self.name, self.function,
                          context_name=self.context_name,
                          context_function=self.context_function)

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
        :class:`Plugin`

        '''
        data = ensure_element(data)

        context_name = None
        context_function = None
        for ctxt in data.findall('./context-plugin'):
            context_name = ctxt.attrib.get('name')
            context_function = ctxt.attrib.get('function')

        return cls(data.attrib['name'], data.attrib['function'],
                   context_name=context_name,
                   context_function=context_function)

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('plugin', attrib=dict(name=self.name,
                                                 function=self.function))
        if self.context_name and self.context_function:
            xml.add_elem(out, 'context-plugin',
                         attrib=dict(name=self.context_name,
                                     function=self.context_function))
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


class PatternEvent(object):
    '''
    Pattern Event

    Parameters
    ----------
    source : string
        Input window
    name : string
        Name of the event
    expr : string
        Event where clause

    Returns
    -------
    :class:`PatternEvent`

    '''

    def __init__(self, source, name, expr):
        self.source = source
        self.name = name
        self.expr = expr

    def copy(self, deep=False):
        return type(self)(self.source, self.name, self.expr)


class PatternFieldExpression(object):
    '''
    Pattern Field Expression

    Parameters
    ----------
    node : string
        Name of the event
    expr : string
        The expression

    Returns
    -------
    :class:`PatternFieldExpression`

    '''

    def __init__(self, node, expr):
        self.expr = expr
        self.node = node

    def copy(self, deep=False):
        return type(self)(self.node, self.expr)


class PatternFieldSelection(object):
    '''
    Pattern Field Selection

    Parameters
    ----------
    node : string
        Name of the event
    name : string
        Field name to select from event

    Returns
    -------
    :class:`PatternFieldSelection`

    '''

    def __init__(self, node, name):
        self.node = node
        self.name = name

    def copy(self, deep=False):
        return type(self)(self.node, self.name)


class PatternTimeField(object):
    '''
    Pattern Time Field

    Parameters
    ----------
    field : string
        Field name to use to derive expiration time
    source : string
        Window the time field is in

    Returns
    -------
    :class:`PatternTimeField`

    '''

    def __init__(self, field, source):
        self.field = field
        self.source = source

    def copy(self, deep=False):
        return type(self)(self.field, self.source)


class Pattern(object):
    '''
    Pattern

    Parameters
    ----------
    name : string
        Name for user-interface tracking
    index : string or list-of-strings, optional
        Optional index
    is_active : boolean, optional
        Is the pattern enabled?

    '''

    def __init__(self, name=None, index=None, is_active=None):
        self.name = name
        if index is None:
            self.index = []
        elif isinstance(index, six.string_types):
            self.index = re.split(r'\s*,\s*', index.strip())
        else:
            self.index = list(index)
        self.is_active = is_active
        self.events = []
        self.logic = ''
        self.output = []
        self.timefields = []

    def copy(self, deep=False):
        out = type(self)(name=self.name, index=self.index, is_active=self.is_active)
        out.logic = self.logic
        if deep:
            out.events = [x.copy(deep=deep) for x in self.events]
            out.output = [x.copy(deep=deep) for x in self.output]
            out.timefields = [x.copy(deep=deep) for x in self.timefields]
        else:
            out.events = list(self.events)
            out.output = list(self.output)
            out.timefields = list(self.timefields)
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
        :class:`FieldExpression`

        '''
        data = ensure_element(data)

        out = cls()

        out.name = data.attrib.get('name')
        out.index = re.split(r'\s*,\s*', data.attrib.get('index', '').strip())
        out.is_active = data.attrib.get('active', None)

        for event in data.findall('./events/event'):
            out.add_event(event.attrib.get('source'), event.attrib.get('name'),
                          event.text)

        for logic in data.findall('./logic'):
            out.set_logic(logic.text)

        for field_expr in data.findall('./output/field-expr'):
            out.add_field_expr(field_expr.text, **field_expr.attrib)

        for field_selection in data.findall('./output/field-selection'):
            out.add_field_selection(**field_selection.attrib)

        for timefield in data.findall('./timefields/timefield'):
            out.add_timefield(**timefield.attrib)

        return out

    from_xml = from_element

    def add_event(self, source, name, expr):
        '''
        Add a Pattern Event

        Parameters
        ----------
        source : string
            Input window
        name : string
            Name of the event
        expr : string
            Event where clause
        '''
        self.events.append(PatternEvent(source, name, expr))

    def add_field_expression(self, expr, node=None):
        '''
        Add a Pattern Field Expression

        Parameters
        ----------
        node : string
            Name of the event
        expr : string
            The expression

        '''
        self.output.append(PatternFieldExpression(node, expr))

    add_field_expr = add_field_expression

    def set_logic(self, expr):
        '''
        Set logic expression

        Parameters
        ----------
        expr : string
            Operator tree as an expression

        '''
        self.logic = expr

    def add_field_selection(self, node, name):
        '''
        Add a Pattern Field Selection

        Parameters
        ----------
        node : string
            Name of the event
        name : string
            Field name to select from event

        '''
        self.output.append(PatternFieldSelection(node, name))

    def add_timefield(self, field, source):
        '''
        Add a Pattern Time Field

        Parameters
        ----------
        field : string
            Field name to use to derive expiration time
        source : string
            Window the time field is in

        '''
        self.timefields.append(PatternTimeField(field, source))

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('pattern', attrib=dict(name=self.name, is_active=self.is_active,
                                                  index=','.join(self.index) or None))

        if self.events:
            events = xml.add_elem(out, 'events')
            for item in self.events:
                xml.add_elem(events, 'event', text_content=item.expr,
                             attrib=dict(source=item.source, name=item.name))

        if self.logic:
            xml.add_elem(out, 'logic', text_content=self.logic)

        if self.output:
            output = xml.add_elem(out, 'output')
            for item in self.output:
                if isinstance(item, PatternFieldExpression):
                    xml.add_elem(output, 'field-expr', text_content=item.expr,
                                 attrib=dict(node=item.node))
                elif isinstance(item, PatternFieldSelection):
                    xml.add_elem(output, 'field-selection',
                                 attrib=dict(node=item.node, name=item.name))

        if self.timefields:
            timefields = xml.add_elem(out, 'timefields')
            for item in self.timefields:
                xml.add_elem(timefields, 'timefield',
                             attrib=dict(field=item.field, source=item.source))

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


class CXXPluginContext(object):
    '''
    C++ Plugin Context

    Parameters
    ----------
    cxx_name : string
        Path to the .so / .dll that contains the function
    cxx_function : string
        Name of the function in the library
    **proprties : keyword-arguments, optional
        Property list

    Returns
    -------
    :class:`CXXPluginContext`

    '''

    def __init__(self, cxx_name, cxx_function, **properties):
        self.name = cxx_name
        self.function = cxx_function
        self.properties = dict(properties)

    def copy(self, deep=False):
        return type(self)(self.name, self.function, **self.properties)

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
        :class:`CXXPluginContext`

        '''
        data = ensure_element(data)

        out = cls(data.attrib['name'], data.attrib['function'])

        for item in data.findall('./properties/property'):
            out.properties[item.attrib['name']] = item.text

        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('cxx-plugin-context',
                           attrib=dict(name=self.name, function=self.function))
        if self.properties:
            xml.add_properties(out, self.properties, bool_as_int=True)
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

    def set_properties(self, **properties):
        '''
        Set plugin properties

        Parameters
        ----------
        **properties : keyword-arguments, optional
            The properties to set

        '''
        self.properties.update(properties)


class CXXPlugin(object):
    '''
    C++ Plugin

    Parameters
    ----------
    source : string
        Input window
    name : string
        Path to the .so / .dll
    function : string
        Function name in the library

    Returns
    -------
    :class:`CXXPlugin`

    '''

    def __init__(self, source, name, function):
        self.source = source
        self.name = name
        self.function = function

    def copy(self, deep=False):
        return type(self)(self.source, self.name, self.function)

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
        :class:`CXXPlugin`

        '''
        data = ensure_element(data)
        return cls(data.attrib['source'], data.attrib['name'],
                   data.attrib['function'])

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('cxx-plugin', attrib=dict(source=self.source,
                                                      name=self.name,
                                                      function=self.function))

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


class DS2TableServer(object):
    '''
    DS2 Table Server

    Parameters
    ----------
    source : string
        Input window
    code : string, optional
        Inline block of code
    code_file : string, optional
        File containing code

    Returns
    -------
    :class:`DS2TableServer`

    '''

    def __init__(self, source, code=None, code_file=None):
        self.source = source
        self.code = code
        self.code_file = code_file

    def copy(self, deep=False):
        return type(self)(self.source, code=self.code, code_file=self.code_file)

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
        :class:`FieldExpression`

        '''
        data = ensure_element(data)
        out = cls(data.attrib['source'])
        for item in data.findall('./code'):
            out.code = item.text
        for item in data.findall('./code_file'):
            out.code_file = item.text
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('ds2-tableserver', attrib=dict(source=self.source))
        if self.code is not None:
            xml.add_elem(out, 'code', text_content=self.code)
        elif self.code_file is not None:
            xml.add_elem(out, 'code-file', text_content=self.code_file)
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


class DSExternal(object):
    '''
    DS External

    Parameters
    ----------
    source : string
        Input window
    code : string, optional
        Inline block of code
    code_file : string, optional
        File containing code
    trace : bool, optional
        Print debugging information
    connection_timeout : int, optional
        Time for SAS to answer back
    max_string_length : int, optional
        Maximum string ESP will pass

    Returns
    -------
    :class:`DSExternal`

    '''

    def __init__(self, source, code=None, code_file=None, trace=False,
                 connection_timeout=300, max_string_length=32000):
        self.source = source
        self.code = code
        self.code_file = code_file
        self.trace = trace
        self.connection_timeout = connection_timeout
        self.max_string_length = max_string_length

    def copy(self, deep=False):
        return type(self)(self.source, code=self.code, code_file=self.code_file,
                          trace=self.trace, connection_timeout=self.connection_timeout,
                          max_string_length=self.max_string_length)

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
        :class:`FieldExpression`

        '''
        data = ensure_element(data)
        out = cls(data.attrib['source'], trace=data.attrib.get('trace', False),
                  connection_timeout=int(data.attrib.get('connection_timeout', 300)),
                  max_string_length=int(data.attrib.get('max_string_length', 32000)))
        for item in data.findall('./code'):
            out.set_code(item.text)
        for item in data.findall('./code_file'):
            out.set_code_file(item.text)
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('ds-external', attrib=dict(trace=self.trace,
                           source=self.source,
                           connection_timeout=self.connection_timeout,
                           max_string_length=self.max_string_length))
        if self.code is not None:
            xml.add_elem(out, 'code', text_content=self.code)
        elif self.code_file is not None:
            xml.add_elem(out, 'code-file', text_content=self.code_file)
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


class WindowFeature(object):
    '''
    Base Window Feature Class

    '''

    def _feature_from_element(self, data):
        '''
        Convert Element to object

        Parameters
        ----------
        data : string or Element
            The Element to convert

        Returns
        -------
        object

        '''
        return

    def _feature_from_xml(self, data):
        '''
        Convert XML to object

        Parameters
        ----------
        data : string or Element
            The XML to convert

        Returns
        -------
        object

        '''
        return self.from_element(xml.from_xml(data))

    def _feature_to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return

    def _feature_to_xml(self, pretty=False):
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
        out = self.to_element()
        if out is not None:
            out = xml.to_xml(out, pretty=pretty)
        return out

    def _copy_feature(self, other, deep=False):
        raise NotImplementedError


class SplitterExpressionFeature(WindowFeature):
    '''
    Splitter Expression Feature

    '''

    def __init__(self):
        self.splitter = None

    def _feature_from_element(self, data):
        for item in data.findall('./splitter-expr'):
            self.splitter = SplitterExpression.from_element(item, session=self.session)

    def _feature_to_element(self):
        if not isinstance(self.splitter, SplitterExpression):
            return
        return self.splitter.to_element()

    def _copy_feature(self, other, deep=False):
        if isinstance(self.splitter, SplitterExpression):
            other.splitter = self.splitter.copy()

    def set_splitter_expr(self, expr, init_type='double', init_expr=None,
                          init_funcs=None):
        '''
        Set expression to direct events to one of n different output slots

        Parameters
        ----------
        expr : string
            The expression to be processed
        init_type : string, optional
            The data type of the return value of the initializer
        init_expr : string, optional
            The initialization expression
        init_funcs : dict, optional
            User-defined functions to create.  The format of the dictionary
            should be {'func-name:return-type': 'code'}.

        '''
        self.splitter = SplitterExpression(expr, init_expr=init_expr,
                                           init_type=init_type, init_funcs=init_funcs)


class SplitterPluginFeature(WindowFeature):
    '''
    Splitter Plugin Feature

    '''

    def __init__(self):
        self.splitter = None

    def _feature_from_element(self, data):
        for item in data.findall('./splitter-plug'):
            self.splitter = SplitterPlugin.from_element(item, session=self.session)

    def _feature_to_element(self):
        if not isinstance(self.splitter, SplitterPlugin):
            return
        return self.splitter.to_element()

    def _copy_feature(self, other, deep=False):
        if isinstance(self.splitter, SplitterPlugin):
            other.splitter = self.splitter.copy(deep=deep)

    def set_splitter_plugin(self, name, function):
        '''
        Set splitter function using a shared library and function name

        Parameters
        ----------
        name : string
            Name of the shared library that contains the function
        function : string
            Name of the function in the shared library

        '''
        self.splitter = SplitterPlugin(name, function)


class FinalizedCallbackFeature(WindowFeature):
    '''
    Finalized Callback Feature

    '''

    def __init__(self):
        self.finalized_callback = None

    def _feature_from_element(self, data):
        self.finalized_callback = FinalizedCallback.from_element(data,
                                                                 session=self.session)

    def _feature_to_element(self):
        if self.finalized_callback is not None:
            return self.finalized_callback.to_element()

    def _copy_feature(self, other, deep=False):
        if self.finalized_callback is not None:
            other.finalized_callback = self.finalized_callback.copy(deep=deep)

    def set_finalized_callback(self, name, function):
        '''
        Set Finalized Callback

        Parameters
        ----------
        name : string
            Path to the library with the callback function
        function : string
            Name of the callback function

        '''
        self.finalized_callback = FinalizedCallback(name, function)


class RetentionFeature(WindowFeature):
    '''
    Retention Feature

    '''

    def __init__(self):
        self.retention = None

    def _feature_from_element(self, data):
        for item in data.findall('./retention'):
            self.retention = Retention.from_element(item, session=self.session)

    def _feature_to_element(self):
        if self.retention is not None:
            return self.retention.to_element()

    def _copy_feature(self, other, deep=False):
        if self.retention is not None:
            other.retention = self.retention.copy(deep=deep)

    def set_retention(self, type, value, field=None, unit=None):
        '''
        Retention

        Parameters
        ----------
        type : string
            Retention type.
            Valid Values: bytime_jumping, bytime_jumping_lookback,
            bytime_sliding, bycount_jumping, bycount_sliding
        value : string
            Retention value
        field : string, optional
            Specifies the name of a field of type datetime or timestamp
        unit : string, optional
            Specifies the unit of the lookback period for
            bytime_jumping_lookback retention policies.

        '''
        self.retention = Retention(type, value, field=field, unit=unit)


class ConnectorsFeature(WindowFeature):
    '''
    Connections Feature

    '''

    def __init__(self):
        self.connectors = []

    def _feature_from_element(self, data):
        for item in data.findall('./connectors/connector'):
            self.connectors.append(Connector.from_xml(item,
                                                      session=self.session))

    def _feature_to_element(self):
        if not self.connectors:
            return
        out = xml.new_elem('connectors')
        for conn in self.connectors:
            xml.add_elem(out, conn.to_element())
        return out

    def _copy_feature(self, other, deep=False):
        if deep:
            other.connectors = []
            for conn in self.connectors:
                other.connectors.append(conn.copy(deep=deep))
        else:
            other.connectors = list(self.connectors)

    def add_connector(self, conn_cls, conn_name=None, conn_type=None,
                      is_active=None, properties=None, **kwargs):
        '''
        Add a connector to the window

        Parameters
        ----------
        conn_cls : string or Connector
            The connecter class name or Connector instance
        conn_name : string, optional
            The name of the connector.  See notes.
        conn_type : string, optional
            The type of the connector.  See notes.
        is_active : boolean, optional
            Is the connector enabled?
        properties : dict, optional
            Dictionary of connector properties.  See notes.
        **kwargs : keyword-arguments, optional
            Connector properties (these get merged with properties=).  See notes.

        Notes
        -----
        If the first argument is a Connector object, all other
        arguments are ignored.

        '''
        if isinstance(conn_cls, Connector):
            self.connectors.append(conn_cls)
        else:
            kwargs = dict(kwargs)
            if properties:
                kwargs.update(properties)
            out = get_connector_class(conn_cls, type=conn_type, properties=kwargs)
            out = out.from_parameters(conn_cls, name=conn_name, type=conn_type,
                                      is_active=is_active, properties=kwargs)
            self.connectors.append(out)


class ParametersFeature(WindowFeature):
    '''
    Parameters Feature

    '''

    def __init__(self):
        self.parameters = {}

    def _feature_from_element(self, data):
        for item in data.findall('./parameters/properties/property'):
            self.parameters[item.attrib['name']] = item.text

    def _feature_to_element(self):
        out = None
        if self.parameters:
            out = xml.new_elem('parameters')
            xml.add_properties(out, self.parameters, bool_as_int=True)
        return out

    def _copy_feature(self, other, deep=False):
        other.parameters = dict(self.parameters)

    def set_parameters(self, **parameters):
        '''
        Set parameters

        Parameters
        ----------
        **parameters : keyword-arguments, optional
            The parameters to set

        '''
        for key, value in six.iteritems(parameters):
            if value is None:
                self.parameters.pop(re.sub(r'_$', r'', key), None)
            else:
                self.parameters[re.sub(r'_$', r'', key)] = value


class InputMapFeature(WindowFeature):
    '''
    Input Map Feature

    '''

    def __init__(self):
        self.input_map = {}

    def _feature_from_element(self, data):
        for item in data.findall('./input-map/properties/property'):
            self.input_map[item.attrib['name']] = item.text

    def _feature_to_element(self):
        out = None

        def strip_types(value):
            if isinstance(value, (set, tuple, list)):
                return [x.split(':')[0].replace('*', '') for x in value]
            return value.split(':')[0].replace('*', '')

        if self.input_map:
            input_map = {k: strip_types(v) for k, v in self.input_map.items()
                         if v is not None}
            if input_map:
                out = xml.new_elem('input-map')
                xml.add_properties(out, input_map, bool_as_int=True)

        return out

    def _copy_feature(self, other, deep=False):
        other.input_map = dict(self.input_map)

    def set_inputs(self, **kwargs):
        '''
        Set input map fields

        Parameters
        ----------
        **kwargs : keyword arguments
            The key / value pairs of input names and values

        '''
        self.input_map.update(kwargs)


class OutputMapFeature(WindowFeature):
    '''
    Output Map Feature

    '''

    def __init__(self):
        self.output_map = {}

    def _feature_from_element(self, data):
        for item in data.findall('./output-map/properties/property'):
            self.output_map[item.attrib['name']] = item.text

    def _feature_to_element(self):
        out = None

        def strip_types(value):
            if isinstance(value, (set, tuple, list)):
                return [x.split(':')[0].replace('*', '') for x in value]
            return value.split(':')[0].replace('*', '')

        if self.output_map:
            output_map = {k: strip_types(v) for k, v in self.output_map.items()
                          if v is not None}
            if output_map:
                out = xml.new_elem('output-map')
                xml.add_properties(out, output_map, bool_as_int=True)

        return out

    def _copy_feature(self, other, deep=False):
        other.output_map = dict(self.output_map)

    def set_outputs(self, **kwargs):
        '''
        Set output map fields

        Parameters
        ----------
        **kwargs : keyword arguments
            The key / value pairs of output names and values

        '''
        self.output_map.update(kwargs)


class SchemaFeature(WindowFeature):
    '''
    Schema Feature

    '''

    def _feature_to_element(self):
        out = self.schema.to_element()
        if not self.schema.fields and not out.attrib:
            return
        return out

    def _copy_feature(self, other, deep=False):
        other.schema = self.schema.copy(deep=deep)


class MASMapFeature(WindowFeature):
    '''
    MAS Map Feature

    '''

    def __init__(self):
        self.mas_map = {}

    def _feature_from_element(self, data):
        for item in data.findall('./mas-map/window-map'):
            name = ':'.join([x for x in [item.attrib['module'],
                                         item.attrib['source'],
                                         item.attrib.get('function')]
                             if x is not None])
            self.mas_map[name] = MASWindowMap.from_element(item, session=self.session)

    def _feature_to_element(self):
        out = None
        if self.mas_map:
            out = xml.new_elem('mas-map')
            for value in six.itervalues(self.mas_map):
                xml.add_elem(out, value.to_element())
        return out

    def _copy_feature(self, other, deep=False):
        if deep:
            other.mas_map = {}
            for key, value in six.iteritems(self.mas_map):
                other.mas_map[key] = value.copy(deep=deep)
        else:
            other.mas_map = dict(self.mas_map)

    def add_mas_window_map(self, module, source, revision='0', function=None):
        '''
        Add MAS Window Map

        Parameters
        ----------
        module : string
            Module name
        source : string
            Input window
        revision : string, optional
            Module revision number
        function : string, optional
            Function in the module

        '''
        name = ':'.join([x for x in [module, source, revision, function]
                         if x is not None])
        self.mas_map[name] = MASWindowMap(module, source, revision=revision,
                                          function=function)


class ModelsFeature(WindowFeature):
    '''
    Models Feature

    '''

    def __init__(self):
        self.online_models = []
        self.offline_models = []

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./models/online'):
            self.online_models.append(OnlineModel.from_element(item, session=self.session))
        for item in data.findall('./models/offline'):
            self.offline_models.append(OfflineModel.from_element(item, session=self.session))

    def _feature_to_element(self):
        if not self.online_models and not self.offline_models:
            return

        out = xml.new_elem('models')

        for item in self.online_models:
            xml.add_elem(out, item.to_element())
        for item in self.offline_models:
            xml.add_elem(out, item.to_element())

        return out

    def _copy_feature(self, other, deep=False):
        if deep:
            other.online_models = []
            other.offline_models = []
            for item in self.online_models:
                other.online_models.append(item.copy(deep=deep))
            for item in self.offline_models:
                other.offline_models.append(item.copy(deep=deep))
        else:
            other.online_models = list(self.online_models)
            other.offline_models = list(self.offline_models)

    def set_outputs(self, model, **kwargs):
        '''
        Set model outputs

        Parameters
        ----------
        model : string
            The name / URL of the model
        **kwargs : keyword-arguments, optional
            The output mappings

        '''
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        for item in self.online_models:
            if item.algorithm == model:
                item.output_map.update(kwargs)
                return
        for item in self.offline_models:
            if item.reference == model:
                item.output_map.update(kwargs)
                return

    def set_inputs(self, model, **kwargs):
        '''
        Set model inputs

        Parameters
        ----------
        model : string
            The name / URL of the model
        **kwargs : keyword-arguments, optional
            The input mappings

        '''
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        for item in self.online_models:
            if item.algorithm == model:
                item.input_map.update(kwargs)
                return
        for item in self.offline_models:
            if item.reference == model:
                item.input_map.update(kwargs)
                return

    def add_online_model(self, algorithm, parameters=None,input_map=None, output_map=None):
        '''
        Online model

        Parameters
        ----------
        algorithm : string
            The name of the algorithm
        parameters : dict, optional
            Parameters
        input_map : dict, optional
            Input mappings
        output_map : dict, optional
            Output mappings

        '''
        self.online_models.append(OnlineModel(algorithm,
                                              parameters=parameters,
                                              input_map=input_map,
                                              output_map=output_map))

    def add_offline_model(self, model_type='astore', parameters=None, input_map=None, output_map=None):
        '''
        Offline model

        Parameters
        ----------
        model_type : string
            Model type
        input_map : dict, optional
            Input mappings
        output_map : dict, optional
            Output mappings

        '''
        # Only allow one
        print("PARMS: " + str(parameters))
        self.offline_models[:] = []
        self.offline_models.append(OfflineModel(model_type=model_type,
                                                parameters=parameters,
                                                input_map=input_map,
                                                output_map=output_map))


class InitializeExpressionFeature(WindowFeature):
    '''
    Initialize Expression Feature

    '''

    def __init__(self):
        self.expr_initializer = None

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./expr-initialize'):
            self.expr_initializer = InitializeExpression.from_element(item, session=self.session)

    def _feature_to_element(self):
        out = None
        if self.expr_initializer is not None:
            out = self.expr_initializer.to_element()
        return out

    def _copy_feature(self, other, deep=False):
        if self.expr_initializer is not None:
            other.expr_initializer = self.expr_initializer.copy(deep=deep)

    def set_expression_initializer(self, expr=None, type=None, funcs=None):
        '''
        Set initialization expression

        Parameters
        ----------
        expr : string, optional
            The initializer expression
        type : string, optional
            The return type of the initializer
        funcs : dict, optional
            User-defined functions to create.  The format of the dictionary
            should be {'func-name:return-type': 'code'}.

        '''
        self.expr_initializer = InitializeExpression(expr=expr, type=type, funcs=funcs)

    set_expr_initializer = set_expression_initializer


class ExpressionFeature(WindowFeature):
    '''
    Expression Feature

    '''

    def __init__(self):
        self.expression = None

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./expression'):
            self.expression = item.text

    def _feature_to_element(self):
        out = None
        if self.expression:
            out = xml.new_elem('expression', text_content=self.expression)
        return out

    def _copy_feature(self, other, deep=False):
        if self.expression:
            other.expression = self.expression

    def set_expression(self, expr):
        '''
        Set the expression

        Parameters
        ----------
        expr : string
           The expression value

        '''
        self.expression = expr


class PluginFeature(WindowFeature):
    '''
    Plugin Feature

    '''

    def __init__(self):
        self.plugin = None

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./plugin'):
            self.plugin = Plugin.from_xml(item, session=self.session)

    def _feature_to_element(self):
        out = None
        if self.plugin is not None:
            out = self.plugin.to_element()
        return out

    def _copy_feature(self, other, deep=False):
        if self.plugin is not None:
            other.plugin = self.plugin.copy(deep=deep)

    def set_plugin(self, name, function, context_name=None,
                   context_function=None):
        '''
        Set plugin

        Parameters
        ----------
        name : string
            Path to .so / .dll
        function : string
            Name of the function in the library
        context_name : string, optional
            The shared library that contains the context–generation function.
        context_function : string, optional
            The function that, when called, returns a new derived context
            for the window’s handler routines.

        '''
        self.plugin = Plugin(name, function, context_name=context_name,
                             context_function=context_function)


class PatternsFeature(WindowFeature):
    '''
    Patterns Feature

    '''

    def __init__(self):
        self.patterns = []

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./patterns/pattern'):
            self.patterns.append(Pattern.from_xml(item, session=self.session))

    def _feature_to_element(self):
        out = None
        if self.patterns:
            out = xml.new_elem('patterns')
            for item in self.patterns:
                xml.add_elem(out, item.to_element())
        return out

    def _copy_feature(self, other, deep=False):
        if self.patterns:
            if deep:
                other.patterns = []
                for item in self.patterns:
                    other.patterns.append(item.copy(deep=deep))
            else:
                other.patterns = list(self.patterns)

    def create_pattern(self, name=None, index=None, is_active=None):
        '''
        Create Pattern object and add it to the `patterns` list

        Parameters
        ----------
        name : string
            Name for user-interface tracking
        index : string or list-of-strings, optional
            Optional index
        is_active : boolean, optional
            Is the pattern enabled?

        Returns
        -------
        :class:`Pattern`

        '''
        out = Pattern(name=name, index=index, is_active=is_active)
        self.patterns.append(out)
        return out


class CXXPluginContextFeature(WindowFeature):
    '''
    C++ Plugin Context Feature

    '''

    def __init__(self):
        self.cxx_plugin_context = None

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./cxx-plugin-context'):
            self.cxx_plugin_context = CXXPluginContext.from_xml(item,
                                                                session=self.session)

    def _feature_to_element(self):
        out = None
        if self.cxx_plugin_context is not None:
            out = self.cxx_plugin_context.to_element()
        return out

    def _copy_feature(self, other, deep=False):
        if self.cxx_plugin_context is not None:
            other.cxx_plugin_context = self.cxx_plugin_context.copy(deep=deep)

    def set_cxx_plugin_context(self, cxx_name, cxx_function, **properties):
        '''
        Set C++ Plugin Context

        Parameters
        ----------
        cxx_name : string
            Path to the .so / .dll that contains the function
        cxx_function : string
            Name of the function in the library
        **proprties : keyword-arguments, optional
            Property list

        '''
        self.cxx_plugin_context = CXXPluginContext(cxx_name, cxx_function, **properties)


class CXXPluginFeature(WindowFeature):
    '''
    C++ Plugin Feature

    '''

    def __init__(self):
        self.cxx_plugins = []

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./cxx-plugin'):
            self.cxx_plugins.append(CXXPlugin.from_xml(item, session=self.session))

    def _feature_to_element(self):
        if not self.cxx_plugins:
            return
        out = []
        for item in self.cxx_plugins:
            out.append(item.to_element())
        return out

    def _copy_feature(self, other, deep=False):
        if self.cxx_plugins:
            if deep:
                other.cxx_plugins = [x.copy(deep=deep) for x in self.cxx_plugins]
            else:
                other.cxx_plugins = list(self.cxx_plugins)

    def add_cxx_plugin(self, source, name, function):
        '''
        Add a C++ Plugin

        Parameters
        ----------
        source : string
            Input window
        name : string
            Path to the .so / .dll
        function : string or list-of-strings
            Function name in the library

        '''
        if isinstance(function, six.string_types):
            function = [function]
        for item in function:
            self.cxx_plugins.append(CXXPlugin(source, name, item))

    add_cxx_plugins = add_cxx_plugin


class DS2TableServerFeature(WindowFeature):
    '''
    DS2 Table Server Feature

    '''

    def __init__(self):
        self.ds2_tableservers = []

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./ds2-tableserver'):
            self.ds2_tableservers.append(DS2TableServer.from_element(item,
                                                                     session=self.session))

    def _feature_to_element(self):
        if not self.ds2_tableservers:
            return
        out = []
        for item in self.ds2_tableservers:
            out.append(item.to_element())
        return out

    def _copy_feature(self, other, deep=False):
        if self.ds2_tableservers:
            if deep:
                other.ds2_tableservers = [x.copy(deep=deep) for x in self.ds2_tableservers]
            else:
                other.ds2_tableservers = list(self.ds2_tableservers)

    def add_ds2_tableserver(self, name, code=None, code_file=None):
        '''
        Add a DS2 Table Server

        Parameters
        ----------
        source : string
            Input window
        code : string, optional
            Inline block of code
        code_file : string, optional
            File containing code

        '''
        self.ds2_tableservers.append(DS2TableServer(name, code=code, code_file=code_file))


class DSExternalFeature(WindowFeature):
    '''
    DS External Feature

    '''

    def __init__(self):
        self.ds_externals = []

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./ds-external'):
            self.ds_externals.append(DSExternal.from_element(item, session=self.session))

    def _feature_to_element(self):
        if not self.ds_externals:
            return
        out = []
        for item in self.ds_externals:
            out.append(item.to_element())
        return out

    def _copy_feature(self, other, deep=False):
        if self.ds_externals:
            if deep:
                other.ds_externals = [x.copy(deep=deep) for x in self.ds_externals]
            else:
                other.ds_externals = list(self.ds_externals)

    def add_ds_external(self, source, code=None, code_file=None,
                        trace=False, connection_timeout=300, max_string_length=32000):
        '''
        Add a DS External

        Parameters
        ----------
        source : string
            Input window
        code : string, optional
            Inline block of code
        code_file : string, optional
            File containing code
        trace : bool, optional
            Print debugging information
        connection_timeout : int, optional
            Time for SAS to answer back
        max_string_length : int, optional
            Maximum string ESP will pass

        '''
        self.ds_externals.append(DSExternal(source, code=code,
                                            code_file=code_file, trace=trace,
                                            connection_timeout=connection_timeout,
                                            max_string_length=max_string_length))


class FunctionContextProperties(object):
    '''
    Container for various types of properties

    Attributes
    ----------
    map : dict-of-strings/dicts
        Executes the function to generate a map of name-value pairs to
        be used for value lookups by name.  The values can be either
        strings delimited using the defaults, or dicts using the form
        {'value': '<value>', 'outer': '<outer-delim>', 'inner': '<inner-delim>'}.
    set : dict-of-strings/dicts
        Executes the function to generate a set of strings to be used
        for value lookups.  The values can be either strings delimited
        using the defaults, or dicts using the form {'value': '<value>',
        delimiter='<delimiter>'}.
    list : dict-of-strings/dicts
        Executes the function to generate a list of strings to be used
        for value lookups.  The values can be either strings delimited
        using the defaults, or dicts using the form {'value': '<value>',
        delimiter='<delimiter>'}.
    xml : dict-of-strings
        Executes the function to generate an XML object that can be
        used for XPATH queries.
    json : dict-of-strings
        Executes the function to generate a JSON object that can be used
        for JSON lookups.
    string : dict-of-strings
        Executes the function to generate a string for general use
        in functions.

    Returns
    -------
    :class:`FunctionContextProperties`

    '''

    def __init__(self):
        self.map = {}
        self.xml = {}
        self.json = {}
        self.string = {}
        self.list = {}
        self.set = {}

    def copy(self, deep=False):
        '''
        Copy the object

        Parameters
        ----------
        deep : boolean, optional
            Should the attributes be deeply copied?

        Returns
        -------
        :class:`FunctionContextProperties`

        '''
        out = type(self)()
        if deep:
            out.map = copy.deepcopy(self.map)
            out.xml = copy.deepcopy(self.xml)
            out.json = copy.deepcopy(self.json)
            out.string = copy.deepcopy(self.string)
            out.list = copy.deepcopy(self.list)
            out.set = copy.deepcopy(self.set)
        else:
            out.map = dict(self.map)
            out.xml = dict(self.xml)
            out.json = dict(self.json)
            out.string = dict(self.string)
            out.list = dict(self.list)
            out.set = dict(self.set)
        return out


class FunctionContext(object):
    '''
    Function Context

    Parameters
    ----------
    expressions : dict, optional
        Dictionary of expressions in the form: {'<name>': '<regex>'}
    functions : dict, optional
        Dictionary of functions

    Attributes
    ----------
    properties : FunctionContextProperties
        Collection of property types

    Returns
    -------
    :class:`FunctionContext`

    '''

    def __init__(self, expressions=None, functions=None):
        self.expressions = collections.OrderedDict(expressions or {})
        self.functions = collections.OrderedDict(functions or {})
        self.properties = FunctionContextProperties()

    def copy(self, deep=False):
        '''
        Copy the object

        Parameters
        ----------
        deep : boolean, optional
            Should the attributes be deeply copied?

        Returns
        -------
        :class:`FunctionContext`

        '''
        out = type(self)(self.expressions, self.functions)
        out.properties = self.properties.copy(deep=deep)
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
        :class:`FunctionContext`

        '''
        data = ensure_element(data)

        out = cls()

        for item in data.findall('./expressions/expression'):
            out.expressions[item.attrib['name']] = item.text

        for item in data.findall('./proprties/property-map'):
            inner = item.attrib.get('inner', '').strip()
            outer = item.attrib.get('outer', '').strip()
            if not inner and not outer:
                out.properties[item.attrib['name']] = item.text
            else:
                value = {k: v for k, v in dict(value=item.text,
                                               inner=inner,
                                               outer=outer) if v is not None}
                out.properties.map[item.attrib['name']] = value

        for item in data.findall('./properties/property-xml'):
            out.properties.xml[item.attrib['name']] = item.text

        for item in data.findall('./properties/property-json'):
            out.properties.json[item.attrib['name']] = item.text

        for item in data.findall('./properties/property-string'):
            out.properties.string[item.attrib['name']] = item.text

        for item in data.findall('./properties/property-list'):
            out.properties.list[item.attrib['name']] = item.text

        for item in data.findall('./properties/property-set'):
            out.properties.set[item.attrib['name']] = dict(value=item.text,
                                                           delimiter=item.attrib['delimiter'])

        for item in data.findall('./functions/function'):
            out.functions[item.attrib['name']] = item.text

        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('function-context')

        if self.expressions:
            exprs = xml.add_elem(out, 'expressions')
            for key, value in six.iteritems(self.expressions):
                xml.add_elem(exprs, 'expression', attrib=dict(name=key),
                             text_content=value)

        props = None

        if self.properties.map:
            if props is None:
                props = xml.add_elem(out, 'properties')
            for key, value in six.iteritems(self.properties.map):
                if isinstance(value, dict):
                    xml.add_elem(props, 'property-map',
                                 attrib=dict(name=key,
                                             inner=value.get('inner'),
                                             outer=value.get('outer')),
                                 text_content=value['value'])
                else:
                    xml.add_elem(props, 'property-map', attrib=dict(name=key),
                                 text_content=value)

        if self.properties.xml:
            if props is None:
                props = xml.add_elem(out, 'properties')
            for key, value in six.iteritems(self.properties.xml):
                xml.add_elem(props, 'property-xml', attrib=dict(name=key),
                             text_content=value)

        if self.properties.json:
            if props is None:
                props = xml.add_elem(out, 'properties')
            for key, value in six.iteritems(self.properties.json):
                xml.add_elem(props, 'property-json', attrib=dict(name=key),
                             text_content=value)

        if self.properties.string:
            if props is None:
                props = xml.add_elem(out, 'properties')
            for key, value in six.iteritems(self.properties.string):
                xml.add_elem(props, 'property-string', attrib=dict(name=key),
                             text_content=value)

        if self.properties.list:
            if props is None:
                props = xml.add_elem(out, 'properties')
            for key, value in six.iteritems(self.properties.list):
                if isinstance(value, dict):
                    xml.add_elem(props, 'property-list',
                                 attrib=dict(name=key, delimiter=value['delimiter']),
                                 text_content=value['value'])
                else:
                    xml.add_elem(props, 'property-list', attrib=dict(name=key),
                                 text_content=value)

        if self.properties.set:
            if props is None:
                props = xml.add_elem(out, 'properties')
            for key, value in six.iteritems(self.properties.set):
                if isinstance(value, dict):
                    xml.add_elem(props, 'property-set',
                                 attrib=dict(name=key, delimiter=value['delimiter']),
                                 text_content=value['value'])
                else:
                    xml.add_elem(props, 'property-set', attrib=dict(name=key),
                                 text_content=value)

        if self.functions:
            funcs = xml.add_elem(out, 'functions')
            for key, value in six.iteritems(self.functions):
                xml.add_elem(funcs, 'function', attrib=dict(name=key),
                             text_content=value)

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

    def set_expressions(self, **kwargs):
        '''
        Set expressions

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Expressions in the form: {'<name>': '<regex>'}

        '''
        self.expressions.update(kwargs)

    def set_properties(self, prop_type, **kwargs):
        '''
        Set properties

        Parameters
        ----------
        prop_type : string
            The type of property: map, xml, json, string, list, set
        delimiter : string, optional
            The delimiter for list or set properties
        inner : string, optional
            The inner delimiter for map properties
        outer : string, optional
            The outer delimiter for map properties
        **kwargs : keyword-arguments, optional
            Function properties

        '''
        types = ['map', 'xml', 'json', 'string', 'list', 'set']

        if prop_type not in types:
            raise ValueError('Property type must be one of: %s' % ', '.join(types))

        if prop_type == 'map':
            inner = kwargs.pop('inner', None)
            outer = kwargs.pop('inner', None)
            for key, value in six.iteritems(kwargs):
                if inner and outer:
                    self.properties.map[key] = dict(value=value, inner=inner, outer=outer)
                else:
                    self.properties.map[key] = value

        elif prop_type == 'xml':
            for key, value in six.iteritems(kwargs):
                self.properties.xml[key] = value

        elif prop_type == 'json':
            for key, value in six.iteritems(kwargs):
                self.properties.json[key] = value

        elif prop_type == 'string':
            for key, value in six.iteritems(kwargs):
                self.properties.string[key] = value

        elif prop_type == 'list':
            delimiter = kwargs.pop('delimiter', None)
            for key, value in six.iteritems(kwargs):
                if delimiter:
                    self.properties.list[key] = dict(value=value, delimiter=delimiter)
                else:
                    self.properties.list[key] = value

        elif prop_type == 'set':
            delimiter = kwargs.pop('delimiter', None)
            for key, value in six.iteritems(kwargs):
                if delimiter:
                    self.properties.set[key] = dict(value=value, delimiter=delimiter)
                else:
                    self.properties.set[key] = value

    def set_functions(self, **kwargs):
        '''
        Set functions

        Parameters
        ----------
        **kwargs : keyword-arguments, optional
            Functions

        '''
        self.functions.update(kwargs)


def set_function_context_expressions(self, **kwargs):
    '''
    Set expressions

    Parameters
    ----------
    **kwargs : keyword-arguments, optional
        Named expressions, where the keyword parameter is the name
        and the value is the expression

    '''
    if self.function_context is None:
        self.function_context = FunctionContext()
    self.function_context.set_expressions(**kwargs)


def set_function_context_properties(self, prop_type, **kwargs):
    '''
    Set properties

    Parameters
    ----------
    prop_type : string
        The type of property: map, xml, json, string, list, set
    delimiter : string, optional
        The delimiter for list or set properties
    inner : string, optional
        The inner delimiter for map properties
    outer : string, optional
        The outer delimiter for map properties
    **kwargs : keyword-arguments, optional
        Function properties

    '''
    if self.function_context is None:
        self.function_context = FunctionContext()
    self.function_context.set_properties(prop_type, **kwargs)


def set_function_context_functions(self, **kwargs):
    '''
    Set functions

    Parameter names are the names of the functions.
    Parameter values correspond to the function code.

    Notes
    -----
    Functions are added in a non-deterministic order.  If you
    need the functions to be in a particular order, you will
    need to call this method multiple times (once for each
    order-dependent function).

    Parameters
    ----------
    **kwargs : keyword-arguments, optional
        Functions

    '''
    if self.function_context is None:
        self.function_context = FunctionContext()
    self.function_context.set_functions(**kwargs)


class RegexEventLoop(object):
    '''
    Regex Event Loop

    Parameters
    ----------
    name : string
        Specifies the loop name
    use_text : string
        Specifies the regular expression to use in the event loop
    regex : string
        Specifies a regular expression in order to retrieve zero
        or more entities during an event loop.
    data : string, optional
        Name of variable to populate with the current element
    regex_group : int, optional
        Group number in regex
    function_context : FunctionContext, optional
        Defines entities to run functions on event data and
        generate values in an output event.

    Returns
    -------
    :class:`RegexEventLoop`

    '''

    def __init__(self, name, use_text, regex, data=None, regex_group=None, function_context=None):
        self.name = name
        self.use_text = use_text
        self.regex = regex
        self.data = data
        self.regex_group = regex_group
        self.function_context = function_context

    def copy(self, deep=True):
        return type(self)(self.name, self.use_text, self.regex,
                          data=self.data, function_context=self.function_context)

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
        :class:`RegexEventLoop`

        '''
        data = ensure_element(data)

        use_text = None
        for item in data.findall('./use-text'):
            use_text = item.text

        regex = None
        regex_group = None
        for item in data.findall('./regex'):
            regex = item.text
            if 'regex_group' in item.attrib:
                regex_group = int(item.attrib['regex_group'])

        out = cls(data.attrib['name'], use_text, regex,
                  data=data.attrib.get('data', None), regex_group=regex_group)
        for item in data.findall('./function-context'):
            out.function_context = FunctionContext.from_xml(item, session=session)
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('event-loop-regex',
                           attrib=dict(name=self.name, data=self.data))
        xml.add_elem(out, 'use-text', text_content=self.use_text)
        xml.add_elem(out, 'regex', text_content=self.regex, group=self.regex_group)
        if self.function_context is not None:
            xml.add_elem(out, self.function_context.to_element())
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

    set_function_context_expressions = set_function_context_expressions

    set_function_context_properties = set_function_context_properties

    set_function_context_functions = set_function_context_functions


class XMLEventLoop(object):
    '''
    XML Event Loop

    Parameters
    ----------
    name : string
        Specifies the loop name
    use_xml : string
        Specifies the XML code to use in the event loop
    xpath : string
        Specifies an XML expression in order to retrieve zero or more
        entities during an event loop.
    data : string, optional
        Name of variable to populate with the current element
    function_context : FunctionContext, optional
        Defines entities to run functions on event data and
        generate values in an output event.

    Returns
    -------
    :class:`XMLEventLoop`

    '''

    def __init__(self, name, use_xml, xpath, data=None, function_context=None):
        self.name = name
        self.use_xml = use_xml
        self.xpath = xpath
        self.data = data
        self.function_context = function_context

    def copy(self, deep=False):
        return type(self)(self.name, self.use_xml, self.xpath,
                          data=self.data, function_context=self.function_context)

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
        :class:`XMLEventLoop`

        '''
        data = ensure_element(data)

        use_xml = None
        for item in data.findall('./use-xml'):
            use_xml = item.text

        xpath = None
        for item in data.findall('./xpath'):
            xpath = item.text

        out = cls(data.attrib['name'], use_xml, xpath, data=data.attrib.get('data', None))
        for item in data.findall('./function-context'):
            out.function_context = FunctionContext.from_xml(item, session=session)
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('event-loop-xml',
                           attrib=dict(name=self.name, data=self.data))
        xml.add_elem(out, 'use-xml', text_content=self.use_xml)
        xml.add_elem(out, 'xpath', text_content=self.xpath)
        if self.function_context is not None:
            xml.add_elem(out, self.function_context.to_element())
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

    set_function_context_expressions = set_function_context_expressions

    set_function_context_properties = set_function_context_properties

    set_function_context_functions = set_function_context_functions


class JSONEventLoop(object):
    '''
    JSON Event Loop

    Parameters
    ----------
    name : string
        Specifies the loop name
    use_json : string
        Specifies the JSON code to use in the event loop
    json : string
        Specifies the JSON expression in order to retrieve zero or more
        entities during an event loop.
    data : string, optional
        Name of variable to populate with the current element
    function_context : FunctionContext, optional
        Defines entities to run functions on event data and
        generate values in an output event.

    Returns
    -------
    :class:`JSONEventLoop`

    '''

    def __init__(self, name, use_json, json, data=None, function_context=None):
        self.name = name
        self.use_json = use_json
        self.json = json
        self.data = data
        self.function_context = function_context

    def copy(self, deep=False):
        return type(self)(self.name, self.use_json, self.json,
                          data=self.data, function_context=self.function_context)

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
        :class:`JSONEventLoop`

        '''
        data = ensure_element(data)

        use_json = None
        for item in data.findall('./use-json'):
            use_json = item.text

        jsn = None
        for item in data.findall('./json'):
            jsn = item.text

        out = cls(data.attrib['name'], use_json, jsn, data=data.attrib.get('data', None))
        for item in data.findall('./function-context'):
            out.function_context = FunctionContext.from_xml(item, session=session)
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = xml.new_elem('event-loop-json',
                           attrib=dict(name=self.name, data=self.data))
        xml.add_elem(out, 'use-json', text_content=self.use_json)
        xml.add_elem(out, 'json', text_content=self.json)
        if self.function_context is not None:
            xml.add_elem(out, self.function_context.to_element())
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

    set_function_context_expressions = set_function_context_expressions

    set_function_context_properties = set_function_context_properties

    set_function_context_functions = set_function_context_functions


class FunctionContextFeature(WindowFeature):
    '''
    Function Context Feature

    '''

    def __init__(self):
        self.function_context = None

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./function-context'):
            self.function_context = FunctionContext.from_element(item, session=self.session)

    def _feature_to_element(self):
        if self.function_context is None:
            return
        return self.function_context.to_element()

    def _copy_feature(self, other, deep=False):
        if self.function_context is not None:
            other.function_context = self.function_context.copy(deep=deep)

    set_function_context_expressions = set_function_context_expressions

    set_function_context_properties = set_function_context_properties

    set_function_context_functions = set_function_context_functions


class EventLoopFeature(WindowFeature):
    '''
    Event Loop Feature

    '''

    def __init__(self):
        self.event_loops = []

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./event-loops/*'):
            if item.tag == 'event-loop-regex':
                self.event_loops.append(RegexEventLoop.from_element(item, session=self.session))
            elif item.tag == 'event-loop-xml':
                self.event_loops.append(XMLEventLoop.from_element(item, session=self.session))
            elif item.tag == 'event-loop-json':
                self.event_loops.append(JSONEventLoop.from_element(item, session=self.session))

    def _feature_to_element(self):
        if not self.event_loops:
            return
        out = xml.new_elem('event-loops')
        for item in self.event_loops:
            xml.add_elem(out, item.to_element())
        return out

    def _copy_feature(self, other, deep=False):
        if self.event_loops:
            if deep:
                other.event_loops = [x.copy(deep=deep) for x in self.event_loops]
            else:
                other.event_loops = list(self.event_loops)

    def create_function_context(self, expressions=None, functions=None):
        '''
        Create a new function context for use in event loops

        Parameters
        ----------
        expressions : dict, optional
            Dictionary of expressions in the form: {'<name>': '<regex>'}
        functions : dict, optional
            Dictionary of functions

        Returns
        -------
        :class:`FunctionContext`

        '''
        return FunctionContext(expressions=expressions, functions=functions)

    def add_regex_event_loop(self, name, use_text, regex, data=None, regex_group=None, function_context=None):
        '''
        Add a Regex Event Loop

        Parameters
        ----------
        name : string
            Specifies the loop name
        use_text : string
            Specifies the regular expression to use in the event loop
        regex : string
            Specifies a regular expression in order to retrieve zero
            or more entities during an event loop.
        data : string, optional
            Name of variable to populate with the current element
        regex_group : int, optional
            Group number in regex
        function_context : FunctionContext, optional
            Defines entities to run functions on event data and
            generate values in an output event.

        '''
        self.event_loops.append(RegexEventLoop(name, use_text, regex, data=data, regex_group=regex_group,
                                               function_context=function_context))

    def add_xml_event_loop(self, name, use_xml, xpath, data=None, function_context=None):
        '''
        Add an XML Event Loop

        Parameters
        ----------
        name : string
            Specifies the loop name
        use_xml : string
            Specifies the XML code to use in the event loop
        xpath : string
            Specifies an XML expression in order to retrieve zero or more
            entities during an event loop.
        data : string, optional
            Name of variable to populate with the current element
        function_context : FunctionContext, optional
            Defines entities to run functions on event data and
            generate values in an output event.

        '''
        self.event_loops.append(XMLEventLoop(name, use_xml, xpath,
                                             data=data, function_context=function_context))

    def add_json_event_loop(self, name, use_json, json, data=None, function_context=None):
        '''
        JSON Event Loop

        Parameters
        ----------
        name : string
            Specifies the loop name
        use_json : string
            Specifies the JSON code to use in the event loop
        json : string
            Specifies the JSON expression in order to retrieve zero or more
            entities during an event loop.
        data : string, optional
            Name of variable to populate with the current element
        function_context : FunctionContext, optional
            Defines entities to run functions on event data and
            generate values in an output event.

        '''
        self.event_loops.append(JSONEventLoop(name, use_json, json, data=data,
                                              function_context=function_context))


class OpcodeFeature(WindowFeature):
    '''
    Opcode Feature

    '''

    def __init__(self):
        self.opcode = None

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./opcode'):
            self.opcode = item.text

    def _feature_to_element(self):
        if self.opcode is None:
            return
        return xml.new_elem('opcode', text_content=self.opcode)

    def _copy_feature(self, other, deep=False):
        other.opcode = self.opcode


class GenerateFeature(WindowFeature):
    '''
    Generate Feature

    '''

    def __init__(self):
        self.generate = None

    def _feature_from_element(self, data):
        data = ensure_element(data)
        for item in data.findall('./generate'):
            self.generate = item.text

    def _feature_to_element(self):
        if self.generate is None:
            return
        return xml.new_elem('generate', text_content=self.generate)

    def _copy_feature(self, other, deep=False):
        other.generate = self.generate
