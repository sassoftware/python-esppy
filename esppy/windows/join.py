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

import copy
import re
import six
from .base import Window, attribute
from .features import InitializeExpressionFeature
from .utils import get_args, ensure_element, connectors_to_end
from ..utils import xml


class FieldExpression(object):
    '''
    Join field expression

    Parameters
    ----------
    name : string
        Output field for the join
    expr : string
        Join expression
    type : string, optional
        Type of the output field

    Returns
    -------
    :class:`FieldExpression`

    '''

    def __init__(self, name, expr, type='double'):
        self.name = name
        self.type = type
        self.expr = expr

    def copy(self, deep=False):
        return type(self)(self.name, self.expr, type=self.type)

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
        return cls(data.attrib['name'], data.text, type=data.attrib['type'])

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('field-expr',
                            attrib=dict(name=self.name, type=self.type),
                            text_content=self.expr)

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


class LeftExpression(object):
    '''
    Join left expression

    Parameters
    ----------
    names : string or list-of-strings, optional
        Specify fields from the left input to the Join window
        to use in output elements.

    Returns
    -------
    :class:`LeftExpression`

    '''

    def __init__(self, names='*'):
        if isinstance(names, six.string_types):
            names = re.split(r'\s*,\s*', names.strip())
        self.names = names or []

    def copy(self, deep=False):
        return type(self)(self.names)

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
        return cls(ensure_element(data).text)

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('left-expr', text_content=','.join(self.names))

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


class RightExpression(object):
    '''
    Join right expression

    Parameters
    ----------
    names : string or list-of-strings, optional
        Specify fields from the right input to the Join window
        to use in output elements.

    Returns
    -------
    :class:`RightExpression`

    '''

    def __init__(self, names='*'):
        if isinstance(names, six.string_types):
            names = re.split(r'\s*,\s*', names.strip())
        self.names = names or []

    def copy(self, deep=False):
        return type(self)(self.names)

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
        return cls(ensure_element(data).text)

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('right-expr', text_content=','.join(self.names))

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


class FieldPlugin(object):
    '''
    Join field plugin

    Parameters
    ----------
    name : string
        Name of the output field
    type : string
        Type of the output field
    plugin : string
        Full path to .so / .dll
    function : string
        Function to call in the library

    Returns
    -------
    :class:`FieldPlugin`

    '''

    def __init__(self, name, type, plugin, function):
        self.name = name
        self.type = type
        self.plugin = plugin
        self.function = function

    def copy(self, deep=False):
        return type(self)(self.name, self.type, self.plugin, self.function)

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
        :class:`FieldPlugin`

        '''
        data = ensure_element(data)
        return cls(data.attrib['name'], data.attrib['type'],
                   data.attrib['plugin'], data.attrib['function'])

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('field-plug', attrib=dict(name=self.name,
                                                      type=self.type,
                                                      plugin=self.plugin,
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


class FieldSelection(object):
    '''
    Join field selection

    Parameters
    ----------
    name : string
        Name of the field in the join schema
    source : string
        Field in the input schema

    Returns
    -------
    :class:`FieldSelection`

    '''

    def __init__(self, name, source):
        self.name = name
        self.source = source

    def copy(self, deep=False):
        return type(self)(self.name, self.source)

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
        :class:`FieldSelection`

        '''
        return cls(data.attrib['name'], data.attrib['source'])

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('field-selection',
                            attrib=dict(name=self.name, source=self.source))

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


class LeftSelection(object):
    '''
    Join left selection

    Parameters
    ----------
    names : string or list-of-strings
        Specify fields from the left input to the Join window
        to use in output elements.
    exclude : string or list-of-strings, optional
        Specifies a comma-separated list of fields to use
        from the input window.

    Returns
    -------
    :class:`LeftSelection`

    '''

    def __init__(self, names, exclude=None):
        if isinstance(names, six.string_types):
            names = re.split(r'\s*,\s*', names.strip())
        if isinstance(exclude, six.string_types):
            exclude = re.split(r'\s*,\s*', exclude.strip())
        self.names = names or []
        self.exclude = exclude or []

    def copy(self, deep=False):
        return type(self)(self.names, self.exclude)

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
        :class:`LeftSelection`

        '''
        data = ensure_element(data)
        return cls(data.text, data.attrib.get('exclude'))

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('left-select',
                            attrib=dict(exclude=','.join(self.exclude) or None),
                            text_content=','.join(self.names))

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


class RightSelection(object):
    '''
    Join right selection

    Parameters
    ----------
    names : string or list-of-strings
        Specify fields from the left input to the Join window
        to use in output elements.
    exclude : string or list-of-strings, optional
        Specifies a comma-separated list of fields to use
        from the input window.

    Returns
    -------
    :class:`RightSelection`

    '''

    def __init__(self, names, exclude=None):
        if isinstance(names, six.string_types):
            names = re.split(r'\s*,\s*', names.strip())
        if isinstance(exclude, six.string_types):
            exclude = re.split(r'\s*,\s*', exclude.strip())
        self.names = names or []
        self.exclude = exclude or []

    def copy(self, deep=False):
        return type(self)(self.names, self.exclude) 

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
        return cls(data.text, data.attrib.get('exclude'))

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('right-select',
                            attrib=dict(exclude=','.join(self.exclude) or None),
                            text_content=','.join(self.names))

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


class JoinWindow(Window, InitializeExpressionFeature):
    '''
    Join window

    Parameters
    ----------
    name : string, optional
        The name of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level
        value of pubsub is manual, true enables publishing and subscribing
        for the window and false disables it.
    description : string, optional
        Description of the window
    output_insert_only : bool, optional
        When true, prevents the window from passing non-insert events to
        other windows.
    collapse_updates : bool, optional
        When true, multiple update blocks are collapsed into a single update block
    pulse_interval : string, optional
        Output a canonical batch of updates at the specified interval
    exp_max_string : int, optional
        Specifies the maximum size of strings that the expression engine
        uses for the window. Default value is 1024.
    index_type : string, optional
        Index type for the window
    pubsub_index_type : string, optional
        Publish/subscribe index type.  Valid values are the same as for the
        `index_type` parameter.
    type : string, optional
        Type of the join
    use_secondary_index : bool, optional
        Use a secondary index
    no_regenerates : bool, optional
        When true, do not regenerate join changes when the dimension
        table changes.
    left_index : string, optional
        Left index type override
    right_index : string, optional
        Right index type override
    conditions : list-of-tuples, optional
        One or more equijoin match pairs.  Each pair should be a two elemnt
        tuple: (left-name, right-name).

    Attributes
    ----------
    expr_initializer : InitializeExpression
        Initialization expression code block
    output : list
        List of FieldPlugins, FieldExpressions, FieldSelections,
        LeftExpressions, RightExpressions, LeftSelections, and RightSelections.

    Returns
    -------
    :class:`JoinWindow`

    '''

    window_type = 'join'

    def __init__(self, name=None, pubsub=None, description=None,
                 output_insert_only=None, collapse_updates=None,
                 pulse_interval=None, exp_max_string=None, index_type=None,
                 pubsub_index_type=None, type='leftouter',
                 use_secondary_index=None, no_regenerates=None,
                 left_index=None, right_index=None, conditions=None):
        Window.__init__(self, **get_args(locals()))
        self.type = type
        self.use_secondary_index = use_secondary_index
        self.no_regenerates = no_regenerates
        self.left_index = left_index
        self.right_index = right_index
        self.conditions = conditions or []
        self.output = []

    def copy(self, deep=False):
        out = Window.copy(self, deep=deep)
        out.type = self.type
        out.use_secondary_index = self.use_secondary_index
        out.no_regenerates = self.no_regenerates
        out.left_index = self.left_index
        out.right_index = self.right_index
        if deep:
            out.conditions = copy.deepcopy(self.conditions)
            out.output = [x.copy(deep=deep) for x in self.output]
        else:
            out.conditions = list(self.conditions)
            out.output = list(self.output)
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
        :class:`JoinWindow`

        '''
        out = super(JoinWindow, cls).from_element(data, session=session)

        for item in data.findall('./join'):
            for key, value in six.iteritems(item.attrib):
                key = key.replace('-', '_')
                if hasattr(out, key):
                    setattr(out, key, value)
            for cond in item.findall('./conditions/fields'):
                out.conditions.append((cond.attrib['left'], cond.attrib['right']))

        for item in data.findall('./output/field-expr'):
            out.output.append(FieldExpression.from_element(item, session=session))
        for item in data.findall('./output/left-expr'):
            out.output.append(LeftExpression.from_element(item, session=session))
        for item in data.findall('./output/right-expr'):
            out.output.append(LeftExpression.from_element(item, session=session))

        for item in data.findall('./output/field-plug'):
            out.output.append(FieldPlugin.from_element(item, session=session))

        for item in data.findall('./output/field-selection'):
            out.output.append(FieldSelection.from_element(item, session=session))
        for item in data.findall('./output/left-select'):
            out.output.append(LeftSelection.from_element(item, session=session))
        for item in data.findall('./output/right-select'):
            out.output.append(RightSelection.from_element(item, session=session))

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
        out = Window.to_element(self, query=query)

        join = xml.add_elem(out, 'join',
                            attrib=dict(type=self.type,
                                        use_secondary_index=self.use_secondary_index,
                                        no_regenerates=self.no_regenerates,
                                        left_index=self.left_index,
                                        right_index=self.right_index))
        cond = xml.add_elem(join, 'conditions')
        for left, right in self.conditions:
            xml.add_elem(cond, 'fields', attrib=dict(left=left, right=right))

        output = xml.add_elem(out, 'output')
        for item in self.output:
            xml.add_elem(output, item.to_element())

        connectors_to_end(out)

        return out

    def add_condition(self, left, right):
        '''
        Add a join condition

        Parameters
        ----------
        left : string
            Left field for match
        right : string
            Right field for match

        '''
        self.conditions.append((left, right))

    def add_field_expression(self, name, expr, type='double'):
        '''
        Add a field expression

        Parameters
        ----------
        name : string, optional
            Specifies the name of the output field for the join
        expr : string
            Specifies an Expression Engine Language (EEL) expression
            that is assigned to a field.
        type : string, optional
            The data type of the expression result.
            Valid Values: int32, int64, double, string, date, stamp,
            money, blob

        '''
        self.output.append(FieldExpression(name, expr, type=type))

    add_field_expr = add_field_expression

    def add_expression(self, names, type='left'):
        '''
        Add an expression

        Parameters
        ----------
        names : string or list-of-strings
            Specifies fields from the left input or the right input
            to the Join window to use in output elements.
        type : string, optional
            The type of expression.
            Valid values: left or right

        '''
        if type.lower().startswith('r'):
            self.output.append(RightExpression(names))
        else:
            self.output.append(LeftExpression(names))

    add_expr = add_expression

    def add_field_selection(self, name, source):
        '''
        Add a field selection

        Parameters
        ----------
        name : string
            Selected field in the output schema
        source : string
            Takes the following form:l_field_name | rfield_name.,
            where 'l_' indicates that the field comes from the left window
            and 'r_' indicates that the field comes from the right window.

        '''
        self.output.append(FieldSelection(name, source))

    def add_selection(self, selection, type='left', exclude=None):
        '''
        Add a selection

        Parameters
        ----------
        selection : string or list-of-strings
            Specifies fields from the left input or the right input to
            the Join window to use in output elements.
        exclude : string or list-of-strings, optional
            Specifies a comma-separated list of fields to use from
            the input window.
        type : string, optional
            Type of selection.
            Valid values: left or right

        '''
        if type.lower().startswith('r'):
            self.output.append(RightSelection(selection, exclude=exclude))
        else:
            self.output.append(LeftSelection(selection, exclude=exclude))

    def add_field_plugin(self, name, type, plugin, function):
        '''
        Add a field plugin

        Parameters
        ----------
        name : string
            Name of the output field
        type : string
            Type of the output field
        plugin : string
            Full path to .so / .dll
        function : string
            Function to call in the library

        '''
        self.output.append(FieldPlugin(name, type, plugin, function))
