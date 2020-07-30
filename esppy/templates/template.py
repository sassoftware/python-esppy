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

''' ESP Template '''

from __future__ import print_function, division, absolute_import, unicode_literals

import collections
import os
import re
import requests
import six
import warnings
from six.moves import urllib
from ..base import ESPObject, attribute
from ..config import get_option
from ..mas import MASModule
from ..windows import Target, BaseWindow, get_window_class, CalculateWindow, TrainWindow, ScoreWindow, ModelReaderWindow
from ..utils import xml
from ..utils.data import gen_name
from ..utils.notebook import scale_svg


class WindowDict(collections.abc.MutableMapping):
    '''
    Dictionary for holding window objects

    Attributes
    ----------
    project : string
        The name of the project
    contquery : string
        The name of the continuous query
    template : string
        The name of the template
    session : requests.Session
        The session for the windows

    Parameters
    ----------
    *args : one-or-more arguments, optional
        Positional arguments to MutableMapping
    **kwargs : keyword arguments, optional
        Keyword arguments to MutableMapping

    '''

    def __init__(self, template, *args, **kwargs):
        collections.abc.MutableMapping.__init__(self, *args, **kwargs)
        self._data = dict()
        self.template = template
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

    @property
    def template(self):
        '''
        The tempalte that windows are associated with

        Returns
        -------
        string

        '''
        return self._template

    @template.setter
    def template(self, value):
        self._template = value
        for item in self._data.values():
            item.template = self._template
        if hasattr(value, 'contquery'):
            self.contquery = value.contquery
        if hasattr(value, 'project'):
            self.project = value.project

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        if not isinstance(value, BaseWindow):
            raise TypeError('Only Window objects can be values '
                            'in a template')

        if key in self._data.keys():
            oldkey = key
            suffix = 0
            while key in self._data.keys():
                key = key.strip(str(suffix))
                suffix += 1
                key = key + str(suffix)

            warnings.warn('%s already exists in Template %s, renamed to %s' % (oldkey, self.template.name, key),
                          Warning)

        value._register_to_project(self.template)

        oldname = value.name

        value.base_name = key
        value.project = self.project
        value.contquery = self.contquery
        value.template = self.template
        value.session = self.session
        self._data[key] = value

        # Make sure targets get updated with new name
        if oldname != value.name:
            for window in self._data.values():
                for target in set(window.targets):
                    if target.name == oldname:
                        role, slot = target.role, target.slot
                        window.targets.discard(target)
                        window.add_target(value, role=role, slot=slot)

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


class Template(ESPObject, collections.abc.MutableMapping):
    '''
    ESP Template

    Parameters
    ----------
    name : string
        Name of the continuous query
    trace : string, optional
        One or more space-separated window names or IDs
    index_type : string, optional
        A default index type for all windows in the template that
        do not explicitly specify an index type
        Valid values: 'rbtree', 'hash', 'ln_hash', 'cl_hash', 'fw_hash', 'empty'
    timing_threshold : int, optional
        When a window in the template takes more than value microseconds to
        compute for a given event or event block, a warning message is logged
    include_singletons : bool, optional
        Specify whether to add unattached source windows
    description : string, optional
        Description of the template

    Attributes
    ----------
    project : string or Project
        Name of the project the template is associated with
    contquery : string or ContinuousQuerry
        The name of the continuous query
    windows : dict
        Collection of windows in the template
    metadata : dict
        Metadata dictionary
    url : string
        URL of the template

    Notes
    -----
    All parameters are also available as instance attributes.

    Returns
    -------
    :class:`Template`

    '''
    trace = attribute('trace', dtype='string')

    index_type = attribute('index', dtype='string',
                           values={'rbtree': 'pi_RBTREE', 'hash': 'pi_HASH',
                                   'ln_hash': 'pi_LN_HASH', 'cl_hash': 'pi_CL_HASH',
                                   'fw_hash': 'pi_FW_HASH', 'empty': 'pi_EMPTY'})
    timing_threshold = attribute('timing-threshold', dtype='int')
    include_singletons = attribute('include-singletons', dtype='bool')

    file_dir = os.path.split(__file__)[0]
    template_list = collections.defaultdict(dict)

    def __init__(self, name, trace=None, index_type=None,
                 timing_threshold=None, include_singletons=None, description=None):
        self.windows = WindowDict(self)
        ESPObject.__init__(self, attrs=locals())
        self.project = None
        self.contquery = None
        self.tag = None
        self.name = name or gen_name(prefix='tp_')
        self.description = description
        self.mas_modules = []
        self.metadata = {}
        self.input_windows = list()
        self.output_windows = list()
        self.required_parameter_map = collections.defaultdict(list)

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
        The name of the template

        Returns
        -------
        string

        '''
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def contquery(self):
        '''
        The name of the continuous query

        Returns
        -------
        string

        '''
        return self._contquery

    @contquery.setter
    def contquery(self, value):
        self._contquery = getattr(value, 'name', value)
        self.windows.contquery = self._contquery

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

    @property
    def input_windows(self):
        return ','.join(self._input_windows)

    @input_windows.setter
    def input_windows(self, value):
        if not isinstance(value, list):
            raise TypeError('Please use a list to set input_windows')

        self._input_windows = list()
        for window in value:
            self.add_input_windows(window)

    def add_input_windows(self, *windows):
        '''
        Add input_windows

        Parameters
        ----------
        windows : one-or-more-Windows
            The Window objects to add as input_windows for Template

        '''

        for window in windows:
            base_name = getattr(window, 'base_name', window)
            if not base_name or base_name not in self.windows:
                raise KeyError('%s is not a one of Template %s' %
                               (base_name, self.name))
            elif base_name in self._input_windows:
                print('%s is already a one of input_windows' % base_name)
            else:
                self._input_windows.append(base_name)

        return self._input_windows

    def delete_input_windows(self, *windows):
        '''
        Delete input_windows

        Parameters
        ----------
        windows : one-or-more-Windows
            The Window objects to deleted from input_windows

        '''

        for window in windows:
            base_name = getattr(window, 'base_name', window)
            if base_name not in self.windows:
                raise KeyError('%s is not a one of Template %s' %
                               (base_name, self.name))
            elif base_name not in self._input_windows:
                print('%s is not a one of input_windows' % base_name)
            else:
                self._input_windows.remove(base_name)

        return self._input_windows

    @property
    def output_windows(self):
        return ','.join(self._output_windows)

    @output_windows.setter
    def output_windows(self, value):
        if not isinstance(value, list):
            raise TypeError('Please use a list to set output_windows')

        self._output_windows = list()
        for window in value:
            self.add_output_windows(window)

    def add_output_windows(self, *windows):
        '''
        Add output_windows

        Parameters
        ----------
        windows : one-or-more-Windows
            The Window objects to add as output_windows for Template

        '''

        for window in set(windows):
            base_name = getattr(window, 'base_name', window)
            if base_name not in self.windows:
                raise KeyError('%s is not a one of Template %s' %
                               (base_name, self.name))
            elif base_name in self._output_windows:
                print('%s is already a one of output_windows' % base_name)
            else:
                self._output_windows.append(base_name)

        return self._output_windows

    def delete_output_windows(self, *windows):
        '''
        Delete output_windows

        Parameters
        ----------
        windows : one-or-more-Windows
            The Window objects to deleted from output_windows

        '''

        for window in set(windows):
            base_name = getattr(window, 'base_name', window)
            if base_name not in self.windows:
                raise KeyError('%s is not a one of Template %s' %
                               (base_name, self.name))
            elif base_name not in self._output_windows:
                print('%s is not a one of input_windows' % base_name)
            else:
                self._output_windows.remove(base_name)

        return self._output_windows

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
        out.project = self.project
        out.session = self.session
        return out

    def set_parameters(self, window, **parameters):
        '''
        Set parameters

        Parameters
        ----------
        window : Window
            The Window object to set parameters
        **parameters : keyword-arguments, optional
            The parameters to set

        '''
        base_name = getattr(window, 'base_name', window)
        try:
            window = self.windows[base_name]
        except KeyError:
            raise KeyError('%s is not a one of Template %s' %
                                         (base_name, self.name))

        if not isinstance(window, (CalculateWindow, TrainWindow, ModelReaderWindow)):
            raise TypeError('Only CalculationWindow and TrainWindow objects support the method')
        return window.set_parameters(**parameters)

    def set_inputs(self, window=None, model=None, **input_map):
        '''
        Set inputs

        Parameters
        ----------
        window : Window, optional
            The Window object to set inputs, default value is None
        model : string
            The name / URL of the model
        **input_map : keyword-arguments, optional
            The parameters to set

        '''

        if window is None:
            try:
                window = self._input_windows[0]
                print("INFO: window is not specified, default for first input window %s" % window)
            except IndexError:
                raise IndexError("Please specify input_windows for Template %s first" % self.name)

        base_name = getattr(window, 'base_name', window)
        try:
            window = self.windows[base_name]
        except KeyError:
            raise ValueError('%s is not a one of Template %s' %
                             (base_name, self.name))

        if isinstance(window, (TrainWindow, CalculateWindow)):
            return window.set_inputs(**input_map)
        elif isinstance(window, ScoreWindow):
            return window.set_inputs(model, **input_map)
        else:
            raise TypeError('Only CalculationWindow, TrainWindow and ScoreWindow objects support the method')

    def set_outputs(self, window=None, model=None, **output_map):
        '''
        Set outputs

        Parameters
        ----------
        window : Window, optional
            The Window object to set outputs, default value is None
        model : string
            The name / URL of the model
        **output_map : keyword-arguments, optional
            The parameters to set

        '''
        if window is None:
            try:
                window = self._output_windows[0]
                print("INFO: window is not specified, default for first output window %s" % window)
            except IndexError:
                raise IndexError("Please specify output_windows for Template %s first" % self.name)

        base_name = getattr(window, 'base_name', window)
        try:
            window = self.windows[base_name]
        except KeyError:
            raise ValueError('%s is not a one of Template %s' %
                             (base_name, self.name))

        if isinstance(window, (CalculateWindow, ScoreWindow)):
            return window.set_outputs(**output_map)
        elif isinstance(window, ScoreWindow):
            return window.set_outputs(model, **output_map)
        else:
            raise TypeError('Only CalculationWindow and ScoreWindow objects support the method')

    def add_target(self, obj, **kwargs):
        '''
        Add target for Template

        Parameters
        ----------
        obj : Window or Template
            The Window or Template object to add as targets
        role : string, optional
            The role of the connection
        slot : string, optional
            Indicates the slot number to use from the splitting
            function for the window.

        Returns
        -------
        ``self``

        '''
        try:
            window_name = self._output_windows[0]
        except IndexError:
            raise IndexError("Please specify output_windows for Template %s first" % self.name)

        window = self.windows[window_name]
        if isinstance(obj, BaseWindow):
            window.add_targets(obj, **kwargs)

        elif isinstance(obj, Template):
            try:
                target_window_name = obj._input_windows[0]
            except IndexError:
                raise IndexError("Please specify input_windows for Template %s first" % obj.name)
            window.add_targets(obj.windows[target_window_name], **kwargs)

    def delete_target(self, *objs):
        '''
        Delete targets for Template

        Parameters
        ----------
        obj : Window or Template
            The Window or Template object to deleted from targets

        Returns
        -------
        ``self``

        '''
        try:
            window_name = self._output_windows[0]
        except IndexError:
            raise IndexError("There is no output_windows for Template %s" % self.name)

        window = self.windows[window_name]
        target_set = set(target.name for target in window.targets)
        for obj in objs:
            if isinstance(obj, BaseWindow):
                window.delete_targets(obj)

            elif isinstance(obj, Template):
                for possible_target in obj._input_windows:
                    if obj.windows[possible_target].name in target_set:
                        window.delete_targets(obj.windows[possible_target])

    def add_window(self, window):
        '''
        Add a window to the template

        Parameters
        ----------
        window : Window
            The Window object to add

        Returns
        -------
        :class:`Window`

        '''
        if not window.base_name:
            window.base_name = gen_name(prefix='w_')

        self.windows[window.base_name] = window

        return window

    def add_windows(self, *windows):
        '''
        Add one or more windows to the template

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

    def import_template(self, template, internal_only=True):
        '''
        import a template object

        Parameters
        ----------
        template : Template
            A Template object to be imported to current template
        internal_only: bool, optional
            Only includes the internal edges or not, default value is True

        Returns
        -------
        :class:`Template`
        '''
        if template is self:
            warnings.warn('You are importing the self template.', Warning)
            return self.import_template(self.copy(None), internal_only=internal_only)

        ref_dict = {}
        for key, window in sorted(six.iteritems(template.windows)):
            copied_win = window.copy(deep=True)
            self.windows[key] = copied_win
            ref_dict[key] = copied_win.base_name

        for old_name, new_name in ref_dict.items():
            copied_win = self.windows[new_name]
            win = template.windows[old_name]
            copied_win.targets = set()
            for target in set(win.targets):
                if target.base_name in ref_dict:
                    copied_win.targets.add(Target(name=ref_dict[target.base_name], template=self,
                                                  role=target.role, slot=target.slot))
                elif not internal_only:
                    copied_win.targets.add(target)

        return

    def copy(self, name, deep=True, internal_only=True):
        '''
        Return a copy of the template

        Parameters
        ----------
        name : string
            Name of the copied template
        deep : bool, optional
            Copy the sub-objects or not, default value is True
        internal_only: bool, optional
            Only includes the internal edges or not, default value is True

        Returns
        -------
        :class:`Template`

        '''
        out = type(self)(name)

        out.session = self.session
        out.contquery = self.contquery
        out.project = self.project

        for key, value in self._get_attributes(use_xml_values=False).items():
            if key != 'name':   # do NOT copy the old name
                setattr(out, key, value)

        if deep:
            for k, win in self.windows.items():
                out.windows[k] = win.copy(deep=deep)
                out.windows[k].targets = set()
                for target in set(win.targets):
                    if target.template.name == self.name:
                        out.windows[k].targets.add(Target(name=target.base_name, template=out,
                                                          role=target.role, slot=target.slot))
                    elif not internal_only:
                        out.windows[k].targets.add(target)
        else:
            out.windows.update(self.windows)

        out.input_windows = self._input_windows
        out.output_windows = self._output_windows
        out.required_parameter_map = self.required_parameter_map

        return out

    def __copy__(self):
        return self.copy(name=None, deep=False)

    def __deepcopy__(self, memo):
        return self.copy(name=None, deep=True)

    @property
    def fullname(self):
        return '%s.%s.%s' % (self.project, self.contquery, self.name)

    @property
    def url(self):
        '''
        URL of the Template

        Returns
        -------
        string

        '''
        self._verify_project()
        return urllib.parse.urljoin(self.base_url, '%s/%s/%s/' %
                                    (self.project, self.contquery, self.name))

    @classmethod
    def from_xml(cls, data, template_name, tag=None, contquery=None, project=None, session=None):
        '''
        Create template from XML definition

        Parameters
        ----------

        data : xml-string or ElementTree.Element
            XML template definition
        template_name: string
            The name for the newly created Template object

        tag: string, optional
            Type of imported template
        contquery : string, optional
            The name of Continuous Query
        project : string, optional
            The name of Project
        session : requests.Session, optionals
            The session object

        Returns
        -------
        :class:`Template`

        '''
        out = cls(template_name)
        out.session = session
        out.project = project
        out.contquery = contquery

        if isinstance(data, six.string_types):
            if re.match(r'^\s*<', data):
                data = data
            elif os.path.isfile(data):
                data = open(data, 'r').read()
            else:
                data = urllib.request.urlopen(data).read().decode('utf-8')
            data = xml.from_xml(data)

        try:
            del data.attrib['name']
        except:
            pass

        try:
            out.tag = data.attrib['tag']
        except:
            out.tag = tag

        out._set_attributes(data.attrib)

        for desc in data.findall('./description'):
            out.description = desc.text

        for item in data.findall('./mas-modules/mas-module'):
            out.mas_modules.append(MASModule.from_xml(item, session=session))

        for item in data.findall('./windows/*'):
            try:
                wcls = get_window_class(item.tag)
            except KeyError:
                raise TypeError('Unknown window type: %s' % item.tag)

            window = wcls.from_xml(item, session=session)
            out.windows[window.base_name] = window

        for item in data.findall('./edges/*'):
            for target in re.split(r'\s+', item.attrib.get('target', '').strip()):
                if not target or target not in out.windows:
                    continue
                for source in re.split(r'\s+', item.attrib.get('source', '').strip()):
                    if not source or source not in out.windows:
                        continue
                    out.windows[source].add_target(out.windows[target], role=item.get('role'),
                                                   slot=item.get('slot'))
        try:
            out.input_windows = re.sub("[^\w]", " ", data.attrib['input-windows']).split()
        except:
            pass

        try:
            out.output_windows = re.sub("[^\w]", " ", data.attrib['output-windows']).split()
        except:
            pass

        for item in data.findall('./metadata/meta'):
            if 'id' in item.attrib.keys():
                out.metadata[item.attrib['id']] = item.text
            elif 'name' in item.attrib.keys():
                out.metadata[item.attrib['name']] = item.text

        for item in data.findall('./required-parameter-map/properties/property'):
            field = item.text.split(',')
            out.required_parameter_map[item.attrib['name']] = field

        return out

    from_element = from_xml

    def to_element(self, template=False):
        '''
        Export template definition to ElementTree.Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''

        extra_attrs = [item for item in ['tag', 'input_windows', 'output_windows'] if getattr(self, item)]
        out = xml.new_elem('template', xml.get_attrs(self, exclude=['name', 'project', 'contquery'], extra=extra_attrs))

        if self.description:
            xml.add_elem(out, 'description', text_content=self.description)

        if self.metadata:
            metadata = xml.add_elem(out, 'metadata')
            for key, value in sorted(six.iteritems(self.metadata)):
                xml.add_elem(metadata, 'meta', attrib=dict(id=key),
                             text_content=value)

        if self.mas_modules:
            mods = xml.add_elem(out, 'mas-modules')
            for item in self.mas_modules:
                xml.add_elem(mods, item.to_element())

        windows = xml.add_elem(out, 'windows')

        sources = {}
        if self.windows:
            edges = []
            for name, window in sorted(six.iteritems(self.windows)):
                win_out = window.to_element()
                if not template:
                    win_out.attrib['name'] = window.base_name
                xml.add_elem(windows, win_out)
                for target in window.targets:
                    sources.setdefault(target.name, []).append(window.name)
                    attrib = dict(source=window.base_name, target=target.base_name)
                    if target.role:
                        attrib['role'] = target.role
                    if target.slot:
                        attrib['slot'] = target.slot
                    edges.append((target._index, attrib))
            if edges:
                elem = xml.add_elem(out, 'edges')
                for i, attrib in sorted(edges):
                    xml.add_elem(elem, 'edge', attrib=attrib)

            if self.required_parameter_map:
                mappings = xml.add_elem(out, 'required-parameter-map')
                xml.add_properties(mappings, self.required_parameter_map, bool_as_int=True)

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

        # return windows, elem
        return out

    def to_xml(self, pretty=False, template=False):
        '''
        Export template definition to XML

        Parameters
        ----------
        pretty : bool, optional
            Should the output embed whitespaced for readability or not, default value is False
        template : bool, optional
            To include template name or not, default value is False

        Returns
        -------
        string

        '''
        return xml.to_xml(self.to_element(template=template), pretty=pretty)

    def save_xml(self, dest, mode='w', pretty=True, **kwargs):
        '''
        Save the template XML to a file

        Parameters
        ----------
        dest : string or file-like
            The destination of the XML content
        mode : string, optional
            The write mode for the output file (only used if `dest` is a string)
        pretty : boolean, optional
            Should the XML include whitespace for readability or not, default value is True

        '''
        if isinstance(dest, six.string_types):
            with open(dest, mode=mode, **kwargs) as output:
                output.write(self.to_xml(pretty=pretty))
        else:
            dest.write(self.to_xml(pretty=pretty))

    def export_to(self, type='xml', pretty=False):
        if type == 'xml':
            return self.to_xml(pretty=pretty)

        return

    def import_from(self, type='xml'):
        if type == 'xml':
            return self.from_xml()

        return

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
        Save the template XML to a file

        Parameters
        ----------
        dest : string or file-like
            The destination of the XML content
        mode : string, optional
            The write mode for the output file (only used if `dest` is a string)
        pretty : boolean, optional
            Should the XML include whitespace for readability or not, default value is True

        '''
        if isinstance(dest, six.string_types):
            with open(dest, mode=mode, **kwargs) as output:
                output.write(self.to_xml(pretty=pretty))
        else:
            dest.write(self.to_xml(pretty=pretty))

    def to_graph(self, graph=None, schema=False, detail=False):
        '''
        Export template definition to graphviz.Digraph

        Parameters
        ----------
        graph : graphviz.Graph, optional
            The parent graph to add to
        schema : bool, optional
            Include window schemas or not, default value is False
        detail : bool, optional
            Show template detail or not, default value is False

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

        label = 'Template-%s: ' % self.tag if self.tag else 'Template: '
        label = label + self.name
        if self.windows and detail:
            tgraph = gv.Digraph(format='svg', name='cluster_%s' % self.fullname.replace('.', '_'))
            tgraph.attr('node', shape='rect')
            tgraph.attr('graph', fontname='helvetica')
            tgraph.attr('edge', fontname='times-italic')
            tgraph.attr(label=label, labeljust='l', style='rounded,bold,dashed', color='blue', fontcolor='black')

            for wkey, window in sorted(self.windows.items()):
                window.to_graph(graph=tgraph,
                                schema=schema or get_option('display.show_schema'))
                for target in window.targets:
                    if target.base_name in self.windows and target.template and target.template.name == self.name:
                        graph.edge(window.fullname, self.windows[target.base_name].fullname, label=target.role or '')

            graph.subgraph(tgraph)

        else:
            graph.node(self.fullname, label=label, labeljust='l',
                       style='bold,filled', color='blue', fillcolor='#f0f0f0',
                       fontcolor='blue', margin='.25,.17', fontname='helvetica')

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
        window : string or Window
            The window to rename
        newname : string
            The new name of the Window object

        '''

        oldname = getattr(window, 'base_name', window)
        self.windows[newname] = self.windows[oldname]
        del self.windows[oldname]

    def delete_windows(self, *windows):
        '''
        Delete windows and update targets

        Parameters
        ----------
        windows : one-or-more strings or Window objects
            The window to delete

        '''
        for item in windows:
            to_delete = self.windows[getattr(item, 'base_name', item)]
            to_delete.template = None
            del self.windows[getattr(item, 'base_name', item)]

    delete_window = delete_windows

    def subscribe(self, mode='streaming', pagesize=50, filter=None,
                  sort=None, interval=None, limit=None, horizon=None, reset=True):
        '''
          Subscribe to events

          Parameters
          ----------
          mode : string, optional
              The mode of subscriber: 'updating' or 'streaming'
          pagesize : int, optional
              The maximum number of events in a page
          filter : string, optional
              Functional filter to subset events
          sort : string, optional
              Sort order for the events (updating mode only)
          interval : int, optional
              Interval between event sends in milliseconds
          limit : int, optional
              The maximum number of rows of data to keep in the internal
              DataFrame object.
          horizon : int or datetime.datetime or string, optional
              Specifies a condition that stops the subscriber.
              If an int, the subscriber stops after than many events.
              If a datetime.datetime, the subscriber stops after the specified
              date and time.  If a string, the string is an expression
              applied to the event using the :meth:`DataFrame.query`
              method.  If that query returns any number of rows, the
              subscriber is stopped.
          reset : bool, optional
              If True, the internal data is reset on subsequent calls
              to the :meth:`subscribe` method.

          See Also
          --------
          :meth:`unsubscribe`
          :class:`Subscriber`

          '''
        for k, win in self.windows.items():
            win.subscribe(mode, pagesize, filter, sort, interval, limit, horizon, reset)

    def unsubscribe(self):
        '''
        Stop event processing

        See Also
        --------
        :meth:`subscribe`

        '''
        for k, win in self.windows.items():
            win.unsubscribe()

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


