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

from .utils import get_args
import inspect
from .base import BaseWindow, attribute, INDEX_TYPES
from .features import (ParametersFeature, SchemaFeature, InputMapFeature,
                       OutputMapFeature, MASMapFeature, ConnectorsFeature)
from .calculate import CalculateWindow
from .helpers import generators


class PythonHelper(BaseWindow, SchemaFeature,
                   ParametersFeature, InputMapFeature, OutputMapFeature,
                   MASMapFeature, ConnectorsFeature):
    '''
    Python Window

    Notes
    -----
    This class is basically a CalculateWindow with the algorithm specified
    to 'MAS'.

    Parameters
    ----------
    name : string, optional
        The name of the window
    schema : Schema, optional
        The schema of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for the
        window and false disables it.
    description : string, optional
        Description of the window
    algorithm : string, optional
        The name of the algorithm
    input_map : dict, optional
        Input mappings
    output_map : dict, optional
        Output mappings
    index_type : string, optional
        Index type for the window
    produces_only_inserts : bool, optional
        Set to true when you know that the procedural window always
        produces inserts
    **parameters : keyword arguments, optional
        The parameters to the algorithm

    Returns
    -------
    :class:`PythonHelper`

    '''

    window_type = 'calculate'

    is_hidden = True

    algorithm = attribute('algorithm', dtype='string')
    index_type = attribute('index', dtype='string', values=INDEX_TYPES)
    produces_only_inserts = attribute('produces-only-inserts', dtype='bool')

    def __init__(self, name=None, schema=None, pubsub=None,
                 description=None, index_type=None,
                 produces_only_inserts=None, input_map=None,
                 output_map=None, mas_info_list=None, **parameters):
        algorithm = 'MAS'
        BaseWindow.__init__(self, **get_args(locals()))
        self.mas_info_list = []

    def _register_to_project(self, project_handle):
        for mas_info in self.mas_info_list:
            module_name = mas_info['module_name']
            entry_func_name = mas_info['entry_func_name']
            source = mas_info['source']
            # If the module doesn't already exist
            if module_name not in [mas_obj.module for mas_obj in project_handle.mas_modules]:
                code = mas_info['code']
                if code is None:
                    raise ValueError('To create a new MAS module, code string or function list must be provided')
                python_module = project_handle.create_mas_module(language="python",
                                                                 module=module_name,
                                                                 func_names='place_holder',
                                                                 code=code)
                project_handle.mas_modules.append(python_module)

            self.add_mas_window_map(module=module_name,
                                    source=source,
                                    function=entry_func_name)

    def add_mas_info(self, module_name, entry_func_name, source, funcs=None, code_file=None, inits=None):
        '''
        Add the information needed to create a MAS module and add MAS window map.

        Notes
        -----
        If code is specified, funcs is ignored.

        Parameters
        ----------
        module_name : string
            Name of the MAS module to be created
        entry_func_name : string
            Name of the entry function in the MAS module.
            An entry functions is supposed to consumes the events sent from
            its source window and returns desired outputs. Any entry function
            must have a doc string to describe its outputs.
        source : string
            Name of the source window
        funcs : list of callable functions
            Functions included in MAS module
        code_file : string
            Path to the Python code file
        inits : dict
            Initialization of global variables if needed

        Returns
        -------
        :class:`Project`

        Examples
        --------
        Create a MAS module with multiple functions and global variables

        >>> win.add_mas_info('foo_module', 'foo_1', 'source_win'
                             funcs=[foo_1, foo_2],
                             inits=dict(gv_1=[], gv_2=0))

        Create a MAS module with a code file. The entry function 'foo_1'
        is defined in the code string.

        >>> win.add_mas_info('foo_module', 'foo_1', 'source_win'
                             code_file='path/to/code.py')

        '''
        if funcs is None and code_file is None:
            code = None
        elif code_file is None:
            code = ''
            # extract code string from function list
            for func in funcs:
                try:
                    code = code + inspect.getsource(func) + '\n'
                except NameError:
                    raise ValueError('{} not found.'.format(func.__name__))
        else:
            code = open(code_file, 'r').read()

        # extract code string from initialization list
        if inits is not None:
            inits_str = '\n'
            for key, value in inits.items():
                inits_str += (key + '=' + str(value) + '\n')
            code = inits_str + code

        mas_info = {'module_name': module_name,
                    'entry_func_name': entry_func_name, 'code': code, 'source': source}
        self.mas_info_list.append(mas_info)


class KerasHelper(PythonHelper):
    '''
    Keras Window

    Parameters
    ----------
    name : string, optional
        The name of the window
    schema : Schema, optional
        The schema of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for the
        window and false disables it.
    description : string, optional
        Description of the window
    algorithm : string, optional
        The name of the algorithm
    input_map : dict, optional
        Input mappings
    output_map : dict, optional
        Output mappings
    index_type : string, optional
        Index type for the window
    produces_only_inserts : bool, optional
        Set to true when you know that the procedural window always
        produces inserts
    **parameters : keyword arguments, optional
        The parameters to the algorithm

    Returns
    -------
    :class:`KerasHelper`

    '''

    def __init__(self, name=None, schema=None, pubsub=None,
                 description=None, index_type=None,
                 produces_only_inserts=None, input_map=None,
                 output_map=None, **parameters):
        PythonHelper.__init__(self, **get_args(locals()))

    def add_model_info(self, model_name, model_file, source, input_name='input', output_name='output', output_class='False'):
        """
        Add the information of a Keras model

        Parameters
        -----------
        model_name : string
            User-specified name of the model
        model_file : string
            The path to hdf5 file that stores the model structure and parameters.
            ESP server should be able to find this file.
        source : string
            Name of the source window
        input_name : string
            Name of input array (features).
            This name should match the schema of the source window
        output_name : string
            Name of output (predictions).
        output_class : bool
            If True, the output is the predicted class. If False, the output is
            an array of pedicted probabilities of each class.
            Only applicable to classification models.

        """
        code_generator = generators.KS_generator(model_file, input_name, output_name, output_class)
        code = code_generator.gen_wrap_str()
        mas_info = {'module_name': model_name,
                    'entry_func_name': 'ks_score', 'code': code, 'source': source}
        self.mas_info_list.append(mas_info)


class TensorflowHelper(PythonHelper):
    '''
    Tensorflow Window

    Parameters
    ----------
    name : string, optional
        The name of the window
    schema : Schema, optional
        The schema of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for the
        window and false disables it.
    description : string, optional
        Description of the window
    algorithm : string, optional
        The name of the algorithm
    input_map : dict, optional
        Input mappings
    output_map : dict, optional
        Output mappings
    index_type : string, optional
        Index type for the window
    produces_only_inserts : bool, optional
        Set to true when you know that the procedural window always
        produces inserts
    **parameters : keyword arguments, optional
        The parameters to the algorithm

    Returns
    -------
    :class:`TensorflowHelper`

    '''

    def __init__(self, name=None, schema=None, pubsub=None,
                 description=None, index_type=None,
                 produces_only_inserts=None, input_map=None,
                 output_map=None, **parameters):
        PythonHelper.__init__(self, **get_args(locals()))

    def add_model_info(self, model_name, model_file, input_op, score_op,
                       source, input_name='input', output_name='output'):
        """
        Add the information of a Tensorflow model

        Parameters
        -----------
        model_name : string
            User-specified name of the model
        model_file : string
            the path to meta file that stores the graph structure.
            The checkpoint files should be within the same directory with the meta file.
            ESP server should be able to find the files.
        input_op : string
            Name of input operation
        score_op : string
            Name of scoring operation
        source : string
            Name of the source window
        input_name : string
            Name of input array (features).
            This name should match the schema of the source window
        output_name : string
            Name of output (predictions).

        Notes
        -----
        The Tensorflow models are expoted in checkpoint files using tf.train.Saver class.
        The name of input and scoring operations should be specified when creating the model.
        """
        code_generator = generators.TF_generator(model_file, input_op, score_op,
                                                 input_name, output_name)
        code = code_generator.gen_wrap_str()
        mas_info = {'module_name': model_name,
                    'entry_func_name': 'tf_score', 'code': code, 'source': source}
        self.mas_info_list.append(mas_info)


class JMPHelper(PythonHelper):
    '''
    JMP Window

    Parameters
    ----------
    name : string, optional
        The name of the window
    schema : Schema, optional
        The schema of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for the
        window and false disables it.
    description : string, optional
        Description of the window
    algorithm : string, optional
        The name of the algorithm
    input_map : dict, optional
        Input mappings
    output_map : dict, optional
        Output mappings
    index_type : string, optional
        Index type for the window
    produces_only_inserts : bool, optional
        Set to true when you know that the procedural window always
        produces inserts
    copy_vars : string or list of string, optional
        Add fields to the automatically generated schema
    **parameters : keyword arguments, optional
        The parameters to the algorithm

    Returns
    -------
    :class:`JMPHelper`

    Notes
    -----
    If no schema is provided users, a schema will be automatically generated
    based on the outputs of the score function from JMP.
    '''

    def __init__(self, name=None, schema=None, pubsub=None,
                 description=None, index_type=None,
                 produces_only_inserts=None, input_map=None,
                 output_map=None, copy_vars=None, **parameters):
        PythonHelper.__init__(self, **get_args(locals()))
        self.copy_vars = copy_vars

    def add_model_info(self, model_name, model_file, source):
        """
        Add the information of a JMP model

        Parameters
        -----------
        module_name : string
            Name of the MAS module to be created
        score_file : string
            The path to the Python file exported by JMP
        source : string
            Name of the source window
        """
        code_generator = generators.JMP_generator(model_file)
        code = code_generator.gen_wrap_str()
        mas_info = {'module_name': model_name,
                    'entry_func_name': 'jmp_score', 'code': code, 'source': source}
        self.mas_info_list.append(mas_info)

        if self._schema.schema_string == '':
            self.schema = code_generator._gen_schema(self.copy_vars)


setattr(CalculateWindow, PythonHelper.__name__, PythonHelper)
setattr(CalculateWindow, KerasHelper.__name__, KerasHelper)
setattr(CalculateWindow, TensorflowHelper.__name__, TensorflowHelper)
setattr(CalculateWindow, JMPHelper.__name__, JMPHelper)
