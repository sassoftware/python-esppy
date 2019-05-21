import abc
import inspect
import ntpath
from keyword import iskeyword
import re
import six
import textwrap


def _is_valid_name(name):
    '''
    check if a string is a valid identifier in Python
    '''
    return name.isidentifier() and not iskeyword(name)


def _to_valid_name(varStr):
    return re.sub('\W|^(?=\d)', '_', varStr)


def _to_valid_dict(old_dict):
    '''
    convert the keys in a dict to valid identifiers

    Parameters:
    -------------
    old_dict: dict
        a dict whose keys may not be valid Python identifiers

    Returns
    -------------
    new_dict: dict
        a dict with all keys being valid Python identifiers
    map_dict: dict
        a dict recording the convert
        key: converted key; value: original key

    Examples:
    -------------
    old_key = {'a b':'float', 'c':'str'}
    new_dict = {'a_b': 'float', 'c':'str'}
    map_dict = {'a_b':'a b'}

    '''
    # dict to hold the valid variable name and its corrsponding datatype
    new_dict = old_dict.copy()
    # dict to hold the valid variable name and its corrsponding non-valid name
    map_dict = dict()
    for key in old_dict:
        # check if key is a valid variable name
        if not _is_valid_name(key):
            # make a valid name
            new_dict[_to_valid_name(key)] = new_dict.pop(key)
            # record it in map_dict
            map_dict[_to_valid_name(key)] = key
    return new_dict, map_dict


class wrap_generator(six.with_metaclass(abc.ABCMeta)):
    @abc.abstractmethod
    def gen_wrap_str(self):
        raise NotImplementedError("gen_wrap_str must be defined!")

    @property
    @abc.abstractmethod
    def type(self):
        raise NotImplementedError(
            "the type of the generator must be specified!")


class KS_generator(wrap_generator):
    """
    Generator to generate wrapper code for exported Keras models.

    Parameters
    -----------
    h5_file : string
        The path to hdf5 file that stores the model structure and parameters.
        ESP server should be able to find this file.
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
    type = 'keras'

    def __init__(self, h5_file, input_name='input', output_name='output', output_class=False):
        self.file = h5_file
        self.input_name = input_name
        self.output_name = output_name
        self.output_class = output_class

    def gen_wrap_str(self):
        if self.output_class:
            predict = 'predict_classes'
        else:
            predict = 'predict'

        wrap_str = '''
model = None
def ks_score({0}):
    "Output: {1}"
    from keras.models import load_model
    import tensorflow as tf
    import numpy as np
    global model
    global graph
    # If it is called for the first time, restore the model
    if model is None:
        model = load_model('{2}')
        model._make_predict_function()
        graph = tf.get_default_graph()

    # make prediction
    {0}_wrap = np.array([{0}])
    with graph.as_default():
        {1} = model.{3}({0}_wrap)[0]

    if isinstance({1}, np.ndarray):
        {1} = {1}.tolist()
    else:
        {1} = {1}.item()

    return {1}'''.format(self.input_name, self.output_name, self.file, predict)
        return wrap_str


class TF_generator(wrap_generator):
    """
    Generator to generate wrapper code for exported Tensorflow models.

    Parameters
    -----------
    meta_file : string
        the path to meta file that stores the graph structure.
        The checkpoint files should be within the same directory with the meta file.
        ESP server should be able to find the files.
    input_op : string
        Name of input operation
    score_op : string
        Name of scoring operation
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
    type = 'tensorflow'

    def __init__(self, meta_file, input_op, score_op, input_name='input', output_name='output'):
        self.file = meta_file
        self.input_op = input_op
        self.score_op = score_op
        self.input_name = input_name
        self.output_name = output_name

    def gen_wrap_str(self):
        dir_path = ntpath.dirname(self.file) + '/'
        wrap_str = '''
sess = None
def tf_score({4}):
    "Output: {5}"
    import tensorflow as tf
    import numpy as np
    global sess
    global score_op
    global input_op
    #If it is called for the first time, restore the model and necessary operations
    if sess is None:
        sess=tf.Session()
        #load meta graph and restore weights
        saver = tf.train.import_meta_graph('{0}')
        saver.restore(sess,tf.train.latest_checkpoint('{1}'))

        graph = tf.get_default_graph()
        #restore the ops. Both ops were pre-defined in the model.
        input_op = graph.get_tensor_by_name("{2}:0") #op to feed input data
        score_op = graph.get_tensor_by_name("{3}:0")    #op to score the input

    #Note that the feed value of x has shape (?,xyz), NOT (,xyz)
    {4}_wrap = np.array([{4}])
    {5} = sess.run(score_op, feed_dict={{input_op: {4}_wrap}})[0]

    if isinstance({5}, np.ndarray):
        {5} = {5}.tolist()
    else:
        {5} = {5}.item()

    return {5}'''.format(self.file, dir_path, self.input_op, self.score_op,
                         self.input_name, self.output_name)

        return wrap_str


class JMP_generator(wrap_generator):
    """
    Add the infos of a JMP model

    Parameters
    -----------
    score_file : string
        The path to the model file exported by JMP
    """
    type = 'jmp'

    def __init__(self, score_file):
        file_string = open(score_file, 'r').read()
        file_string = file_string.replace('import jmp_score as jmp', '')
        file_string = file_string.replace('from __future__ import division', '')
        file_string = file_string.replace('from math import *', '')
        file_string = file_string.replace('import numpy as np', '')
        self.file_string = file_string
        six.exec_(self.file_string, globals())

        try:
            self.input_dict = getInputMetadata()
            self.output_dict = getOutputMetadata()
            self.model_dict = getModelMetadata()
        except NameError:
            raise ValueError("The file is not a valid Python scroing file from JMP")

    def _gen_schema(self, copy_var=None):
        # generate schema if not provided by user
        valid_output_dict, _ = _to_valid_dict(self.output_dict)
        type_dict = {'str': 'string', 'float': 'double'}
        schema = ['id*:int64']
        for key, value in valid_output_dict.items():
            for jmp_type, esp_type in type_dict.items():
                value = value.replace(jmp_type, esp_type)
            schema.append(key + ':' + value)

        if copy_var is not None:
            if isinstance(copy_var, six.string_types):
                schema.append(copy_var)
            elif isinstance(copy_var, (tuple, list)):
                schema = schema + list(copy_var)
        return schema

    def _set_jmp_funcs(self):
        from . import jmp_score
        func_source = inspect.getsource(jmp_score)
        # empty jmp class
        jmp_class = '''\
class jmp:
    pass\n\n'''
        # set each function in jmp_score as an attribute of jmp class
        set_attr = ''''''
        func_list = [o for o in inspect.getmembers(jmp_score, inspect.isfunction)]
        for func_name, _ in func_list:
            set_attr += '''setattr(jmp, "{0}", {0})\n'''.format(func_name)
        return func_source + jmp_class + set_attr

    def gen_wrap_str(self):
        valid_input_dict, input_map = _to_valid_dict(self.input_dict)
        valid_output_dict, output_map = _to_valid_dict(self.output_dict)

        intent = '    '
        # import necessary modules
        import_str = '''
from __future__ import division
import inspect
import numpy as np
from math import *\n\n'''

        jmp_funcs = self._set_jmp_funcs()
        jmp_class = '''
try:
    temp = jmp()
except NameError:
{}\n'''.format(textwrap.indent(jmp_funcs, 4 * ' '))

        # original score function
        orig_code = self.file_string + '\n\n'

        # create the signiture of the wrapper
        signiture = "def jmp_score(" + \
            ",".join(valid_input_dict.keys()) + '):\n'

        # create the docstring
        docstring = '''"Output: ''' + \
            ", ".join(valid_output_dict.keys()) + '''"'''

        # convert tuple to dict(indata)
        t_to_dict = '''
    frame = inspect.currentframe()
    _, _, _, values = inspect.getargvalues(frame)
    values.pop('frame', None)
    indata = values.copy()\n'''
        for key, value in input_map.items():
            t_to_dict += intent + \
                '''indata["''' + value + \
                '''"] = indata.pop("''' + key + '''")\n'''

        # score the input
        score_input = '''
    outdata=dict()
    score(indata, outdata)\n'''

        # convert outdata(dict)
        outdata_split = ''
        for key, value in output_map.items():
            outdata_split += intent + \
                '''outdata["''' + key + \
                '''"] = outdata.pop("''' + value + '''")\n'''
        for key in valid_output_dict:
            outdata_split += intent + key + \
                '''= outdata["''' + key + '''"]\n'''

        # return outputs
        return_outputs = '''return ''' + ",".join(valid_output_dict.keys())

        return import_str + jmp_class + orig_code + signiture + intent + docstring + \
            t_to_dict + score_input + outdata_split + intent + return_outputs
