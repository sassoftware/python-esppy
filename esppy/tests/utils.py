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

'''
Utilities for testing

'''

from __future__ import print_function, division, absolute_import, unicode_literals

import copy
import difflib
import os
import re
import sys
import unittest
import six
from ..utils import xml


class TestCase(unittest.TestCase):
    ''' TestCase with SWAT Customizations '''

    def assertContentsEqual(self, file1, file2):
        if os.path.isfile(file1):
            with open(file1, 'rb') as infile:
                file1 = infile.read().decode('utf-8').split('\n')
        else:
            file1 = file1.split('\n')

        if os.path.isfile(file2):
            with open(file2, 'rb') as infile:
                file2 = infile.read().decode('utf-8').split('\n')
        else:
            file1 = file1.split('\n')

        out = list(difflib.Differ().compare(file1, file2) )
        print(out)
        if out:
            for line in out:
                print(line)
            raise ValueError('Files are not equal')

    def assertRegex(self, *args, **kwargs):
        ''' Compatibility across unittest versions '''
        if hasattr(unittest.TestCase, 'assertRegex'):
            return unittest.TestCase.assertRegex(self, *args, **kwargs)
        return self.assertRegexpMatches(*args, **kwargs)

    def assertNotEqual(self, *args, **kwargs):
        ''' Compatibility across unittest versions '''
        if hasattr(unittest.TestCase, 'assertNotEqual'):
            return unittest.TestCase.assertNotEqual(self, *args, **kwargs)
        return self.assertNotEquals(*args, **kwargs)

    def assertEqual(self, *args, **kwargs):
        ''' Compatibility across unittest versions '''
        if hasattr(unittest.TestCase, 'assertEqual'):
            return unittest.TestCase.assertEqual(self, *args, **kwargs)
        return self.assertEquals(*args, **kwargs)


def get_data_dir():
    ''' Return the testing data directory '''
    return os.path.join(os.path.dirname(__file__), 'data')


def file_contents(*args, encoding='utf-8'):
    ''' Return the contents of the file at the given path '''
    with open(os.path.join(*args), 'rb') as infile:
        return infile.read().decode('utf-8') 


def get_user_pass():
    ''' 
    Get the username and password from the environment if possible 

    If the environment does not contain a username and password,
    they will be retrieved from a ~/.authinfo file.

    '''
    username = None
    password = None
    if 'ESPUSER' in os.environ:
        username = os.environ['ESPUSER'] 
    if 'ESPPASSWORD' in os.environ:
        password = os.environ['ESPPASSWORD'] 
    return username, password


def get_host_port_proto():
    ''' 
    Get the host, port and protocol from a .esprc file 

    NOTE: .esprc files are written in Lua

    Returns
    -------
    (esphost, espport, espprotocol)

    '''
    esphost = os.environ.get('ESPHOST')
    espport = os.environ.get('ESPPORT')
    espprotocol = os.environ.get('ESPPROTOCOL')

    if espport is not None:
        espport = int(espport)

    if esphost and espport:
        return esphost, espport, espprotocol

    # If there is no host or port in the environment, look for .esprc
    esprc = None
    rcname = '.esprc'
    homepath = os.path.join(os.path.expanduser(os.environ.get('HOME', '~')), rcname)
    upath = os.path.join(r'u:', rcname)
    cfgfile = os.path.abspath(os.path.normpath(rcname))

    while not os.path.isfile(cfgfile):
        if os.path.samefile(os.path.dirname(homepath), os.path.dirname(cfgfile)):
            break
        newcfgfile = os.path.abspath(os.path.normpath(rcname)) 
        if os.path.samefile(os.path.dirname(cfgfile), os.path.dirname(newcfgfile)):
            break

    if os.path.isfile(cfgfile):
        esprc = cfgfile
    elif os.path.exists(homepath):
        esprc = homepath
    elif os.path.exists(upath):
        esprc = upath
    else:
        return esphost, espport, espprotocol

    return _read_esprc(esprc)


def _read_esprc(path):
    '''
    Read the .esprc file using Lua 

    Parameters
    ----------
    path : string
        Path to the .esprc file

    Returns
    -------
    (esphost, espport, espprotocol)

    '''
    esphost = None
    espport = None
    espprotocol = None

    if not os.path.isfile(path):
        return esphost, espport, espprotocol

    try:
        from lupa import LuaRuntime
        lua = LuaRuntime()
        lua.eval('dofile("%s")' % path)
        lg = lua.globals()

    except ImportError:
        import subprocess
        import tempfile

        lua_script = tempfile.TemporaryFile(mode='w')
        lua_script.write('''
            if arg[1] then
                dofile(arg[1])
                for name, value in pairs(_G) do
                    if name:match('esp') then
                        print(name .. ' ' .. tostring(value))
                    end
                end
            end
        ''')
        lua_script.seek(0)

        class LuaGlobals(object):
            pass

        lg = LuaGlobals()

        config = None
        try:
            config = subprocess.check_output('lua - %s' % path, stdin=lua_script,
                                             shell=True).strip().decode('utf-8')
        except (OSError, IOError, subprocess.CalledProcessError):
            pass
        finally:
            lua_script.close()

        if config:
            for name, value in re.findall(r'^\s*(esp\w+)\s+(.+?)\s*$', config, re.M):
                setattr(lg, name, value)
        else:
            # Try to parse manually
            config = re.sub(r'\-\-.*?$', r' ', open(path, 'r').read(), flags=re.M)
            for name, value in re.findall(r'\b(esp\w+)\s*=\s*(\S+)(?:\s+|\s*$)', config):
                setattr(lg, name, eval(value))

    try:
       esphost = str(lg.esphost)
    except:
       sys.sterr.write('ERROR: Could not access esphost setting\n')
       sys.exit(1)

    try:
       espport = int(lg.espport)
    except:
       sys.sterr.write('ERROR: Could not access espport setting\n')
       sys.exit(1)

    try:
       if lg.espprotocol:
           espprotocol = str(lg.espprotocol)
    except:
       pass

    return esphost, espport, espprotocol


def runtests(xmlrunner=False):
   ''' Run unit tests '''
   import sys

   if '--profile' in sys.argv:
       import profile
       import pstats

       sys.argv = [x for x in sys.argv if x != '--profile']

       if xmlrunner:
           import xmlrunner as xr
           profile.run("unittest.main(testRunner=xr.XMLTestRunner(output='test-reports', verbosity=2))", '_stats.txt')
       else:
           profile.run('unittest.main()', '_stats.txt')

       stats = pstats.Stats('_stats.txt')
       #stats.strip_dirs()
       stats.sort_stats('cumulative', 'calls')
       stats.print_stats(25)
       stats.sort_stats('time', 'calls')
       stats.print_stats(25)

   elif xmlrunner:
       import xmlrunner as xr
       unittest.main(testRunner=xr.XMLTestRunner(output='test-reports', verbosity=2)) 

   else:
       unittest.main()


def normalize_xml(s):
    # Remove http-servers
    s = re.sub(r'<http-servers>.*?</http-servers>', r'', s, flags=re.S)

    # Normalize connector type
    s = re.sub(r'(<connector[^>]+)(>\s*<properties>)\s*<property\s+name=[\'"]type[\'"]>(pub(?:lish)?|sub(?:scribe)?)</property>', r'\1 type="\3"\2', s)

    # Use full name of type
    s = re.sub(r'(type=[\'"])pub([\'"])', r'\1publish\2', s)
    s = re.sub(r'(type=[\'"])sub([\'"])', r'\1subscribe\2', s)

    # Remove CDATA from properties
    s = re.sub(r'(<property[^>]+>)<!\[CDATA\[(.+?)\]\]>(</property>)', r'\1\2\3', s)

    # Remove port= / copy-keys=
    s = re.sub(r'\s*(port|copy-keys)=[\'"][^\'"]*[\'"]\s*', r'', s)

    # Remove default attributes
    s = re.sub(r'\s*connection-timeout=[\'"]300[\'"]', r'', s)
    s = re.sub(r'\s*max-string-length=[\'"]32000[\'"]', r'', s)

    # Remove key='false'
    s = re.sub(r'\s+key=[\'"]false[\'"]', r'', s)

    # Remove engine / edges description
    s = re.sub(r'(<(engine|edges)[^>]*>\s*)<description>.*?</description>',
               r'\1', s, flags=re.S)

    # Normalize description whitespace
    s = re.sub(r'(<description>)\s*(.*?)\s*(</description>)',
               r'\1\2\3', s, flags=re.S)

    # Remove full path of files
    s = re.sub(r'(<property name=[\'"]fsname[\'"]>).*?(\w+\.\w+)(</property>)', r'\1\2\3', s)
    s = re.sub(r'(<code-file>).*?(\w+\.\w+)(</code-file>)', r'\1\2\3', s)

    # Expand schema strings
    s = re.sub(r'<schema-string>(.*?)</schema-string>', schema_string_to_schema, s)

    # Expand edges
    s = re.sub(r'(<edge [^>]*/>)', expand_edge, s)

    # Add engine / projects wrapper
    if re.match(r'^\s*<project\b', s):
        s = '<engine>\n<projects>\n%s\n</projects>\n</engine>\n' % s

    return s


def strip(s):
    return (s or '').strip()


def expand_edge(edge):
    out = []
    edge = xml.ensure_element(edge.group(1))
    sources = re.split(r'\s+', edge.attrib['source'].strip())
    targets = re.split(r'\s+', edge.attrib['target'].strip())
    for src in sources:
        for tgt in targets:
            new_elem = copy.deepcopy(edge)
            new_elem.attrib['source'] = src
            new_elem.attrib['target'] = tgt
            out.append(xml.to_xml(new_elem))
    return '\n'.join(out) 


def schema_string_to_schema(schema):
    schema = schema.group(1)
    out = ['<schema>', '<fields>']
    for item in re.split(r'\s*,\s*', schema.strip()):
        name, dtype = item.split(':')
        is_key = '*' in name and 'true' or 'false'
        name = name.replace('*', '')
        out.append('<field name="%s" type="%s" key="%s" />' % (name, dtype, is_key))
    out.append('</fields>')
    out.append('</schema>')
    return '\n'.join(out)


def elements_equal(e1, e2, path='/'):
    '''
    Compare elements (in an order-independent manner)

    Parameters
    ----------
    e1 : Element
        The first element
    e2 : Element
        The second element

    Raises
    ------
    ValueError
        Any difference raises a ValueError

    '''
    if isinstance(e1, six.string_types):
        e1 = xml.ensure_element(normalize_xml(e1))
    if isinstance(e2, six.string_types):
        e2 = xml.ensure_element(normalize_xml(e2))

    if e1.tag != e2.tag:
        raise ValueError('%s%s != %s%s' % (path, e1.tag, path, e2.tag))

    if strip(e1.text) != strip(e2.text):
        raise ValueError('In %s%s: %s != %s' % (path, e1.tag, strip(e1.text), strip(e2.text)))

    if strip(e1.tail) != strip(e2.tail):
        raise ValueError('In %s%s: %s != %s' % (path, e1.tag, strip(e1.tail), strip(e2.tail)))

    if e1.attrib != e2.attrib:
        raise ValueError('In %s%s: %s != %s' % (path, e1.tag, e1.attrib, e2.attrib))

    e1_children = sorted(list(e1), key=lambda x: (x.tag, x.attrib.get('name', ''), x.attrib.get('source', ''), x.attrib.get('target', ''), x.attrib.get('connector')))
    e2_children = sorted(list(e2), key=lambda x: (x.tag, x.attrib.get('name', ''), x.attrib.get('source', ''), x.attrib.get('target', ''), x.attrib.get('connector', '')))

    e1_tags = [x.tag for x in e1_children]
    e2_tags = [x.tag for x in e2_children]

    if e1_tags != e2_tags:
        print(xml.to_xml(e1, pretty=True))
        print(xml.to_xml(e2, pretty=True))
        raise ValueError('In %s%s: %s != %s' % (path, e1.tag, e1_tags, e2_tags))

    for c1, c2 in zip(e1_children, e2_children):
        elements_equal(c1, c2, path='%s%s/' % (path, e1.tag))
