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

import copy
import six
import unittest
from . import utils as tm
from ..config import (get_option, set_option, reset_option, describe_option, options, 
                          get_suboptions, ESPOptionError, get_default,
                          check_int, check_float, check_string, check_url, check_boolean)
from ..utils.config import subscribe, _subscribers, unsubscribe


class TestConfig(tm.TestCase):

    def setUp(self):
        reset_option()

    def tearDown(self):
        reset_option()

    def test_basic(self):
        self.assertEqual(get_option('display.show_schema'), False)
#       self.assertEqual(get_option('display.notebook.repr_html'), True)
#       self.assertEqual(get_option('display.notebook.repr_javascript'), False)

        set_option('display.show_schema', True)

        self.assertEqual(get_option('display.show_schema'), True)

        with self.assertRaises(ESPOptionError):
            options.display.show_schema = 'foo'

        options.display.show_schema = False
        self.assertEqual(options.display.show_schema, False)

        with self.assertRaises(ESPOptionError):
            options.display.show_schema = 10

        self.assertEqual(options.display.show_schema, False)

        self.assertEqual(type(options.display), type(options))

        # This key exists, but it's a level in the hierarchy, not an option
#       with self.assertRaises(ESPOptionError):
#           get_option('display.notebook.css')

        options.display.show_schema = True

        reset_option('display.show_schema')

        self.assertEqual(options.display.show_schema, False)

        with self.assertRaises(ESPOptionError):
            reset_option('display.foo')
 
        with self.assertRaises(ESPOptionError):
            reset_option('display')

    def test_shortcut_options(self):
        debug_events = get_option('debug.events')
        image_scale = get_option('display.image_scale')

        self.assertEqual(get_option('debug.events'), debug_events)
        self.assertEqual(options.debug.events, debug_events)

        options.debug.events = True

        self.assertEqual(get_option('debug.events'), True)
        self.assertEqual(options.debug.events, True)
        self.assertEqual(options.events, True)

        self.assertEqual(get_option('image_scale'), image_scale)
        self.assertEqual(get_option('display.image_scale'), image_scale)
        self.assertEqual(options.image_scale, image_scale)

        options.image_scale = 0.8
        
        self.assertEqual(get_option('image_scale'), 0.8)
        self.assertEqual(get_option('display.image_scale'), 0.8)
        self.assertEqual(options.image_scale, 0.8)

        reset_option('image_scale')

        self.assertEqual(get_option('image_scale'), 1.0)
        self.assertEqual(get_option('display.image_scale'), 1.0)
        self.assertEqual(options.image_scale, 1.0)

    def test_missing_options(self):
        with self.assertRaises(ESPOptionError):
            set_option('debug.foo', 10) 

        with self.assertRaises(ESPOptionError):
            options.debug.foo = 10

        with self.assertRaises(ESPOptionError):
            get_option('debug.foo') 

        with self.assertRaises(ESPOptionError):
            print(options.debug.foo)

        # You can not access a midpoint in the hierarchy with (s|g)et_option
        with self.assertRaises(ESPOptionError):
            set_option('debug', 10)

        with self.assertRaises(ESPOptionError):
            get_option('debug')

    def test_function_subscribers(self):
        opts = {}

        def options_subscriber(key, value, opts=opts):
            opts[key] = value

        num_subscribers = len(_subscribers) 

        subscribe(options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers + 1) 

        options.display.show_schema = True
        self.assertEqual(opts, {'display.show_schema': True})

        options.display.show_schema = False
        self.assertEqual(opts, {'display.show_schema': False})

        options.display.image_scale = 0.8
        self.assertEqual(opts, {'display.show_schema': False, 'display.image_scale': 0.8})

        options.display.image_scale = 0.8
        self.assertEqual(opts, {'display.show_schema': False, 'display.image_scale': 0.8})

        options.display.show_schema = True
        self.assertEqual(opts, {'display.show_schema': True, 'display.image_scale': 0.8})

        options.display.show_schema = False
        reset_option('display.show_schema')
        self.assertEqual(opts, {'display.show_schema': False, 'display.image_scale': 0.8})

        unsubscribe(options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers) 

        subscribe(options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers + 1) 

        del options_subscriber

        self.assertEqual(len(_subscribers), num_subscribers) 

        options.display.show_schema = False

        self.assertEqual(opts, {'display.show_schema': False, 'display.image_scale': 0.8})

    def _test_method_subscribers(self):
        opts = {}

        class OptionsSubscriber(object):
            def options_subscriber(self, key, value, opts=opts):
                opts[key] = value
        os = OptionsSubscriber()

        num_subscribers = len(_subscribers)

        subscribe(os.options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers + 1)

        options.display.show_schema = False
        self.assertEqual(opts, {'display.show_schema': False})

        options.display.show_schema = True
        self.assertEqual(opts, {'display.show_schema': True})

        options.debug.events = True
        self.assertEqual(opts, {'display.show_schema': True, 'debug.events': True})

        options.debug.events = False
        self.assertEqual(opts, {'display.show_schema': True, 'debug.events': False})

        options.display.show_schema = False
        self.assertEqual(opts, {'display.show_schema': False, 'debug.events': False})

        reset_option('display.show_schema')
        self.assertEqual(opts, {'display.show_schema': True, 'debug.events': False})

        unsubscribe(os.options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers)

        subscribe(os.options_subscriber)

        self.assertEqual(len(_subscribers), num_subscribers + 1)

        del os.options_subscriber

        self.assertEqual(len(_subscribers), num_subscribers)

        options.display.show_schema = False

        self.assertEqual(opts, {'display.show_schema': True, 'debug.events': False})

    def test_errors(self):
        with self.assertRaises(ESPOptionError):
            set_option('display.show_schema', 'hi')

    def test_doc(self):
        out = describe_option('display.show_schema', 'debug.events', _print_desc=False)
        for line in out.split('\n'):
            if not line or line.startswith(' '):
                continue
            self.assertRegex(line, r'^(display\.show_schema|debug\.events)')

        # Displays entire option hierarchy
        out = describe_option('debug', _print_desc=False)
        for line in out.split('\n'):
            if not line or line.startswith(' '):
                continue
            self.assertRegex(line, r'^debug\.')

        with self.assertRaises(ESPOptionError):
           describe_option('debug.foo')

        out = describe_option(_print_desc=False)
        self.assertRegex(out, r'\bdebug\.events :')
        self.assertRegex(out, r'\bdebug\.requests :')
        self.assertRegex(out, r'\bhostname :')
        self.assertRegex(out, r'\bport :')

    def test_suboptions(self):
        self.assertEqual(list(sorted(get_suboptions('debug').keys())), 
                          ['events', 'request_bodies', 'requests', 'responses'])

        with self.assertRaises(ESPOptionError):
            get_suboptions('display.foo')

        # This is an option, not a level in the hierarchy
        with self.assertRaises(ESPOptionError):
            get_suboptions('display.show_schema')

    def test_get_default(self):
        self.assertEqual(get_default('display.show_schema'), False)

        with self.assertRaises(ESPOptionError):
            get_default('display.foo')

        # This is a level in the hierarchy, not an option
        with self.assertRaises(ESPOptionError):
            get_default('debug')

    def test_check_int(self):
        self.assertEqual(check_int(10), 10) 
        self.assertEqual(check_int(999999999999), 999999999999) 
        self.assertEqual(check_int('10'), 10) 

        with self.assertRaises(ESPOptionError):
            check_int('foo')

        self.assertEqual(check_int(10, minimum=9), 10) 
        self.assertEqual(check_int(10, minimum=10), 10) 
        with self.assertRaises(ESPOptionError):
            check_int(10, minimum=11)
       
        self.assertEqual(check_int(10, minimum=9, exclusive_minimum=True), 10) 
        with self.assertRaises(ESPOptionError):
            check_int(10, minimum=10, exclusive_minimum=True)
        with self.assertRaises(ESPOptionError):
            check_int(10, minimum=11, exclusive_minimum=True)
       
        self.assertEqual(check_int(10, maximum=11), 10) 
        self.assertEqual(check_int(10, maximum=10), 10) 
        with self.assertRaises(ESPOptionError):
            check_int(10, maximum=9)
       
        self.assertEqual(check_int(10, maximum=11, exclusive_minimum=True), 10) 
        with self.assertRaises(ESPOptionError):
            check_int(10, maximum=10, exclusive_maximum=True)
        with self.assertRaises(ESPOptionError):
            check_int(10, maximum=9, exclusive_maximum=True)

        self.assertEqual(check_int(10, multiple_of=5), 10)
        with self.assertRaises(ESPOptionError):
            check_int(10, multiple_of=3) 

    def test_check_float(self):
        self.assertEqual(check_float(123.567), 123.567)
        self.assertEqual(check_float(999999999999.999), 999999999999.999)
        self.assertEqual(check_float('123.567'), 123.567)

        with self.assertRaises(ESPOptionError):
            check_float('foo')

        self.assertEqual(check_float(123.567, minimum=123.566), 123.567)
        self.assertEqual(check_float(123.567, minimum=123.567), 123.567)
        with self.assertRaises(ESPOptionError):
            check_float(123.567, minimum=123.577)

        self.assertEqual(check_float(123.567, minimum=123.566, exclusive_minimum=True), 123.567)
        with self.assertRaises(ESPOptionError):
            check_float(123.567, minimum=123.567, exclusive_minimum=True)
        with self.assertRaises(ESPOptionError):
            check_float(123.567, minimum=123.568, exclusive_minimum=True)

        self.assertEqual(check_float(123.567, maximum=123.568), 123.567)
        self.assertEqual(check_float(123.567, maximum=123.567), 123.567)
        with self.assertRaises(ESPOptionError):
            check_float(123.567, maximum=123.566)

        self.assertEqual(check_float(123.567, maximum=123.567, exclusive_minimum=True), 123.567)
        with self.assertRaises(ESPOptionError):
            check_float(123.567, maximum=123.567, exclusive_maximum=True)
        with self.assertRaises(ESPOptionError):
            check_float(123.567, maximum=123.566, exclusive_maximum=True)

        with self.assertRaises(ESPOptionError):
            check_float(123.567, multiple_of=3)

    def test_check_string(self):
        self.assertEqual(check_string('hi there'), 'hi there')
        self.assertTrue(isinstance(check_string('hi there'), six.string_types))

        self.assertEqual(check_string('hi there', pattern=r' th'), 'hi there')
        with self.assertRaises(ESPOptionError):
            check_string('hi there', pattern=r' th$')

        self.assertEqual(check_string('hi there', max_length=20), 'hi there')
        self.assertEqual(check_string('hi there', max_length=8), 'hi there')
        with self.assertRaises(ESPOptionError):
            check_string('hi there', max_length=7)

        self.assertEqual(check_string('hi there', min_length=3), 'hi there')
        self.assertEqual(check_string('hi there', min_length=8), 'hi there')
        with self.assertRaises(ESPOptionError):
            check_string('hi there', min_length=9)

        self.assertEqual(check_string('hi there', valid_values=['hi there', 'bye now']), 'hi there')
        with self.assertRaises(ESPOptionError):
            check_string('foo', valid_values=['hi there', 'bye now']) 

        # Invalid utf8 data
        with self.assertRaises(ESPOptionError):
            check_string(b'\xff\xfeW[')

    def test_check_url(self):
        self.assertEqual(check_url('hi there'), 'hi there')
        self.assertTrue(isinstance(check_url('hi there'), six.string_types))

        # Invalid utf8 data
        with self.assertRaises(ESPOptionError):
            check_url(b'\xff\xfeW[')

    def test_check_boolean(self):
        self.assertEqual(check_boolean(True), True)
        self.assertEqual(check_boolean(False), False)
        self.assertEqual(check_boolean(1), True)
        self.assertEqual(check_boolean(0), False)

        with self.assertRaises(ESPOptionError):
            check_boolean(2)
        with self.assertRaises(ESPOptionError):
            check_boolean('true')
        with self.assertRaises(ESPOptionError):
            check_boolean(1.1)


if __name__ == '__main__':
    tm.runtests()
