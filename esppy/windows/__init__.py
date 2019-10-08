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

from .base import BaseWindow, Window, get_window_class, Target
from .subscriber import Subscriber
from .publisher import Publisher
from .aggregate import AggregateWindow
from .calculate import CalculateWindow
from .copy import CopyWindow
from .compute import ComputeWindow
from .counter import CounterWindow
from .filter import FilterWindow
from .functional import FunctionalWindow
from .geofence import GeofenceWindow
from .join import JoinWindow
from .modelsuper import ModelSupervisorWindow
from .modelreader import ModelReaderWindow
from .notification import NotificationWindow
from .objectTracker import ObjectTrackerWindow
from .pattern import PatternWindow
from .procedural import ProceduralWindow
from .removeState import RemoveStateWindow
from .score import ScoreWindow
from .source import SourceWindow
from .textcategory import TextCategoryWindow
from .textcontext import TextContextWindow
from .textsentiment import TextSentimentWindow
from .texttopic import TextTopicWindow
from .train import TrainWindow
from .transpose import TransposeWindow
from .union import UnionWindow
from .pythonmas import PythonHelper


def get_subclasses(cls):
    for subclass in cls.__subclasses__():
        for subcls in get_subclasses(subclass):
            yield subcls
        yield subclass


for cls in get_subclasses(BaseWindow):
    if cls.window_type and not cls.is_hidden:
        BaseWindow.window_classes['window-%s' % cls.window_type] = cls
