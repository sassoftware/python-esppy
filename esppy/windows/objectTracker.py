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

from xml.etree import ElementTree as xml
from .base import Window, attribute
from .features import WindowFeature
from .utils import get_args

class TrackerFeature(WindowFeature):
    def __init__(self):
        pass

    def set(self,method = "iou",score_sigma_low = 0.5,score_sigma_high = 0.3,
            iou_sigma = 0.5,iou_sigma2 = 0.3,iou_sigma_dup = 0.0,
            velocity_vector_frames = 15,max_track_lives = 10,
            min_track_length = 0,track_retention = 0):
        self._method = method
        self._score_sigma_low = score_sigma_low
        self._score_sigma_high = score_sigma_high
        self._iou_sigma = iou_sigma
        self._iou_sigma2 = iou_sigma2
        self._iou_sigma_dup = iou_sigma_dup 
        self._velocity_vector_frames = velocity_vector_frames 
        self._max_track_lives = max_track_lives
        self._min_track_length = min_track_length
        self._track_retention = track_retention

    def _feature_to_element(self):
        e = xml.Element("tracker")
        e.attrib["method"] = str(self._method)
        e.attrib["iou-sigma"] = str(self._iou_sigma)
        e.attrib["iou-sigma2"] = str(self._iou_sigma2)
        e.attrib["iou-sigma-dup"] = str(self._iou_sigma_dup)
        e.attrib["score-sigma-low"] = str(self._score_sigma_low)
        e.attrib["score-sigma-high"] = str(self._score_sigma_high)
        e.attrib["max-track-lives"] = str(self._max_track_lives)
        e.attrib["min-track-length"] = str(self._min_track_length)
        e.attrib["velocity-vector-frames"] = str(self._velocity_vector_frames)
        e.attrib["track-retention"] = str(self._track_retention)
        return(e);


class OutputFeature(WindowFeature):
    def __init__(self):
        pass

    def set(self,mode = "wide",prefix = "Object", tracks = 0,
            velocity_vector = False, newborn_tracks = False,
            scale_x = None, scale_y = None):
        self._mode = mode
        self._prefix = prefix
        self._tracks = tracks
        self._velocity_vector = velocity_vector
        self._newborn_tracks = newborn_tracks
        self._scale_x = scale_x
        self._scale_y = scale_y

    def _feature_to_element(self):
        e = xml.Element("output");
        e.attrib["mode"] = str(self._mode)
        e.attrib["velocity-vector"] = str(self._velocity_vector).lower()
        e.attrib["tracks"] = str(self._tracks)
        e.attrib["prefix"] = str(self._prefix)
        e.attrib["newborn-tracks"] = str(self._velocity_vector).lower()
        if self._scale_x != None:
            e.attrib["scale-x"] = str(self._scale_x)
        if self._scale_y != None:
            e.attrib["scale-y"] = str(self._scale_y)
        return(e)

class InputFeature(WindowFeature):
    def __init__(self):
        pass

    def set_rect(self,count = None,score = None, label = None, x = None,y = None,width = None,height = None):
        self._coordType = "rect"
        self._count = count
        self._score = score
        self._label = label
        self._x = x
        self._y = y
        self._width = width
        self._height = height

    def set_yolo(self,count = None,score = None, label = None, x = None,y = None,width = None,height = None):
        self._coordType = "yolo"
        self._count = count
        self._score = score
        self._label = label
        self._x = x
        self._y = y
        self._width = width
        self._height = height

    def set_coco(self,count = None,score = None, label = None, xMin = None,yMin = None,xMax = None,yMax = None):
        self._coordType = "yolo"
        self._count = count
        self._score = score
        self._label = label
        self._xMin = xMin
        self._yMin = yMin
        self._xMax = xMax
        self._yMax = yMax

    def _feature_to_element(self):
        e = xml.Element("input");
        e.attrib["count"] = str(self._count)
        e.attrib["score"] = str(self._score)
        e.attrib["label"] = str(self._label)
        e.attrib["coord-type"] = str(self._coordType)
        if self._coordType == "rect" or self._coordType == "yolo":
            e.attrib["x"] = str(self._x)
            e.attrib["y"] = str(self._y)
            e.attrib["width"] = str(self._width)
            e.attrib["height"] = str(self._height)
        elif self._coordType == "coco":
            e.attrib["x-min"] = str(self._xMin)
            e.attrib["y-min"] = str(self._yMin)
            e.attrib["x-max"] = str(self._xMax)
            e.attrib["y-max"] = str(self._yMax)
        return(e)

class ObjectTrackerWindow(TrackerFeature,OutputFeature,InputFeature,Window):
    '''
    Object Tracker window

    Parameters
    ----------
    name : string, optional
        The name of the window
    pubsub : bool, optional
        Publish/subscribe mode for the window. When the project-level value
        of pubsub is manual, true enables publishing and subscribing for
        the window and false disables it.
    description : string, optional
        Description of the window

    Attributes
    ----------
    tracker : Tracker
        The window tracker
    output : Output
        The window output
    input : Input
        The window input

    Returns
    -------
    :class:`ObjectTrackerWindow`

    '''

    window_type = 'object-tracker'

    def __init__(self, name=None, pubsub=None, description=None):
        Window.__init__(self, **get_args(locals()))

    def set_tracker(self,method = "iou",score_sigma_low = 0.5,score_sigma_high = 0.3,
                    iou_sigma = 0.5,iou_sigma2 = 0.3,iou_sigma_dup = 0.0,
                    velocity_vector_frames = 15,max_track_lives = 10,
                    min_track_length = 0,track_retention = 0):
        TrackerFeature.set(self,method,score_sigma_low,score_sigma_high,iou_sigma,iou_sigma2,iou_sigma_dup,velocity_vector_frames,max_track_lives,min_track_length,track_retention)
        '''
        Set the tracker

        Parameters
        ----------
        method : string
            Tracking method
        score_sigma_low : float
            Score low detection threshold (σl)
        score_sigma_high : float
            Score high detection threshold (σh)
        iou_sigma : float
            1st iou threshold (σiou)
        iou_sigma2 : float
            2nd iou threshold (σiou-2)
        iou_sigma_dup : float
            Iou duplicate threshold
        velocity_vector_frames : float
            Number of history frames used to calculate the velocity vector
        max_track_lives : float
            Life duration of tracks without detection
        min_track_length : float
            Minimum track length before allowing a missing frame (tmin)
        track_retention : float
            Number of frames the tracks keep the history of positions in memory
        '''

    def set_output(self,mode = "wide",prefix = "Object", tracks = 0,
                    velocity_vector = False, newborn_tracks = False,
                    scale_x = None, scale_y = None):
        '''
        Set the tracker output

        Parameters
        ----------
        mode : string
            wide: values --> rows, long: values --> fields
        prefix : string
            Prefix name of output fields.
        tracks : integer
            Number of tracks to output
        velocity_vector : boolean
            Do we output the velocity vector coordinates?
        newborn_tracks : boolean
            Whether we output the tracks with length < min-track-length
        scale_x : string
            Rescale factor for x dimension. It can be a double or a fractional value (ex '1920/416').
        scale_y : string
            Rescale factor for y dimension. It can be a double or a fractional value (ex '1920/416').
        '''
        OutputFeature.set(self,mode,prefix,tracks,velocity_vector,newborn_tracks,scale_x,scale_x)
 
    def set_input_rect(self,count = None,score = None,label = None,x = None,y = None,width = None,height = None):
        '''
        Set the tracker input

        Parameters
        ----------
        count : string
            Input object count field name
        score : string
            Input object score field name
        label : string
            Input object label field name
        x : string
            Input object x field name
        y : string
            Input object y field name
        width : string
            Input object width field name
        height : string
            Input object height field name
        '''
        InputFeature.set_rect(self,count,score,label,x,y,width,height)
 
    def set_input_yolo(self,count = None,score = None,label = None,x = None,y = None,width = None,height = None):
        '''
        Set the tracker input

        Parameters
        ----------
        count : string
            Input object count field name
        score : string
            Input object score field name
        label : string
            Input object label field name
        x : string
            Input object x field name
        y : string
            Input object y field name
        width : string
            Input object width field name
        height : string
            Input object height field name
        '''
        InputFeature.set_yolo(self,count,score,label,x,y,width,height)
 
    def set_input_coco(self,count = None,score = None,label = None,xMin = None,yMin = None,xMax = None,yMax = None):
        '''
        Set the tracker input

        Parameters
        ----------
        count : string
            Input object count field name
        score : string
            Input object score field name
        label : string
            Input object label field name
        xMin : string
            Input object x min field name
        yMin : string
            Input object y min field name
        xMax : string
            Input object x max field name
        yMax : string
            Input object y max field name
        '''
        InputFeature.set_yolo(self,count,score,label,xMin,yMin,xMax,yMax)
