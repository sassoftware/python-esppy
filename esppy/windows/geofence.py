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

import six
from .base import Window, attribute
from .utils import ensure_element, get_args, connectors_to_end
from ..utils import xml


class Geometry(object):
    '''
    Geofence geometry

    Parameters
    ----------
    desc_fieldname : string, optional
        Specifies the name of the geometry input window’s field that
        contains the polygon coordinates data.
    data_fieldname : string, optional
        Specifies the name of the geometry input window’s field that
        contains the geometry description.
    x_fieldname : string, optional
        Specifies the name of the geometry input window’s field that
        contains the location X or longitude coordinate of the
        circle's center.
    y_fieldname : string, optional
        Specifies the name of the geometry input window’s field that
        contains the location Y or latitude coordinate of the
        circle's center.
    radius_fieldname : string, optional
        Specifies the name of the geometry input window’s field that
        contains the circle radius distance.
    radius : int or float, optional
        Specifies the default circle's radius distance.
        Default: 1000
    data_separator : string, optional
        Specifies the coordinate delimiter character used in the
        geometry data field specified by the property data-fieldname.
        Default: <space>

    Returns
    -------
    :class:`Geometry`

    '''

    def __init__(self, desc_fieldname=None, data_fieldname=None, x_fieldname=None,
                 y_fieldname=None, radius_fieldname=None, radius=None,
                 data_separator=None):
        self.desc_fieldname = desc_fieldname
        self.data_fieldname = data_fieldname
        self.x_fieldname = x_fieldname
        self.y_fieldname = y_fieldname
        self.radius_fieldname = radius_fieldname
        self.radius = radius
        self.data_separator = data_separator

    def copy(self, deep=False):
        return type(self)(desc_fieldname=self.desc_fieldname,
                          data_fieldname=self.data_fieldname,
                          x_fieldname=self.x_fieldname,
                          y_fieldname=self.y_fieldname,
                          radius_fieldname=self.radius_fieldname,
                          radius=self.radius,
                          data_separator=self.data_separator)

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
        :class:`Geometry`

        '''
        data = ensure_element(data)
        out = cls()
        for key, value in six.iteritems(data.attrib):
            key = key.replace('-', '_')
            if hasattr(out, key):
                setattr(out, key, value)
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('geometry',
                            attrib=dict(desc_fieldname=self.desc_fieldname,
                                        data_fieldname=self.data_fieldname,
                                        x_fieldname=self.x_fieldname,
                                        y_fieldname=self.y_fieldname,
                                        radius_fieldname=self.radius_fieldname,
                                        radius=self.radius,
                                        data_separator=self.data_separator))

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


class Position(object):
    '''
    Geofence Position

    Parameters
    ----------
    x_fieldname : string, optional
       Specifies the name of the position input window’s field that
       contains the position X or longitude coordinate.
    y_fieldname : string, optional
       Specifies the name of the position input window’s field that
       contains the position Y or latitude coordinate.
    lookupdistance_fieldname : string, optional
       Specifies the name of the position input window’s field that
       contains the position lookup distance.
    lookupdistance : int or float, optional
       This distance is in units for Cartesian coordinates and in
       meters for geographic coordinates.
       Default: 0.

    Returns
    -------
    :class:`Position`

    '''

    def __init__(self, x_fieldname=None, y_fieldname=None,
                 lookupdistance_fieldname=None, lookupdistance=None):
        self.x_fieldname = x_fieldname
        self.y_fieldname = y_fieldname
        self.lookupdistance_fieldname = lookupdistance_fieldname
        self.lookupdistance = lookupdistance

    def copy(self, deep=False):
        return type(self)(x_fieldname=self.x_fieldname, y_fieldname=self.y_fieldname,
                          lookupdistance_fieldname=self.lookupdistance_fieldname,
                          lookupdistance=self.lookupdistance)

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
        :class:`Position`

        '''
        data = ensure_element(data)
        out = cls()
        for key, value in six.iteritems(data.attrib):
            key = key.replace('-', '_')
            if hasattr(out, key):
                setattr(out, key, value)
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('position',
                            attrib=dict(x_fieldname=self.x_fieldname,
                                        y_fieldname=self.y_fieldname,
                                        loopupdistance_fieldname=self.lookupdistance_fieldname,
                                        lookupdistance=self.lookupdistance))

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


class Output(object):
    '''
    Geofence Output

    Parameters
    ----------
    geotype_fieldname : string, optional
        Specifies the name of the output schema field that receives
        the geometry Type.
    geoid_fieldname : string, optional
        Specifies the name of the output schema field that receives
        the geometry ID.
    geodesc_fieldname : string, optional
        Specifies the name of the output schema field that receives
        the geometry description.
    geodistance_fieldname : string, optional
        Specifies the name of the output schema field that receives
        the distance between the position and the geometry (center
        point for circle geometries and centroid for polygons).
    eventnumber_fieldname : string, optional
        Specifies the name of the output schema additional key field
        that receives the generated event number.

    Returns
    -------
    :class:`Output`

    '''

    def __init__(self, geotype_fieldname=None, geoid_fieldname=None,
                 geodesc_fieldname=None, geodistance_fieldname=None,
                 eventnumber_fieldname=None):
        self.geotype_fieldname = geotype_fieldname
        self.geoid_fieldname = geoid_fieldname
        self.geodesc_fieldname = geodesc_fieldname
        self.geodistance_fieldname = geodistance_fieldname
        self.eventnumber_fieldname = eventnumber_fieldname

    def copy(self, deep=False):
        return type(self)(geotype_fieldname=self.geotype_fieldname,
                          geoid_fieldname=self.geoid_fieldname,
                          geodesc_fieldname=self.geodesc_fieldname,
                          geodistance_fieldname=self.geodistance_fieldname,
                          eventnumber_fieldname=self.eventnumber_fieldname)

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
        :class:`Output`

        '''
        data = ensure_element(data)
        out = cls()
        for key, value in six.iteritems(data.attrib):
            key = key.replace('-', '_')
            if hasattr(out, key):
                setattr(out, key, value)
        return out

    from_xml = from_element

    def to_element(self):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        return xml.new_elem('output',
                            attrib=dict(geotype_fieldname=self.geotype_fieldname,
                                        geoid_fieldname=self.geoid_fieldname,
                                        geodesc_fieldname=self.geodesc_fieldname,
                                        geodistance_fieldname=self.geodistance_fieldname,
                                        eventnumber_fieldname=self.eventnumber_fieldname))

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


class GeofenceWindow(Window):
    '''
    Geofence window

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
    pubsub_index_type : string, optional
        Publish/subscribe index type.  Valid values are the same as for the
        `index_type` parameter.
    polygon_compute_distance_to : string, optional
        Specifies whether to compute the distance from the position to the
        closest segment or to the centroid.
    proximity_analysis : bool, optional
        Specifies to return polygons that are within the distance define by
        the radius property value.
        Default: false.
    geofencing_algorithm : string, optional
        Specifies the geofencing algorithm to use for polygon geometries.
        Valid Values: crossing or winding
    coordinate_type : string, optional
        Coordinate type.
        Valid Values: cartesian or geographic
        Default: cartesian
    autosize_mesh : bool, optional
        Specifies whether to compute and set the mesh factor automatically
    meshfactor_x : int, optional
        Specifies the mesh factor for the X or longitude axis.
        Default: 0
    meshfactor_y : int, optional
        Specifies the mesh factor for the Y or latitude axis.
        Default: 0
    max_meshcells_per_geometry : int, optional
        Specifies the maximum allowed mesh cells created per geometries to
        avoid creating an oversized mesh that would generate useless
        intensive calculations.
        Default: 500
    output_multiple_results : bool, optional
        Specifies whether to write a single or multiple geometries,
        regardless of the number of matching geometries.
    output_sorted_results : bool, optional
        When set to true, specifies to sort the output result by increasing
        distance between the position and the geometry.
        Default: false
    log_invalid_geometry : bool, optional
        Specifies whether to log invalid geometries in the standard
        output log.
        Default: false

    Attributes
    ----------
    geometry : Geometry
        Geofence window geometry
    position : Position
        Geofence window position
    output : Output
        Geofence window output

    Returns
    -------
    :class:`GeofenceWindow`

    '''

    window_type = 'geofence'

    def __init__(self, name=None, pubsub=None, description=None,
                 output_insert_only=None, collapse_updates=None,
                 pulse_interval=None, exp_max_string=None, index_type=None,
                 pubsub_index_type=None, polygon_compute_distance_to=None,
                 proximity_analysis=None, geofencing_algorithm=None,
                 coordinate_type=None, autosize_mesh=None,
                 meshfactor_x=None, meshfactor_y=None,
                 max_meshcells_per_geometry=None,
                 output_multiple_results=None,
                 output_sorted_results=None, log_invalid_geometry=None):
        Window.__init__(self, **get_args(locals()))
        self.polygon_compute_distance_to = polygon_compute_distance_to
        self.proximity_analysis = proximity_analysis
        self.geofencing_algorithm = geofencing_algorithm
        self.coordinate_type = coordinate_type
        self.autosize_mesh = autosize_mesh
        self.meshfactor_x = meshfactor_x
        self.meshfactor_y = meshfactor_y
        self.max_meshcells_per_geometry = max_meshcells_per_geometry
        self.output_multiple_results = output_multiple_results
        self.output_sorted_results = output_sorted_results
        self.log_invalid_geometry = log_invalid_geometry
        self.geometry = Geometry()
        self.position = Position()
        self.output = Output()

    def copy(self, deep=False):
        out = Window.copy(self, deep=deep)
        out.polygon_compute_distance_to = self.polygon_compute_distance_to
        out.proximity_analysis = self.proximity_analysis
        out.geofencing_algorithm = self.geofencing_algorithm
        out.coordinate_type = self.coordinate_type
        out.autosize_mesh = self.autosize_mesh
        out.meshfactor_x = self.meshfactor_x
        out.meshfactor_y = self.meshfactor_y
        out.max_meshcells_per_geometry = self.max_meshcells_per_geometry
        out.output_multiple_results = self.output_multiple_results
        out.output_sorted_results = self.output_sorted_results
        out.log_invalid_geometry = self.log_invalid_geometry
        out.geometry = self.geometry.copy(deep=deep) 
        out.position = self.position.copy(deep=deep) 
        out.output = self.output.copy(deep=deep) 

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
        :class:`GeofenceWindow`

        '''
        out = super(GeofenceWindow, cls).from_element(data, session=session)

        for item in data.findall('./geofence'):
            for key, value in six.iteritems(item.attrib):
                key = key.replace('-', '_')
                if hasattr(out, key):
                    setattr(out, key, value)

        for item in data.findall('./geometry'):
            out.geometry = Geometry.from_element(item, session=session)

        for item in data.findall('./position'):
            out.position = Position.from_element(item, session=session)

        for item in data.findall('./output'):
            out.output = Output.from_element(item, session=session)

        return out

    from_xml = from_element

    def to_element(self, query=None):
        '''
        Convert object to Element

        Returns
        -------
        :class:`ElementTree.Element`

        '''
        out = Window.to_element(self, query=query)
        xml.add_elem(out, 'geofence',
                     attrib=dict(polygon_compute_distance_to=self.polygon_compute_distance_to,
                                 proximity_analysis=self.proximity_analysis,
                                 geofencing_algorithm=self.geofencing_algorithm,
                                 coordinate_type=self.coordinate_type,
                                 autosize_mesh=self.autosize_mesh,
                                 meshfactor_x=self.meshfactor_x,
                                 meshfactor_y=self.meshfactor_y,
                                 max_meshcells_per_geometry=self.max_meshcells_per_geometry,
                                 output_multiple_results=self.output_multiple_results,
                                 output_sorted_results=self.output_sorted_results,
                                 log_invalid_geometry=self.log_invalid_geometry))
        xml.add_elem(out, self.geometry.to_element())
        xml.add_elem(out, self.position.to_element())
        xml.add_elem(out, self.output.to_element())
        connectors_to_end(out)
        return out

    def set_geometry(self, desc_fieldname=None, data_fieldname=None,
                     x_fieldname=None, y_fieldname=None,
                     radius_fieldname=None, radius=None,
                     data_separator=None):
        '''
        Set geometry parameters

        Parameters
        ----------
        desc_fieldname : string, optional
            Specifies the name of the geometry input window’s field that
            contains the polygon coordinates data.
        data_fieldname : string, optional
            Specifies the name of the geometry input window’s field that
            contains the geometry description.
        x_fieldname : string, optional
            Specifies the name of the geometry input window’s field that
            contains the location X or longitude coordinate of the
            circle's center.
        y_fieldname : string, optional
            Specifies the name of the geometry input window’s field that
            contains the location Y or latitude coordinate of the
            circle's center.
        radius_fieldname : string, optional
            Specifies the name of the geometry input window’s field that
            contains the circle radius distance.
        radius : int or float, optional
            Specifies the default circle's radius distance.
            Default: 1000
        data_separator : string, optional
            Specifies the coordinate delimiter character used in the
            geometry data field specified by the property data-fieldname.
            Default: <space>

        '''
        for key, value in six.iteritems(get_args(locals())):
            if value is not None:
                setattr(self.geometry, key, value)

    def set_position(self, x_fieldname=None, y_fieldname=None,
                     lookupdistance_fieldname=None, lookupdistance=None):
        '''
        Set position parameters

        Parameters
        ----------
        x_fieldname : string, optional
           Specifies the name of the position input window’s field that
           contains the position X or longitude coordinate.
        y_fieldname : string, optional
           Specifies the name of the position input window’s field that
           contains the position Y or latitude coordinate.
        lookupdistance_fieldname : string, optional
           Specifies the name of the position input window’s field that
           contains the position lookup distance.
        lookupdistance : int or float, optional
           This distance is in units for Cartesian coordinates and in
           meters for geographic coordinates.
           Default: 0.

        '''
        for key, value in six.iteritems(get_args(locals())):
            if value is not None:
                setattr(self.position, key, value)

    def set_output(self, geotype_fieldname=None, geoid_fieldname=None,
                   geodesc_fieldname=None, geodistance_fieldname=None,
                   eventnumber_fieldname=None):
        '''
        Set output parameters

        Parameters
        ----------
        geotype_fieldname : string, optional
            Specifies the name of the output schema field that receives
            the geometry Type.
        geoid_fieldname : string, optional
            Specifies the name of the output schema field that receives
            the geometry ID.
        geodesc_fieldname : string, optional
            Specifies the name of the output schema field that receives
            the geometry description.
        geodistance_fieldname : string, optional
            Specifies the name of the output schema field that receives
            the distance between the position and the geometry (center
            point for circle geometries and centroid for polygons).
        eventnumber_fieldname : string, optional
            Specifies the name of the output schema additional key field
            that receives the generated event number.

        '''
        for key, value in six.iteritems(get_args(locals())):
            if value is not None:
                setattr(self.output, key, value)
