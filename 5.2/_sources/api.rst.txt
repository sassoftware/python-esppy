
.. Copyright SAS Institute

.. currentmodule:: esppy
.. _api:

*************
API Reference
*************

.. _api.functions:


ESP
---

The :class:`ESP` class is used to create the connection to the ESP server.
Once you have a connection to the server, you can query it for information
about projects (running and stopped) and the server itself.  You can
also create, load, start, stop, and delete projects.

Other objects in the server can also be introspected such as windows,
events, loggers, MAS modules, routers, and algorithms.

Finally, you can also save and reload server configurations, as well as 
shut the server down.


Constructor
~~~~~~~~~~~

.. currentmodule:: esppy.connection

.. autosummary::
   :toctree: generated/

   ESP

Server Methods
~~~~~~~~~~~~~~

These methods correspond to server configuration and shutting down.

.. autosummary::
   :toctree: generated/

   ESP.save
   ESP.reload
   ESP.shutdown

Project Methods
~~~~~~~~~~~~~~~

The project methods allow you to adminster projects.

.. autosummary::
   :toctree: generated/

   ESP.create_project
   ESP.load_project
   ESP.install_project
   ESP.get_project_stats
   ESP.get_project
   ESP.get_projects
   ESP.start_project
   ESP.start_projects
   ESP.stop_project
   ESP.stop_projects
   ESP.delete_project
   ESP.delete_projects
   ESP.get_running_project
   ESP.get_running_projects
   ESP.get_stopped_project
   ESP.get_stopped_projects
   ESP.validate_project

The :meth:`get_project_stats` method returns an object which subscribes
to the project statistics: :class:`ProjectStats`.  The :attr:`stats` attribute of that object
is a :class:`pandas.DataFrame` of the current statistics.


Window Methods
~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   ESP.get_window
   ESP.get_windows

Event Methods
~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   ESP.get_events
   ESP.get_pattern_events
   ESP.get_event_generator_state
   ESP.get_event_generators

Logging Methods
~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   ESP.get_logger
   ESP.get_loggers
   ESP.enable_server_log_capture
   ESP.disable_server_log_capture
   ESP.get_server_log
   ESP.get_server_log_state

MAS Module Methods
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   ESP.get_mas_modules

Router Methods
~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   ESP.create_router
   ESP.get_router
   ESP.get_routers
   ESP.get_router_stats

Event Generator Methods
~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   ESP.create_event_generator
   ESP.delete_event_generator
   ESP.delete_event_generators

Algorithm Methods
~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   ESP.get_algorithm 
   ESP.get_algorithms


Project Definitions
-------------------

The classes in this section allow you to programmatically create
projects.  Once you have a :class:`Project` object, you can load it
in the server and administer it.

Projects
~~~~~~~~

.. currentmodule:: esppy.project

.. autosummary::
   :toctree: generated/

   Project
   Project.get_stats
   Project.start
   Project.stop
   Project.save
   Project.restore
   Project.update
   Project.delete
   Project.copy
   Project.sync
   Project.validate
   Project.get_window
   Project.get_windows
   Project.get_mas_module
   Project.get_mas_modules
   Project.from_xml
   Project.to_xml
   Project.save_xml
   Project.to_graph

Project Construction Methods
............................

.. autosummary::
   :toctree: generated/

   Project.add_query
   Project.add_continuous_query
   Project.add_contquery
   Project.add_window
   Project.add_edge
   Project.add_connectors
   Project.start_connectors
   Project.create_mas_module
   Project.replace_mas_module

Dictionary Methods
..................

.. autosummary::
   :toctree: generated/

   Project.clear
   Project.get
   Project.items
   Project.keys
   Project.pop
   Project.popitem
   Project.setdefault
   Project.values


Continuous Queries
~~~~~~~~~~~~~~~~~~

.. currentmodule:: esppy.contquery

.. autosummary::
   :toctree: generated/

   ContinuousQuery
   ContinuousQuery.get_window
   ContinuousQuery.get_windows
   ContinuousQuery.add_window
   ContinuousQuery.add_windows
   ContinuousQuery.rename_window
   ContinuousQuery.delete_window
   ContinuousQuery.delete_windows
   ContinuousQuery.copy
   ContinuousQuery.from_xml
   ContinuousQuery.to_xml
   ContinuousQuery.save_xml
   ContinuousQuery.to_graph


Windows
~~~~~~~


Base Window Methods
...................

The base window class is an abstract class that defines methods used
by all window classes.  It should not be instantiated directly.
The common methods are described below.

.. currentmodule:: esppy.windows

Window Constructor
++++++++++++++++++

.. autosummary::
   :toctree: generated/

   Window

Monitoring
++++++++++

.. autosummary::
   :toctree: generated/

   Window.enable_tracing
   Window.disable_tracing

Creating Events
+++++++++++++++

.. autosummary::
   :toctree: generated/

   Window.create_event_generator
   Window.create_publisher
   Window.publish_events

Retrieving Events
+++++++++++++++++

.. autosummary::
   :toctree: generated/

   Window.subscribe
   Window.unsubscribe
   Window.create_subscriber
   Window.get_events
   Window.get_pattern_events

Transforming Events
+++++++++++++++++++

.. autosummary::
   :toctree: generated/

   Window.add_event_transformer
   Window.apply_transformers

Plotting
++++++++

.. autosummary::
   :toctree: generated/

   Window.streaming_bar
   Window.streaming_hbar
   Window.streaming_hist
   Window.streaming_line
   Window.streaming_area
   Window.streaming_scatter
   Window.streaming_bubble
   Window.streaming_scatter
   Window.streaming_donut
   Window.streaming_pie
   Window.streaming_images

Project Construction
++++++++++++++++++++

.. autosummary::
   :toctree: generated/

   Window.add_connector
   Window.add_target
   Window.add_targets
   Window.delete_target
   Window.delete_targets
   Window.set_finalized_callback
   Window.set_splitter_plugin
   Window.set_splitter_expr

Utilities
+++++++++

.. autosummary::
   :toctree: generated/

   Window.copy
   Window.from_xml
   Window.to_xml
   Window.save_xml
   Window.to_graph


Source Windows
..............

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   SourceWindow
   SourceWindow.set_retention


Calculation Windows
...................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   CalculateWindow
   CalculateWindow.set_parameters
   CalculateWindow.set_inputs
   CalculateWindow.set_outputs
   CalculateWindow.add_mas_window_map


Aggregation Windows
...................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   AggregateWindow
   AggregateWindow.add_field_expr
   AggregateWindow.add_field_expression
   AggregateWindow.add_field_exprs
   AggregateWindow.add_field_expressions
   AggregateWindow.add_field_plugin


Computation Windows
...................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   ComputeWindow
   ComputeWindow.add_field_expr
   ComputeWindow.add_field_exprs
   ComputeWindow.add_field_expression
   ComputeWindow.add_field_expressions
   ComputeWindow.add_field_plugin
   ComputeWindow.set_context_plugin
   ComputeWindow.set_expr_initializer
   ComputeWindow.set_expression_initializer


Copying Windows
...............

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   CopyWindow
   CopyWindow.set_retention


Counting Windows
................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   CounterWindow


Filtering Windows
.................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   FilterWindow
   FilterWindow.set_expr_initializer
   FilterWindow.set_expression_initializer
   FilterWindow.set_expression
   FilterWindow.set_plugin


Functional Windows
..................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   FunctionalWindow
   FunctionalWindow.set_function_context_expressions
   FunctionalWindow.set_function_context_properties
   FunctionalWindow.set_function_context_functions
   FunctionalWindow.add_regex_event_loop
   FunctionalWindow.add_xml_event_loop
   FunctionalWindow.add_json_event_loop
   FunctionalWindow.create_function_context

The :meth:`FunctionalWindow.create_function_context` method returns
a :class:`FunctionConext` instance with the following methods.

.. currentmodule:: esppy.windows.features

.. autosummary::
   :toctree: generated/

   FunctionContext
   FunctionContext.set_expressions
   FunctionContext.set_properties
   FunctionContext.set_functions


Geofence Windows
................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   GeofenceWindow
   GeofenceWindow.set_geometry
   GeofenceWindow.set_position
   GeofenceWindow.set_output


Joining Windows
...............

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   JoinWindow
   JoinWindow.set_expression_initializer
   JoinWindow.add_condition
   JoinWindow.add_field_expr
   JoinWindow.add_field_expression
   JoinWindow.add_expr
   JoinWindow.add_expression
   JoinWindow.add_field_selection
   JoinWindow.add_selection
   JoinWindow.add_field_plugin
   JoinWindow.set_expr_initializer


Model Reading Windows
.....................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   ModelReaderWindow


Model Supervisor Windows
........................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   ModelSupervisorWindow


Notification Windows
....................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   NotificationWindow
   NotificationWindow.set_smtp_settings
   NotificationWindow.add_email
   NotificationWindow.add_sms
   NotificationWindow.add_mms
   NotificationWindow.set_function_context_expressions
   NotificationWindow.set_function_context_properties
   NotificationWindow.set_function_context_functions


Pattern Windows
...............

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   PatternWindow
   PatternWindow.create_pattern

The ``patterns`` attribute of :class:`PatternWindow` contains
:class:`Pattern` objects which have the following methods.

.. currentmodule:: esppy.windows.features

.. autosummary::
   :toctree: generated/

   Pattern.add_event
   Pattern.set_logic
   Pattern.add_field_expression
   Pattern.add_field_selection
   Pattern.add_timefield


Procedural Windows
..................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   ProceduralWindow
   ProceduralWindow.add_cxx_plugins
   ProceduralWindow.set_cxx_plugin_context
   ProceduralWindow.add_cxx_plugin
   ProceduralWindow.add_ds_external


Scoring Windows
...............

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   ScoreWindow
   ScoreWindow.set_inputs
   ScoreWindow.set_outputs
   ScoreWindow.add_online_model
   ScoreWindow.add_offline_model
   ScoreWindow.import_schema_from_astore_output


Text Category Windows
.....................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   TextCategoryWindow


Text Context Windows
....................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   TextContextWindow


Text Sentiment Windows
......................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   TextSentimentWindow


Text Topic Windows
..................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   TextTopicWindow


Training Windows
................

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   TrainWindow
   TrainWindow.set_inputs
   TrainWindow.set_parameters


Union Windows
.............

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

   UnionWindow


Schemas
~~~~~~~

.. currentmodule:: esppy.schema

.. autosummary::
   :toctree: generated/

   Schema
   Schema.add_field
   Schema.from_xml
   Schema.to_xml
   Schema.from_schema_string
   Schema.from_string


Schema Fields
~~~~~~~~~~~~~

.. currentmodule:: esppy.schema

.. autosummary::
   :toctree: generated/

   SchemaField
   Schema.from_xml
   Schema.to_xml


Subscriber
----------

Subscriber objects are returned by the :meth:`Window.create_subscriber` method.
They are used to subscribe to event streams.

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

    Subscriber
    Subscriber.start
    Subscriber.stop
    Subscriber.close


Publisher
---------

Publisher objects are returned by the :meth:`Window.create_publisher` method.
They are used to publish events.

.. currentmodule:: esppy.windows

.. autosummary::
   :toctree: generated/

    Publisher
    Publisher.send
    Publisher.close


Streaming Charts
----------------

Streaming charts are a wrapper around Chart.js charts which include features
for making animated figures in a Jupyter notebook easier.

.. currentmodule:: esppy.plotting

.. autosummary::
   :toctree: generated/

    StreamingChart
    StreamingChart.area
    StreamingChart.bar
    StreamingChart.hbar
    StreamingChart.bubble
    StreamingChart.line
    StreamingChart.scatter
    StreamingChart.pie
    StreamingChart.donut
    StreamingChart.doughnut


Streaming Images
----------------

Streaming images allow you to view a series of images in a Jupyter notebook cell.

.. autosummary::
   :toctree: generated/

    StreamingImages

Chart Layouts
-------------

Chart layouts allow you to create complex layouts of multiple streaming chart
and image components.  These layouts will display in a single output cell in
a Jupyter notebook.  All of the components in the layout are controlled from
a single set of transport buttons.

.. autosummary::
   :toctree: generated/

   ChartLayout


Event Generators
----------------

.. currentmodule:: esppy.evtgen

.. autosummary::
   :toctree: generated/

   EventGenerator
   EventGenerator.add_initializers
   EventGenerator.add_fields
   EventGenerator.add_map_resources
   EventGenerator.add_list_resources
   EventGenerator.add_set_resources
   EventGenerator.add_map_url_resources
   EventGenerator.add_list_url_resources
   EventGenerator.add_set_url_resources
   EventGenerator.start
   EventGenerator.stop
   EventGenerator.initialize
   EventGenerator.delete
   EventGenerator.save
   EventGenerator.from_xml
   EventGenerator.to_xml


Loggers
-------

.. currentmodule:: esppy.logger

.. autosummary::
   :toctree: generated/

   Logger
   Logger.set_level


MAS Modules
-----------

.. currentmodule:: esppy.mas

.. autosummary::
   :toctree: generated/

   MASModule
   MASModule.save
   MASModule.from_xml
   MASModule.to_xml


Routers
-------

.. currentmodule:: esppy.router

Router
~~~~~~

.. autosummary::
   :toctree: generated/

   Router
   Router.add_engine
   Router.add_publish_destination
   Router.add_writer_destination
   Router.add_route
   Router.save
   Router.delete
   Router.initialize_destination
   Router.from_xml
   Router.to_xml

Engine
~~~~~~

.. note:: :class:`Engine` objects are typically instantiated using
          the :meth:`Router.add_engine` method.

.. autosummary::
   :toctree: generated/

   Engine
   Engine.to_element
   Engine.to_xml

PublishDestination
~~~~~~~~~~~~~~~~~~

.. note:: :class:`PublishDestination` objects are typically instantiated using
          the :meth:`Router.add_publish_destination` method.

.. autosummary::
   :toctree: generated/

   PublishDestination
   PublishDestination.initialize
   PublishDestination.to_element
   PublishDestination.to_xml

WriterDestination
~~~~~~~~~~~~~~~~~

.. note:: :class:`WriterDestination` objects are typically instantiated using
          the :meth:`Router.add_writer_destination` method.

.. autosummary::
   :toctree: generated/

   WriterDestination
   WriterDestination.initialize
   WriterDestination.to_element
   WriterDestination.to_xml

Route
~~~~~

.. note:: :class:`Route` objects are typically instantiated using
          the :meth:`Router.add_route` method.

.. autosummary::
   :toctree: generated/

   Route
   Route.to_element
   Route.to_xml


Connectors
----------

.. currentmodule:: esppy.connectors

The :class:`Connector` class is the base class for all connectors.

.. autosummary::
   :toctree: generated/

   Connector
   Connector.set_properties
   Connector.to_xml

The following connectors are concrete implementations of the 
connectors available to ESP.

.. autosummary::
   :toctree: generated/

    BacnetPublisher
    AdapterPublisher
    AdapterSubscriber
    DatabasePublisher
    DatabaseSubscriber
    FilePublisher
    FileSubscriber
    SocketPublisher
    SocketSubscriber
    KafkaSubscriber
    KafkaPublisher
    MQTTSubscriber
    MQTTPublisher
    ModbusSubscriber
    ModbusPublisher
    NuregoSubscriber
    OPCUASubscriber
    OPCUAPublisher
    PISubscriber
    PIPublisher
    ProjectPublisher
    PylonPublisher
    RabbitMQSubscriber
    RabbitMQPublisher
    SMTPSubscriber
    SnifferPublisher
    SolaceSubscriber
    SolacePublisher
    TeradataSubscriber
    TeradataListenerSubscriber
    TervelaSubscriber
    TervelaPublisher
    TibcoSubscriber
    TibcoPublisher
    TimerPublisher
    URLPublisher
    UVCPublisher
    WebSocketPublisher
    WebSphereMQSubscriber
    WebSphereMQPublisher


Algorithms
----------

.. currentmodule:: esppy.algorithm

.. autosummary::
   :toctree: generated/

   Algorithm
   Algorithm.from_xml


Project Statistics
------------------

The :class:`ProjectStats` class subscribes to the statistics stream on
the server.  It acts like a :class:`pandas.DataFrame` and contains the
current project statistics.

.. currentmodule:: esppy.connection

.. autosummary::
   :toctree: generated/

   ProjectStats
   ProjectStats.start
   ProjectStats.stop
   ProjectStats.close


Configuration Options
---------------------

The ``options`` object at the top-level of the package allows you to 
get and set options as attributes.  For example, to set the 
``display.image_scale`` option to 0.5, you would do the following:

.. ipython:: python
   :suppress:

   import esppy

.. ipython:: python

    esppy.options.display.image_scale = 0.5

This is equivalent to:

.. ipython:: python

    esppy.set_option('display.image_scale', 0.5)

You can get help for an option using the ``describe_option`` function
or IPython's help system:

.. ipython:: python

    esppy.describe_option('display.image_scale')

To get help for all options, execute ``describe_option`` without any arguments.

.. currentmodule:: esppy

.. autosummary::
   :toctree: generated/

   get_option
   set_option
   describe_option
   reset_option
   option_context
