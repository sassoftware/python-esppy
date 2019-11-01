# SAS Event Stream Processing Python Interface

The ESPPy package enables you to create
[SAS Event Stream Processing (ESP)](https://www.sas.com/en_us/software/event-stream-processing.html)
models programmatically in Python. Using ESPPy, you can connect to 
an ESP server and interact with projects and their components as 
Python objects. These objects include projects, continuous queries, 
windows, events, loggers, SAS Micro Analytic Service modules, 
routers, and analytical algorithms.

ESPPy has full integration with [Jupyter](https://jupyter.org/) notebooks including visualizing 
diagrams of your ESP projects, and support for streaming charts and 
images. This allows you to easily explore and prototype your ESP 
projects in a familiar notebook interface.

## Installation

To install ESPPy, you can use either `pip` or `conda`.  This will install
ESPPy as well as the Python package dependencies.

```
pip install sas-esppy
```

or, if you are using an Anaconda distribution:

```
conda install -c sas-institute sas-esppy
```

### Additional Requirements

In addition to the Python package dependencies, you will also need the 
`graphviz` command-line tools to fully take advantage of ESPPy.  These can
be downloaded from http://www.graphviz.org/download/.

### Performance Enhancement

Also, ESPPy uses the `ws4py` websocket Python package.  In some cases
you can improve performance greatly by installing the `wsaccel` package.
This may not be available on all platforms though, and is left up to 
the user to install.

## The Basics

Importing the package is done just as it is with any other Python package.

```
>>> import esppy
```

To connect to an ESP server, you use the `ESP` class.  In most cases, the only
information that is needed is the hostname and port.

```
>>> esp = esppy.ESP('http://myesp.com:8777')
```

### Getting Information about the Server

Now that we have a connection to the server, we can get information about the
server and projects.

```
>>> esp.server_info
{'analytics-license': True,
 'engine': 'esp',
 'http-admin': 8777,
 'pubsub': 8778,
 'version': '5.2'}

# Currently no projects are loaded
>>> esp.get_projects()
{}
```

### Loading a Project

Loading a project is done with the `load_project` method.

```
>>> esp.load_project('project.xml')

>>> esp.get_projects()
{'project': Project(name='project')}
```

Continous queries and windows within projects can be accessed using
the `queries` and `windows` attributes of the `Project` and
`ContinuousQuery` objects, respectively.

```
>>> proj = esp.get_project('project')
>>> proj.queries
{'contquery': ContinuousQuery(name='contquery', project='project')}

>>> proj.queries['contquery'].windows
{'w_data': CopyWindow(name='w_data', continuous_query='contquery', project='project'),
 'w_request': SourceWindow(name='w_request', continuous_query='contquery', project='project'),
 'w_calculate': CalculateWindow(name='w_calculate', continuous_query='contquery', project='project')}

>>> dataw = proj.queries['contquery'].windows['w_data']
```

You can even drop the `queries` and `windows` attribute name as a shortcut.
projects and continuous queries act like dictionaries of those components.

```
>>> dataw = proj['contquery']['w_data']
```

### Publishing Event Data

To publish events to a window, you simply use the `publish_events` method.
It will accept a file name, file-like object, DataFrame, or a string of
CSV, XML, or JSON data.

```
>>> dataw.publish_events('data.csv')
```

### Monitoring Events

You can subscribe to the events of any window in a project.  By default,
all event data will be cached in the local window object.

```
>>> dataw.subscribe()
>>> dataw
       time        x        y        z
id                                    
6   0.15979 -2.30180  0.23155  10.6510
7   0.18982 -1.41650  1.18500  11.0730
8   0.22040 -0.27241  2.22010  11.9860
9   0.24976 -0.61292  2.22010  11.9860
10  0.27972  1.33480  4.24950  11.4140
11  0.31802  3.44590  7.58650  12.5990
```

You can limit the number of cached events using the `limit`
parameter.  For example, to only keep the last 20 events, you would do
the following.

```
>>> dataw.subscribe(limit=20)
```

You can also limit the amount of time that events are collected using
the `horizon` parameter.  It will take a `datetime`, `date`, `time`,
or `timedelta` object.

```
>>> dataw.subscribe(horizon=datetime.timedelta(hours=1))
```

You can also perform any DataFrame operation on your ESP windows.

```
>>> dataw.info()
<class 'pandas.core.frame.DataFrame'>
Int64Index: 2108 entries, 6 to 2113
Data columns (total 4 columns):
time    2108 non-null float64
x       2108 non-null float64
y       2108 non-null float64
z       2108 non-null float64
dtypes: float64(4)
memory usage: 82.3 KB

>>> dataw.describe()
            time          x          y          z
count  20.000000  20.000000  20.000000  20.000000
mean   69.655050  -4.365320   8.589630  -1.675292
std     0.177469   1.832482   2.688911   2.108300
min    69.370000  -7.436700   4.862500  -5.175700
25%    69.512500  -5.911250   7.007675  -3.061150
50%    69.655000  -4.099700   7.722700  -1.702500
75%    69.797500  -2.945400   9.132350  -0.766110
max    69.940000  -1.566300  14.601000   3.214400
```

### Using ESPPY Visualizations with Jupyter LAB

NOTE: These instructions assume you have Anaconda installed.

The steps to use the new ESPPY 6.2 jupyterlab visualizations are:

1. Create new Anaconda environment. This can be called anything you want, but for this demonstration the environment will be called esp
```
    $ conda create -n esp python=3.7
```
2. Activate the new environment, i.e. make it your current environment
```
$ conda activate esp
```
3. Install the following packages:
```
$ pip install jupyter
$ pip install jupyterlab
$ pip install matplotlib
$ pip install ipympl
$ pip install pandas
$ pip install requests
$ pip install image
$ pip install ws4py
$ pip install plotly
$ pip install ipyleaflet
$ pip install graphviz
```
4. Install the following Jupyterlab extensions:
```
$ jupyter labextension install @jupyter-widgets/jupyterlab-manager
$ jupyter labextension install plotlywidget
$ jupyter labextension install jupyter-leaflet
```

5. Install the following packages (WINDOWS ONLY):
```
$ conda install -c conda-forge python-graphviz
```

6. Create a working directory and change to it
```
$ cd $HOME
$ mkdir esppy
$ cd esppy
```

7. Install ESPPY
```
pip install sas-esppy==6.2
```

8. Create a notebooks directory to store your notebooks
```
$ mkdir notebooks
```

9. Start the Jupyterlab server (Pick an available port of your choosing, this example uses 35000)
```
$ jupyter lab --port 35000
```

Once these steps are complete, you should be able to use the latest ESP graphics in your Jupyter notebooks.

### Documentation

The full API documentation of ESPPy is available at 
https://sassoftware.github.io/python-esppy/.
