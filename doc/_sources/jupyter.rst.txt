
.. Copyright SAS Institute

Jupyter Notebook Integration
============================

Some of the objects in the ESP interface have support for integrating with
Jupyter notebooks.  One example is the diagrams of :class:`Project`,
:class:`ContinuousQuery`, and :class:`Window` objects.  When you evaluate
one of these objects as the last line of a code cell in Jupyter, you'll get
a digram similar to the following:

.. image:: _images/walk_proj.svg

These diagrams to require the ``graphviz`` program to be installed on your
machine as well as the ``graphviz`` Python module though.  While the ``graphviz``
Python module will install automatically when you install ``esppy``, you need
to install the ``graphviz`` command-line utilites from the following site.

    http://www.graphviz.org/
