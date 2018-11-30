Databases
=========

.. role:: python(code)
   :language: python

.. role:: sql(code)
  :language: sql


All database clients are expected to be subclasses of `DatabaseClient`,
and so will share a common API and inherit a suite of IPython magics. Protocol
implementations are also free to add extra methods, which are documented in the
"Subclass Reference" section below.

Common API
----------

.. autoclass:: omniduct.databases.base.DatabaseClient
    :members:
    :special-members: __init__
    :show-inheritance:
    :member-order: bysource


IPython Magics
--------------

While it is possible in an IPython/Jupter notebook session to write code along
the lines of:

.. code-block:: python

    results = db_client.query("""
    SELECT *
    FROM table
    WHERE condition = 1
    """, format='pandas', ...)

manually encapsulating queries in quotes quickly becomes tiresome and cluttered.
We therefore expose most functionality as IPython magic functions. For example,
the above code could instad be rendered (assuming magic functions have been
registered under the name `db_client`):

.. code-block:: sql

    %%db_client results format='pandas' ...
    SELECT *
    FROM table
    WHERE condition = 1

Especially when combined with templating, this can greatly improve the
readability of your code.

In the following, all of the provided magic functions are listed along with
the equivalent programmatic code. Note that all arguments are passed in as
space-separated tokens after the magic's name. Position-arguments are always
interpreted as strings and keyword arguments are expected to be provided in the
form '<key>=<value>', where the <value> will be run as Python code and the
resulting value passed on to the underlying function/method as::
    db_client.method(..., key=eval('<value>'), ...)

Where present in the following, arguments in square brackets after the magic
name are the options specific to the magic function, and an ellipsis ('...')
indicates that any additional keyword arguments will be passed on to the
appropriate method.

Querying
^^^^^^^^

.. code-block:: sql

    %%<name> [variable=None show='head' transpose=False ...]
    SELECT *
    FROM table
    WHERE condition = 1

This magic is equivalent to calling :code:`db_client.query("<sql>", ...)`, with the
following magic-specific parameters offering additional flexibility:

* variable (str):
    The name of the local variable where the output should be
    stored (typically not referenced directly by name)
* show (str, int, None):
    What should be shown if variable is specified (if not
    the entire output is returned). Allowed values are ‘all’, ‘head’ (first 5
    rows), ‘none’, or an integer which specifies the number of rows to be shown.
* transpose (bool):
    If format is pandas, whether the shown results, as defined
    above, should be transposed. Data stored into variable is never transposed.

There is also a line-magic version if you are querying using an existing template:

.. code-block:: python

    results = %<name> variable='<template_name>' ...

which is equivalent to :code:`db_client.query_from_template('<template_name>', context=locals())`.
Note that one would typically pass this the template name as a position
argument, i.e. :code:`%<name> <template_name>`.

Executing
^^^^^^^^^

.. code-block:: sql

    %%<name>.execute [variable=None ...]
    INSERT INTO database.table (field1, field2) VALUES (1, 2);

This magic is equivalent to :code:`db_client.execute('<sql>', ...)`, with the
`variable` argument functioning as previously for the query magic.

As for the query magic, there is also a template version:

.. code-block: sql

    cursor = %<name>.execute variable='<template_name>' ...

Streaming
^^^^^^^^^

.. code-block:: sql

    %%<name>.stream [variable=None ...]
    SELECT *
    FROM table
    WHERE condition = 1

This magic is equivalent to :code:`db_client.stream('<sql>', ...)`, with the
`variable` argument functioning as previously for the query magic. Keep in mind
that the value returned from this method is a generator object.

As for the query magic, there is also a template version:

.. code-block: sql

    result_generator = %<name>.stream variable='<template_name>' ...

Templating
^^^^^^^^^^

To create a new template:

.. code-block:: sql

    %%<name>.template <template_name>
    SELECT *
    FROM table
    WHERE condition = 1

which is equivalent to :code:`db_client.add_template("<template_name>", "<sql>")`.

You can render a template in the cell body using current context (or specified
context):

.. code-block:: sql

    %%<name>.render [context=None, show=True]
    SELECT 1 FROM test

or if the template has already been created, you can render it directly by name:

.. code-block:: python

    %<name>.render [name=None, context=None, show=True]

In both cases, the `context` and `show` parameters respectively control the
context from which template variables are extracted and whether the rendered
template should be shown (printed to screen) or returned as a string.

Table properties
^^^^^^^^^^^^^^^^

:todo: Resolve what to keep and dump here.

.. code-block:: sql

    %%<name>.desc
    SELECT 1 FROM test

.. code-block:: sql

    %%<name>.head
    SELECT 1 FROM test

.. code-block:: sql

    %%<name>.props

Subclass Reference
------------------

For comprehensive documentation on any particular subclass, please refer
to one of the below documents.

.. toctree::
    :glob:

    reference/*
