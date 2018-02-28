Installation
============

If your company/organisation has provided a package that wraps around `omniduct`
to provide a library of services, then a direct installation of `omniduct` is
not required. Otherwise, you can install it using the standard Python package
manager: `pip`. If you use Python 3, you may need to change `pip` references
to `pip3`, depending on your system configuration.

.. code-block:: shell

    pip install omniduct[<comma separated list of protocols>]

For example, if you want access to Presto and HiveServer2, you can run:

.. code-block:: shell

    pip install omniduct[presto,hiveserver2]

Omitting the list of protocols (i.e. `pip install omniduct`) will mean that
the external dependencies required to interface with the protocols indicated in
:doc:`protocols` will not be automatically installed. Attempts to use these
protocols will throw an error with instructions as to which additional dependencies
you will need to install.

To install `omniduct` and all possible dependencies, you can install `omniduct`
using:

.. code-block:: shell

    pip install omniduct[all]

This is only recommended for casual use, as dragging in unneeded dependencies
could lead to complications with other packages on your machine (and is
otherwise just generally messy!).
