Quickstart
==========

.. role:: python(code)
   :language: python

.. role:: sql(code)
  :language: sql

`omniduct` is designed to be intuitive and uniform in its APIs. As such, insofar
as possible, all `Duct` subclasses have a reasonable default configuration,
making it possible to quickly create working connections to remote services.
Depending on the complexity of your service configuration, it may or may not
make sense to use `omniduct`'s registry utilities, and so this quickstart
will show you how to directly create `Duct` instances, as well as how to work
with a `Duct` registry. Though we only use `PrestoClient` explicitly in the
following, since all `Duct` instances have the same basic API, the same
methodology will work with all `Duct` subclasses.

If you are looking deploy `omniduct` into production or as part of a
company specific package, or want to share your service configuration with
others, you will likely also be interested in :doc:`deployment`.

Task 1: Create a Presto client that connects direct to the database service
---------------------------------------------------------------------------

*Method 1: Via PrestoClient class*

.. code-block:: python

    >>> from omniduct.databases.presto import PrestoClient

    >>> pc = PrestoClient(host="<host>", port=8080)

    >>> pc.query("SELECT 42")
    PrestoClient: Query: Complete after 0.14 sec on 2017-10-13.
        _col0
     0     42

    >>> pc.register_magics('presto_local')

    # The following assumes that you are using an IPython/Jupyter console
    >>> %%presto_local
    ... {# magics are created and queries rendered using Jinja2 templating #}
    ... SELECT {{ 4 * 10 + 2 }}
    ...
    presto_local: Query: Complete after 1.20 sec on 2017-10-13.
       _col0
    0     42

*Method 2: Via Duct subclass registry*

.. code-block:: python

    >>> from omniduct import Duct

    >>> pc = Duct.for_protocol('presto')(host='<host>', port=8080)

    >>> pc.query("SELECT 42")
    # ... And all of the rest from above.


*Method 3: Via DuctRegistry*

.. code-block:: python

    >>> from omniduct import DuctRegistry

    >>> duct_registry = DuctRegistry()

    >>> pc = duct_registry.new(name='presto_local', protocol='presto',
    ...                        host='localhost', port=8080, register_magics=True)

    >>> # Or: pc = duct_registry['presto_local']

    >>> # Or: pc = duct_registry.get_proxy(by_kind=True).databases.presto_local

    >>> pc.query("SELECT 42")
    presto_local: Query: Complete after 0.14 sec on 2017-10-13.
       _col0
    0     42

    # The following assumes that you are using an IPython/Jupyter console
    >>> %%presto_local
    ... {# magics are created and queries rendered using Jinja2 templating #}
    ... SELECT {{ 4 * 10 + 2 }}
    ...
    presto_local: Query: Complete after 1.20 sec on 2017-10-13.
       _col0
    0     42

Task 2: Create a Presto client that connects via ssh to a remote server
-----------------------------------------------------------------------

*Method 1: Directly passing `RemoteClient` instance to PrestoClient constructor*

.. code-block:: python

    >>> from omniduct import Duct

    >>> remote = Duct.for_protocol('ssh')(host='<remote_host>', port=22)

    >>> pc = Duct.for_protocol('presto')(host='<host_relative_to_remote>',
                                         port=8080, remote=remote)

    >>> pc.query("SELECT 42")  # Query sent to port-forwarded remote service
    PrestoClient: Query: Complete after 0.14 sec on 2017-10-13.
        _col0
     0     42

*Method 2: Passing name of `RemoteClient` instance via Registry*

.. code-block:: python

    >>> from omniduct import DuctRegistry

    >>> duct_registry = DuctRegistry()

    >>> duct_registry.new('my_server', protocol='ssh', host='<remote_host>', port=22)
    <omniduct.remotes.ssh.SSHClient at 0x110bab550>

    >>> duct_registry.new('presto_remote', protocol='presto', remote='my_server',
                          host='<host_relative_to_remote>', port=8080)
    <omniduct.databases.presto.PrestoClient at 0x110c04a58>

    # Query sent to port-forwarded remote service

    >>> %%presto_remote
    ... SELECT 42
    ...
    presto_remote: Query: Connecting: Connected to localhost:8080 on <remote_host>.
    presto_remote: Query: Complete after 7.30 sec on 2017-10-13.
       _col0
    0     42

Task 3: Persist service configuration for use in multiple sessions
------------------------------------------------------------------

*Method 1: Manually import configuration into `DuctRegistry`*

.. code-block:: python

    >>> from omniduct import DuctRegistry

    >>> duct_registry = DuctRegistry()

    # Specify a YAML configuration verbatim (or the filename of a yaml configuration)
    # In this case we create the configuration for the previous task.
    >>> duct_registry.register_from_config("""
    ... remotes:
    ...     my_server:
    ...         protocol: ssh
    ...         host: <remote_host>
    ... databases:
    ...     presto_local:
    ...         protocol: presto
    ...         host: <host_relative_to_remote>
    ...         port: 8080
    ...         remote: my_server
    ... """)

    >>> %%presto_local
    ... SELECT 42
    ...
    # And so on.


*Method 2: Save configuration to `~/.omniduct/config`, and autoload*

Assuming that the above YAML file has been saved to `~/.omniduct/config`,
or to a file located at the location pointed to by the `OMNIDUCT_CONFIG`
environment variable, you can directly restore your configuration by importing
from `omniduct.session`.

.. code-block:: python

    >>> from omniduct.session import *

    >>> presto_local
    <omniduct.databases.presto.PrestoClient at 0x110c04a58>

    >>> %%presto_local
    ... SELECT 42

    # And so on.
