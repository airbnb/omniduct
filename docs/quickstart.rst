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


.. toctree::
    :hidden:

    examples/presto
    examples/mysql
