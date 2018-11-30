Filesystems
===========

All database clients are expected to be subclasses of `DatabaseClient`,
and so will share a common API and inherit a suite of IPython magics. Protocol
implementations are also free to add extra methods, which are documented in the
"Subclass Reference" section below.

Common API
----------

.. automodule:: omniduct.filesystems.base
    :members:
    :special-members: __init__
    :show-inheritance:
    :member-order: bysource

Subclass Reference
------------------

For comprehensive documentation on any particular subclass, please refer
to one of the below documents.

.. toctree::
    :glob:

    reference/*
