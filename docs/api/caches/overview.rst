Caches
======

All remote clients are expected to be subclasses of `Cache`, and so will share a
common API. Protocol implementations are also free to add extra methods, which
are documented in the "Subclass Reference" section below.

Common API
----------

.. automodule:: omniduct.caches.base
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
