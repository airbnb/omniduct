Core Classes
============

All protocol implementations are subclasses (directly or indirectly) of `Duct`.
This base class manages the basic life-cycle, connection management and protocol
registration. When a subclass of `Duct` is loaded into memory, and has at least
one protocol name in the `PROTOCOLS` attribute, then `Duct` registers that class
into its subclass registry. This class can then be conveniently accessed by:
`Duct.for_protocol('<protocol_name>')`. This empowers the accompanying registry
tooling bundled with omniduct, as documented in :doc:`registry`.

Protocol implementations may also (directly or indirectly) be subclasses of
`MagicsProvider`, which provides a common API to registry IPython magics into
the user's session. If implemented, the accompanying registry tooling can
automatically register these magics, as documented in :doc:`registry`.

Duct
----

.. autoclass:: omniduct.duct.Duct
    :members:
    :special-members: __init__
    :private-members:
    :show-inheritance:
    :member-order: bysource

MagicsProvider
--------------

.. autoclass:: omniduct.utils.magics.MagicsProvider
    :members:
    :private-members:
    :show-inheritance:
    :member-order: bysource
