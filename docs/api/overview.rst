API & IPython Magics
====================

.. toctree::
    :hidden:

    core
    databases/overview
    filesystems/overview
    remotes/overview
    caches/overview
    registry

Omniduct's API has been designed to ensure that ducts which provide the same
type of service (i.e. database querying, filesystem grokking, etc) also provide
a programmatically similar API. As such, all protocol implementations are
subclasses of a generic abstract class `Duct` via a protocol type-specific
subclass (such as `DatabaseClient` for database protocols). This ensures that
the core API is consistent between all instances of the same protocol type.
These type-specific classes may also derive from
`omniduct.utils.magics.MagicsProvider`, and provide IPython magic functions to
provide convenient access to these protocols in IPython sessions. Protocol
implementations can also have protocol-specific additions to the core API.

The `Duct` class provides the scaffolding for connection management and other
"magic" such as the automatic creation of a registry of the protocols handled by
subclasses. This class is described in more detail in :doc:`core`, along with
the `MagicsProvider` class.

The protocol-specific subclasses of `Duct` that provide the shared APIs
(including any IPython magics) for each protocol type are detailed in dedicated
pages; i.e. :doc:`databases/overview`, :doc:`filesystems/overview`, :doc:`remotes/overview`, and
:doc:`caches/overview`.

Lastly, utility classes and methods are provided to help manage registries of
connections to various services. These are documented in :doc:`registry`.

:Note: Omniduct does not guarantee a stable API between major versions.
    However, we do commit to ensuring that version `x.y.z` of `omniduct` is
    API forward-compatible with all future minor versions `x.y.*`. While there
    is no guarantee of APIs remaining fixed between major versions, we expect
    that in practice these breaking API changes will be small, and in all cases
    will be documented in the release notes. As such, if you are using Omniduct
    in a production environment, we recommend installing using a static pinned
    version or something like `omniduct>=1.2.3<1.3`, where 1.2.3 is the version
    found to work well in your environment.
