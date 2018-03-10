=================================
Omniduct |release| documentation
=================================

.. toctree::
    :hidden:

    protocols
    installation
    quickstart
    deployment
    api/overview
    extensions
    contributions

Omniduct is an extensible Python library that provides uniform interfaces to a
wide variety of (potentially) remote data providers such as databases,
filesystems, and REST services. Its primary objective is to simplify the process
of collecting and analysing data in a heterogeneous data environment, and is
suitable for deployment in interactive and production environments. To that
end, it offers the following features:

- A generic plugin-based programmatic API to access data in a consistent manner
  across different services (see :doc:`protocols`).
- A framework for lazily connecting to data sources and maintaining these
  connections during the entire lifetime of the relevant Python session.
- Automatic port forwarding of remote services over SSH where connections cannot
  be made directly.
- Convenient IPython magic functions for interfacing with data providers from
  within IPython and Jupyter Notebook sessions.
- Utility classes and methods to assist in maintaining registries of useful
  services.

Omniduct has been designed such that it is convenient to use directly (each
user can configure their own service definitions) or via another package (which
can create a library of pre-defined services, such as for a company). For more
information on how to deploy `omniduct` refer to :doc:`deployment`.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
