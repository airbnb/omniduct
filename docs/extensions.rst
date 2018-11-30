Extensions and Plug-ins
=======================

Extending Omniduct to support additional services is relatively straightforward,
requiring you only to subclass `Duct` or one of the protocol specific common
API subclasses (a template for each of these is provided as a `stub.py` file
in the appropriate subpackage, e.g. https://github.com/airbnb/omniduct/blob/master/omniduct/databases/stub.py).

As soon as your subclass is in memory, it will integrate automatically with the
rest of the Omniduct ecosystem, and be instantiatable by protocol name through
the `DuctRegistry` or `Duct.for_protocol()` systems.

If you would like to contribute this extension into the upstream Omniduct
library, we welcome your contribution. This would entail simply adding a module
containing your subclass to the appropriate Omniduct subpackage, and then
(if it is stable and ready for broad usage) importing that subpackage from
`omniduct.protocols`. Once your module is merged into the master branch of
Omniduct, maintainance will fall to the core Omniduct maintainers, though you
are of course welcome to continue submitting patches to improve it or any
other aspect of Omniduct.

If you need further assistance, please do not hesitate to open an issue on our
issue tracker: https://github.com/airbnb/omniduct/issues .
