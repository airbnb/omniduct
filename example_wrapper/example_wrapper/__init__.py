from collections import OrderedDict

from omniduct.utils.about import show_about as _show_about

from ._version import __author__, __author_email__, __version__  # noqa: F401
from .services import config, logger, registry  # noqa: F401


# Expose services via a proxy object
services = registry.get_proxy(by_kind=True)

# For convenience, add some services to the top-level module
registry.populate_namespace(
    namespace=globals(),
    names=['presto']
)


def about():
    """
    Show information about this package.
    """
    _show_about(
        name='Example Wrapper',
        version=__version__,
        maintainers=OrderedDict(zip(
            [a.strip() for a in __author__.split(',')],
            [a.strip() for a in __author_email__.split(',')]
        )),
        description="""
        A simple example wrapper around Omniduct for pre-configuring services
        in the context of an organisation.
        """
    )
