# flake8: noqa

from omniduct.duct import Duct
from omniduct.registry import DuctRegistry
from omniduct.utils.config import config
from omniduct.utils.debug import logger

from . import protocols
from ._version import __author__, __author_email__, __version__, __logo__, __docs_url__


def about():
    from collections import OrderedDict
    from .utils.about import show_about

    return show_about(
        "Omniduct",
        version=__version__,
        logo=__logo__,
        maintainers=OrderedDict(zip(
            [a.strip() for a in __author__.split(',')],
            [a.strip() for a in __author_email__.split(',')]
        )),
        attributes={
            'Documentation': __docs_url__,
        },
        description="""
        Omniduct provides uniform interfaces for connecting to and extracting data
        from a wide variety of (potentially remote) data stores (including HDFS,
        Hive, Presto, MySQL, etc).
        """,
        endorse_omniduct=False
    )
