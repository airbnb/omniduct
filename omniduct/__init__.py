from omniduct.duct import Duct
from omniduct.registry import DuctRegistry
from . import protocols
from ._version import *
from .utils.session import is_directly_imported

if is_directly_imported():
    from .session import *
