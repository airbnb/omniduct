import os

from omniduct.registry import DuctRegistry

from .utils.config import config

__all__ = ['config', 'registry', '__author__', '__author_email__', '__version__']

OMNIDUCT_CONFIG = os.environ.get('OMNIDUCT_CONFIG', None) or os.path.expanduser('~/.omniduct/config')

config.register("ducts", "The ducts to register with the system.")

config._config_path = OMNIDUCT_CONFIG

registry = DuctRegistry(getattr(config, 'ducts', {}))
registry.populate_namespace(globals())
