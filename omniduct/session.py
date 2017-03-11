import os

import IPython

from omniduct.registry import DuctRegistry

from .utils.config import config

# The default console width is too wide for most notebooks, leading to ugly logging message / progress bars. We set this
# to a more reasonable value for Jupyter Notebooks.
ip = IPython.get_ipython()
if ip and ip.__class__.__name__ == "ZMQInteractiveShell":
    os.environ['COLUMNS'] = "80"

__all__ = ['config', 'registry', '__author__', '__author_email__', '__version__']

OMNIDUCT_CONFIG = os.environ.get('OMNIDUCT_CONFIG', None) or os.path.expanduser('~/.omniduct/config')

config.register("ducts", "The ducts to register with the system.")

config._config_path = OMNIDUCT_CONFIG

registry = DuctRegistry(getattr(config, 'ducts', {}))
registry.populate_namespace(globals())
