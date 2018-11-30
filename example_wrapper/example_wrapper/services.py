import os
import yaml

from omniduct import config, logger, DuctRegistry


SERVICES_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'services.yml')

# Set up Omniduct configuration path
config._config_path = '~/.example_wrapper/config'

# Build registry from configuration
# Note: If you need to transform the configuration before importing, you can
# directly load the configuration into a dictionary using the `yaml` package,
# modify it before passing it in as the configuration below.
registry = DuctRegistry(config=SERVICES_CONFIG_PATH)
