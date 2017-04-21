import inspect
import io
import json
import logging
import os
import sys

import six


def ensure_path_exists(path):
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        os.makedirs(path)
    return path


logger = logging.getLogger(__name__)


class ConfigurationRegistry(object):

    def __init__(self):
        self._register = {}

    def register(self, key, description=None, default=None, onchange=None, onload=None,
                 type=None, host=None):
        '''
        Register a configuration key that can be set by the user. As noted in the
        class level documentation, these keys should not lead to changes in the
        output of omniduct functions. The same code should generate the same results
        independent of this configuration.

        The arguments to this method are:
         - key : A string used to identify the configuration option.
         - description* : A string description of the configuration key.
         - default* : A default value for the key.
         - onchange* : A function to call when the parameter changes (should have
            a signature accepting one variable).
         - onload* : A function of no arguments to call to initialize the value
            of this configuration setting
         - type : Values set will be of `isinstance(val, type)`

        * If not specified, these fields default to None.
        '''
        if key in dir(self):
            raise KeyError("Key `{0}` cannot be registered as it conflicts with a method of OmniductConfiguration.".format(key))
        if key in self._register:
            logger.warn("Overwriting existing key `{0}`, previously registered by {1}".format(key, self._register[key]['host']))

        try:
            caller_frame = inspect.current_frame().f_back
            host = inspect.getmodule(caller_frame).__name__
        except:
            host = 'unknown'

        if default is not None and type is not None:
            assert isinstance(default, type)
        self._register[key] = {
            'description': description,
            'host': host,
            'default': default,
            'onchange': onchange,
            'onload': onload,
            'type': type,
        }

    def show(self):
        '''
        Pretty print the configuration options available to be set, as well as
        their current values, descriptions and the module from which they were
        registered.
        '''
        for key in sorted(self._register.keys()):
            desc = self._register[key].get('description')
            if desc is None:
                desc = 'No description'
            print('{0} with default = {1}'.format(key, self._register[key]['default']))
            print('\t{0}'.format(desc))
            print('\t({0})'.format(self._register[key]['host']))


class Configuration(ConfigurationRegistry):
    """
    Configuration is a hub for storing runtime configuration settings, as
    well as persisting them to disk. Ideally, it should store only configuration
    options that allow omniduct to run optimally on the system (such as preferred
    hostnames, servers and usernames). In particular, the same code run on different
    systems should output the same results (independent of configuration); so,
    for example, this configuration hub should *not* be used for things like
    default plot styling, etc.

    Retrieving a configuration option looks like:
    >>> config.logging_level
    20

    Setting a configuration option looks like:
    >>> config.logging_level = 10

    Reviewing all available options and set values looks like:
    >>> config.show()
    """

    def __init__(self, *registries, **kwargs):
        ConfigurationRegistry.__init__(self)

        for registry in registries:
            for key, props in registry.items():
                self.register(key, **props)

        self._config = {}
        self.__config_path = kwargs.pop('config_path', None)

    def __dir__(self):
        return sorted(self._register.keys())

    @property
    def _config_path(self):
        return self.__config_path

    @_config_path.setter
    def _config_path(self, path):
        self.__config_path = path

        if path is not None and os.path.exists(self.__config_path):
            # Restore configuration
            try:
                self.load(force=True)
            except:
                raise RuntimeError(
                    "Configuration file at {0} cannot be loaded. Perhaps try deleting it.".format(self.__config_path))

    def all(self):
        '''
        Return a dictionary containing all configuration keys. Note that this is
        the actual dictionary storing the configuration options, so modifying
        this dictionary will modify the configuration options *without* running
        the standard checks.
        '''
        return self._config

    def show(self):
        '''
        Pretty print the configuration options available to be set, as well as
        their current values, descriptions and the module from which they were
        registered.
        '''
        for key in sorted(self._register.keys()):
            desc = self._register[key].get('description')
            if desc is None:
                desc = 'No description'
            val = str(self._config.get(key, '<Not Set>'))
            print('{0} = {1} (default = {2})'.format(key, val, self._register[key]['default']))
            print('\t{0}'.format(desc))
            print('\t({0})'.format(self._register[key]['host']))

    def __setattr__(self, key, value):
        '''
        Allow setting configuration options using the standard python attribute
        methods, as described in the class documentation.

        Attributes prefixed with '_' are loaded from this class.
        '''
        if key.startswith('_'):
            object.__setattr__(self, key, value)
        elif key in self._register:
            if self._register[key]['type'] is not None:
                if not isinstance(value, self._register[key]['type']):
                    raise ValueError(
                        "{} must be in type(s) {}".format(key, self._register[key]['type']))
            if self._register[key]['onchange'] is not None:
                self._register[key]['onchange'](value)
            self._config[key] = value
        else:
            raise KeyError("No such configuration key `{0}`.".format(key))

    def __getattr__(self, key):
        '''
        Allow retrieval of configuration keys using standard python atribute
        methods, as described in the class documentation.

        Attributes prefixed with '_' are loaded from this class.
        '''
        if key.startswith('_'):
            return object.__getattr__(self, key)
        if key in self._register:
            if key in self._config:
                return self._config[key]

            # if a lazy loader is specified, use it
            if self._register[key]['default'] is None and self._register[key]['onload'] is not None:
                setattr(self, key, self._register[key]['onload']())

            return self._config.get(key, self._register[key]['default'])
        raise AttributeError("No such configuration key `{0}`.".format(key))

    def reset(self, *keys, **target_config):
        '''
        Reset all configuration keys specifed to their default values, or values
        specified in `target_config`. If both `keys` and `target_config` are
        specified, `keys` acts to both filter the keys of `target_config` and add
        default values as the missing keys.
        >>> config.reset('logging_level')
        >>> config.reset('logging_level', logging_level=10)
        >>> config.reset(logging_level=10)

        If no keys are specified, reset all keys:
        >>> config.reset()
        '''
        if len(keys) == 0:
            keys = set(list(self._register.keys()) + list(target_config.keys()))

        target_config = self.__restrict_keys(target_config, keys)
        reset_keys = [key for key in keys if key not in target_config]

        for key, value in target_config.items():
            self._config[key] = value
            if key in self._register:
                if value == self._register[key]['default']:
                    self._config.pop(key)
                if self._register[key]['onchange'] is not None:
                    self._register[key]['onchange'](getattr(self, key))
            else:  # Allow users to delete deprecated keys
                logger.warning("Added value for configuration key `{0}` which has yet to be registered.".format(key))

        for key in reset_keys:
            self._config.pop(key)
            if key in self._register and self._register[key]['onchange'] is not None:
                self._register[key]['onchange'](getattr(self, key))

    def __restrict_keys(self, d, keys):
        if keys is None:
            return d
        return {key: d[key] for key in keys if key in d}

    def save(self, filename=None, keys=None, replace=None):
        '''
        Save the current configuration as a JSON file. Accepted arguments are:
         - filename : The location of the file to be saved. If not specified,
            default configuration location is used (and autoloaded on startup).
         - keys : The keys to be saved. If `None`, all keys are saved (or set to
            default values if missing).
         - replace : Whether the configuration file should be replaced (True), or
            simply updated (False). If False, then the existing keys stored in the
            maintained except where they conflict with the keys specified. The
            default value is `None`, in which case it maps to `True` if keys=None,
            or `False` if specific keys are specified. (default=None)
        '''
        filename = filename or self._config_path
        filename = os.path.join(ensure_path_exists(os.path.dirname(filename)), os.path.basename(filename))
        config = {}
        if replace is None:
            replace = True if keys is None else False
        if keys is None:
            replace = True
        if not replace and os.path.exists(filename):
            with io.open(filename, 'r') as f:
                config = json.load(f)
        config.update(self.__restrict_keys(self._config, keys))
        with io.open(filename, 'w') as f:
            json_config = json.dumps(config, ensure_ascii=False, indent=4)
            if sys.version_info.major == 2 and isinstance(json_config, six.string_types):
                json_config = json_config.decode("utf-8")
            f.write(json_config)

    def load(self, filename=None, keys=None, replace=None, force=False):
        '''
        Load a configuration from the disk. Accepted arguments are:
         - filename : The location of the configuration. By default, this will
            point to the automatically loaded configuration file.
         - keys : The keys to load from the configuration. If `None`, all keys
            are loaded from config file (or set to default values if missing).
         - replace : Whether the current configuration should be replaced (True), or
           simply updated (False). If False, then the existing configuration will be
           maintained except where conflicts exist with the keys being loaded. The
           default value is `None`, in which case it maps to `True` if keys=None,
           or `False` if specific keys are specified. (default=None)
         - force : Ordinarily, new configuration is run through the standard checks
            but in some cases (such as startup), the register has yet to be filled,
            and so results in spouts spurious warnings. This allows one to bypass
            all checks.
        '''
        filename = filename or self._config_path
        if replace is None:
            replace = True if keys is None else False
        if keys is None:
            replace = True
        with io.open(filename, 'r') as f:
            config = self.__restrict_keys(json.load(f), keys)
            if force:
                self._config = config
            else:
                if replace:
                    self.reset(**config)
                else:
                    self.reset(*keys, **config)


config = Configuration()
