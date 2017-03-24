import six
import yaml

from omniduct.duct import Duct
from omniduct.utils.magics import MagicsProvider


class DuctRegistry(object):

    def __init__(self, config=None):
        self._registry = {}

        if config:
            self.import_from_config(config)

    # Registry methods
    def register(self, duct, name=None):
        name = name or duct.name
        if name is None:
            raise ValueError("Client must be named to be registered. Either specify a name to this method call, or add a name to the Duct.")
        self._registry[name] = duct

    def lookup(self, name, kind=None):
        if kind and not isinstance(kind, Duct.Type):
            kind = Duct.Type(kind)
        r = self._registry[name]
        if kind and r.DUCT_TYPE != kind:
            raise KeyError("No duct called '{}' of kind '{}'.".format(name, kind.value))
        return r

    # Duct creation/loading methods
    def new(self, names, protocol, register_magics=True, **options):
        if isinstance(names, six.string_types):
            names = names.split(',')
        duct = Duct.for_protocol(protocol)(name=names[0], registry=self, **options)
        for name in names:
            self.register(duct, name=name)
            if register_magics and isinstance(duct, MagicsProvider):
                duct.register_magics(base_name=name)
        return duct

    def import_from_config(self, config):
        config = self._process_config(config)

        for t in [t.value for t in Duct.Type]:
            for names, options in config.get(t, {}).items():
                protocol = options.pop('protocol')
                register_magics = options.pop('register_magics', True)
                self.new(names, protocol, register_magics=register_magics, **options)

        return self

    def _process_config(self, config):
        if isinstance(config, six.string_types):
            try:
                config = yaml.load(config)
            except:
                with open(config) as f:
                    if config.endswith('.py'):
                        namespace = {}
                        exec(f.read(), namespace)
                        config = namespace.get('OMNIDUCT_CONFIG')
                    elif config.endswith('.yml') or config.endswith('.yaml'):
                        config = yaml.load(f.read())
                    else:
                        raise RuntimeError("Configuration file '{}' not understood.".format(config))
        return config

    def populate_namespace(self, namespace=None):
        if namespace is None:
            namespace = {}
        for name, duct in self._registry.items():
            namespace[name] = duct
        return namespace
