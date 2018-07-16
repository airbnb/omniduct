import six
import yaml

from omniduct.duct import Duct
from omniduct.errors import DuctProtocolUnknown
from omniduct.utils.debug import logger
from omniduct.utils.magics import MagicsProvider
from omniduct.utils.proxies import NestedDictObjectProxy


class DuctRegistry(object):

    class Proxy(NestedDictObjectProxy):

        def __init__(self, registry, by_kind=True):
            self._self_registry = registry
            get_nesting = None
            if by_kind:
                def get_nesting(k, v):
                    nesting = k.split('/')
                    if v.DUCT_TYPE is not None:
                        nesting.insert(0, v.DUCT_TYPE.value)
                    return nesting

            NestedDictObjectProxy.__init__(self, registry._registry, is_flat=True, get_nesting=get_nesting)

        @property
        def registry(self):
            # This will only appear at top level of proxy, since children will not be of this type
            return self._self_registry

        def __dir__(self):
            return NestedDictObjectProxy.__dir__(self) + ['registry']

    def __init__(self, config=None):
        self._registry = {}

        if config:
            self.import_from_config(config)

    def __repr__(self):
        return "<DuctRegistry with {} registered ducts>".format(len(self._registry))

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

    @property
    def names(self):
        return sorted(self._registry.keys())

    def __getitem__(self, name):
        return self._registry[name]

    def __contains__(self, name):
        return name in self._registry

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
                try:
                    self.new(names, protocol, register_magics=register_magics, **options)
                except DuctProtocolUnknown as e:
                    logger.error("Failed to configure `Duct` instance(s) '{}'. {}".format("', '".join(names.split(',')), str(e)))

        return self

    def _process_config(self, config):
        if isinstance(config, six.string_types):
            try:
                config = yaml.load(config)
                if not isinstance(config, dict):
                    raise ValueError("Invalid configuration specified.")
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

    # Accessing ducts
    def populate_namespace(self, namespace=None, include=None, kinds=None):
        if namespace is None:
            namespace = {}
        if kinds is not None:
            kinds = [Duct.Type(kind) if not isinstance(kind, Duct.Type) else kind for kind in kinds]
        for name, duct in self._registry.items():
            if (kinds is None or duct.DUCT_TYPE in kinds) and (include is None or name in include):
                namespace[name.split('/')[-1]] = duct
        return namespace

    def get_proxy(self, by_kind=False):
        return DuctRegistry.Proxy(self, by_kind=by_kind)
