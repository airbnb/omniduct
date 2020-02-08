import six
import yaml

from omniduct.duct import Duct
from omniduct.errors import DuctNotFound, DuctProtocolUnknown
from omniduct.utils.debug import logger
from omniduct.utils.magics import MagicsProvider
from omniduct.utils.proxies import TreeProxy


class DuctRegistry(object):
    """
    A convenient registry for `Duct` instances.

    This class provides a simple interface to a pool of configured services,
    allowing convenient lookups of available services and the creation of new
    ones. It also allows for the batch creation of services from a shared
    configuration, which is especially useful in a company deployment.
    """

    def __init__(self, config=None):
        """
        Args:
            config (iterable, dict, str, None): Refer to `.import_from_config`
                for more details (default: `None`).
        """
        self._registry = {}

        if config:
            self.register_from_config(config)

    def __repr__(self):
        return "<DuctRegistry with {} registered ducts>".format(len(self._registry))

    # Registration methods
    def register(self, duct, name=None, override=False, register_magics=True):
        """
        Register an existing Duct instance into the registry.

        Names of ducts can consist of any valid Python identifier, and multiple
        names can be provided as a comma separated list in which case the names
        will be aliases referring to the same `Duct` instance. Keep in mind that
        any name must uniquely identify one `Duct` instance.

        Args:
            duct (Duct): The `Duct` instance to be registered.
            name (str): An optional name to use when registering. If not
                provided this will fall back to `duct.name`. If neither is
                configured, an error will be thrown. Name can be a
                comma-separated list of names, in which case the names are
                aliases and will point to the same `Duct` instance.
            override (bool): Whether to override any existing `Duct` instance
                of the same name. If `False`, any overrides will result in an
                exception.

        Returns:
            Duct: The `Duct` instance being registered.
        """
        name = name or duct.name
        if name is None:
            raise ValueError("`Duct` instances must be named to be registered. Please either specify a name to this method call, or add a name to the Duct using `duct.name = '...'`.")
        names = [n.strip() for n in name.split(',')]
        for name in names:
            if name in self._registry and not override:
                raise ValueError("`Duct` with the same name ('{}') already present in the registry. Please pass `override=True` if you want to override the existing instance, or `name='...'` to specify a new name.".format(name))
            if register_magics and isinstance(duct, MagicsProvider):
                duct.register_magics(base_name=name)
            self._registry[name] = duct
        return duct

    def new(self, name, protocol, override=False, register_magics=True, **kwargs):
        """
        Create a new service and register it into the registry.

        Args:
            name (str): The name (or names) of the target service. If multiple
                aliases are to be used, names should be a comma separated list.
                See `.register` for more details.
            protocol (str): The protocol of the new service.
            override (bool): Whether to override any existing `Duct` instance
                of the same name. If `False`, any overrides will result in an
                exception.
            register_magics (bool): Whether to register the magics if running in
                and IPython session (default: `True`).
            **kwargs (dict): Additional arguments to pass to the constructor of
                the class associated with the nominated protocol.

        Returns:
            Duct: The `Duct` instance registered into the registry.
        """
        return self.register(
            Duct.for_protocol(protocol)(
                name=name.split(',')[0].strip(),
                registry=self,
                **kwargs
            ),
            name=name,
            override=override,
            register_magics=register_magics
        )

    # Inspection and retrieval methods
    @property
    def names(self):
        """list: The names of all ducts in the registry."""
        return sorted(self._registry.keys())

    def __getitem__(self, name):
        return self._registry[name]

    def __contains__(self, name):
        return name in self._registry

    def __iter__(self):
        return iter(self._registry.values())

    def lookup(self, name, kind=None):
        """
        Look up an existing registered `Duct` by name and (optionally) kind.

        Args:
            name (str): The name of the `Duct` instance.
            kind (str, Duct.Type): The kind of `Duct` to which the lookup should
                be restricted.

        Returns:
            `Duct`: The looked up `Duct` instance.

        Raises:
            DuctNotFound: If no `Duct` can be found for requested name and/or
                type.
        """
        if kind and not isinstance(kind, Duct.Type):
            kind = Duct.Type(kind)
        if name not in self._registry:
            raise DuctNotFound(name)
        duct = self._registry[name]
        if kind and duct.DUCT_TYPE != kind:
            raise DuctNotFound("Duct named '{}' exists, but is not of kind '{}'.".format(name, kind.value))
        return duct

    # Exposing `Duct` instances.
    def populate_namespace(self, namespace=None, names=None, kinds=None):
        """
        Populate a nominated namespace with references to a subset of ducts.

        While a registry object is a great way to store and configure `Duct`
        instances, it is sometimes desirable to surface frequently used
        instances in other more convenient namespaces (such as the globals of
        your module).

        Args:
            namespace (dict, None): The namespace to populate. If using from a
                module you can pass `globals()`. If `None`, a new dictionary is
                created, populated and then returned.
            names (list<str>, None): The names to include in the population. If
                not specified then all names will be exported.
            kinds (list<str>, None): The kinds of ducts to include in the
                population. If not specified, all kinds will be exported.

        Returns:
            dict: The populated namespace.
        """
        if namespace is None:
            namespace = {}
        if kinds is not None:
            kinds = [Duct.Type(kind) if not isinstance(kind, Duct.Type) else kind for kind in kinds]
        for name, duct in self._registry.items():
            if (kinds is None or duct.DUCT_TYPE in kinds) and (names is None or name in names):
                namespace[name.split('/')[-1]] = duct
        return namespace

    def get_proxy(self, by_kind=True):
        """
        Return a structured proxy object for easy exploration of services.

        This method returns a proxy object to the registry upon which the `Duct`
        instances are available as attributes. This object is
        also by default structured such that one first accesses an attribute
        associated with a kind, which makes larger collections of services
        more easily navigatable.

        For example, if you have `DatabaseClient` subclass registered as
        'my_service', you could access it on the proxy using:
        >>> proxy = registry.get_proxy(by_kind=True)
        >>> proxy.databases.my_service

        Args:
            by_kind (bool): Whether to nest proxy of `Duct` instances by kind.

        Returns:
            ServicesProxy: The proxy object.
        """
        def key_parser(k, v):
            keys = k.split('/')
            if by_kind and getattr(v, 'DUCT_TYPE', None) is not None:
                keys.insert(0, v.DUCT_TYPE.value)
            return keys

        dct = self._registry.copy()
        dct['registry'] = self

        return TreeProxy._for_dict(dct, key_parser=key_parser, name='services')

    # Batch registration of duct configurations
    def register_from_config(self, config, override=False):
        """
        Register a collection of Duct service configurations.

        The configuration format must be one of the following:
        - An iterable sequence of dictionaries containing a mapping between the
          keyword arguments required to instantiate the `Duct` subclass.
        - A dictionary mapping names of `Duct` instances to dictionaries of
          keyword arguments.
        - A dictionary mapping Duct types ('databases', 'filesystems', etc) to
          mappings like those immediately above.
        - A string YAML representation of one of the above (with at least one
          newline character).
        - A string filename containing such a YAML representation.

        There are three special keyword arguments that are required by the
        `DuctRegistry` instance:
        - name: Should be present only in the configuration dictionary when
          config is provided as an iterable sequence of dictionaries.
        - protocol: Which specifies which `Duct` subclass to fetch. Failure to
          correctly set this will result in a warning and an ignoring of this
          configuration.
        - register_magics (optional): A boolean flag indicating whether to
          register any magics defined by this Duct class (default: True).

        Args:
            config (iterable, dict, str, None): A configuration specified in one
                of the above described formats.
            override (bool): Whether to override any existing `Duct` instance
                of the same name(s). If `False`, any overrides will result in an
                exception.
        """
        # Extract configuration from a file if necessary, and then process it.
        if isinstance(config, six.string_types):
            if '\n' in config:
                config = yaml.safe_load(config)
            else:
                with open(config) as f:
                    config = yaml.safe_load(f.read())
        config = self._process_config(config)

        for duct_config in config:
            names = duct_config.pop('name')
            protocol = duct_config.pop('protocol')
            register_magics = duct_config.pop('register_magics', True)
            try:
                self.new(names, protocol, register_magics=register_magics, override=override, **duct_config)
            except DuctProtocolUnknown as e:
                logger.error("Failed to configure `Duct` instance(s) '{}'. {}".format("', '".join(names.split(',')), str(e)))

        return self

    def _process_config(self, config, name=None):
        """
        Coerce the configuration into a generator of dictionaries of keyword
        arguments; each corresponding to a duct instance.
        """

        if isinstance(config, dict) and (name is not None or 'name' in config) and 'protocol' in config and not config.get('__OMNIDUCT_SKIP__', False):
            kwargs = config.copy()
            if 'name' not in config:
                kwargs['name'] = name
            yield kwargs

        elif isinstance(config, dict):
            for name, subconfig in config.items():
                for config in self._process_config(subconfig, name=name):
                    yield config

        elif isinstance(config, list):
            for subconfig in config:
                for config in self._process_config(subconfig):
                    yield config
