import atexit
import functools
import getpass
import inspect
import os
import pwd
import re
from abc import abstractmethod
from builtins import input
from enum import Enum

import six
from future.utils import raise_with_traceback, with_metaclass

from omniduct.errors import DuctServerUnreachable
from omniduct.utils.debug import logger, logging_scope
from omniduct.utils.dependencies import check_dependencies
from omniduct.utils.docs import quirk_docs
from omniduct.utils.metaclasses import ProtocolRegisteringQuirkDocumentedABCMeta
from omniduct.utils.ports import is_port_bound, naive_load_balancer


class Duct(with_metaclass(ProtocolRegisteringQuirkDocumentedABCMeta, object)):
    """
    The abstract base class for all protocol implementations.

    This class defines the basic lifecycle of service connections, along with
    some magic that provides automatic registration of Duct protocol
    implementations. All connections made by `Duct` instances are lazy, meaning
    that instantiation is "free", and no protocol connections are made until
    required by subsequent interactions (i.e. when the value of any attribute in
    the list of `connection_fields` is accessed). All `Ducts` will automatically
    connnect and disconnect as required, and so manual intervention is not
    typically required to maintain connections.

    Attributes:
        protocol (str): The name of the protocol for which this instance was
            created (especially useful if a `Duct` subclass supports multiple
            protocols).
        name (str): The name given to this `Duct` instance (defaults to class
            name).
        registry (None, omniduct.registry.DuctRegistry): A reference to a
            `DuctRegistry` instance for runtime lookup of other services.
        remote (None, omniduct.remotes.base.RemoteClient): A reference to a
            `RemoteClient` instance to manage connections to remote services.
        cache (None, omniduct.caches.base.Cache): A reference to a `Cache`
            instance to add support for caching, if applicable.
        connection_fields (tuple<str>, list<str>): A list of instance attributes
            to monitor for changes, whereupon the `Duct` instance should automatically
            disconnect. By default, the following attributes are monitored:
            'host', 'port', 'remote', 'username', and 'password'.
        prepared_fields (tuple<str>, list<str>): A list of instance attributes to
            be populated (if their values are callable) when the instance first
            connects to a service. Refer to `Duct.prepare` and `Duct._prepare` for
            more details. By default, the following attributes are prepared:
            '_host', '_port', '_username', and '_password'.

    Additional attributes including `host`, `port`, `username` and `password` are
    documented inline below.

    Class Attributes:
        AUTO_LOGGING_SCOPE (bool): Whether this class should be used by omniduct
            logging code as a "scope". Should be overridden by subclasses as
            appropriate.
        DUCT_TYPE (Duct.Type): The type of `Duct` service that is provided by
            this Duct instance. Should be overridden by subclasses as
            appropriate.
        PROTOCOLS (list<str>): The name(s) of any protocols that should be
            associated with this class. Should be overridden by subclasses as
            appropriate.
    """
    __doc_attrs = """
        protocol (str): The name of the protocol for which this instance was
            created (especially useful if a `Duct` subclass supports multiple
            protocols).
        name (str): The name given to this `Duct` instance (defaults to class
            name).
        host (str): The host name providing the service (will be '127.0.0.1', if
            service is port forwarded from remote; use `._host` to see remote
            host).
        port (int): The port number of the service (will be the port-forwarded
            local port, if relevant; for remote port use `._port`).
        username (str, bool): The username to use for the service.
        password (str, bool): The password to use for the service.
        registry (None, omniduct.registry.DuctRegistry): A reference to a
            `DuctRegistry` instance for runtime lookup of other services.
        remote (None, omniduct.remotes.base.RemoteClient): A reference to a
            `RemoteClient` instance to manage connections to remote services.
        cache (None, omniduct.caches.base.Cache): A reference to a `Cache`
            instance to add support for caching, if applicable.
        connection_fields (tuple<str>, list<str>): A list of instance attributes
            to monitor for changes, whereupon the `Duct` instance should automatically
            disconnect. By default, the following attributes are monitored:
            'host', 'port', 'remote', 'username', and 'password'.
        prepared_fields (tuple<str>, list<str>): A list of instance attributes to
            be populated (if their values are callable) when the instance first
            connects to a service. Refer to `Duct.prepare` and `Duct._prepare` for
            more details. By default, the following attributes are prepared:
            '_host', '_port', '_username', and '_password'.
    """
    __doc_cls_attrs__ = None

    class Type(Enum):
        """
        The `Duct.Type` enum specifies all of the permissible values of
        `Duct.DUCT_TYPE`. Also determines the order in which ducts are loaded by DuctRegistry.
        """
        REMOTE = 'remotes'
        FILESYSTEM = 'filesystems'
        CACHE = 'caches'
        RESTFUL = 'rest_clients'
        DATABASE = 'databases'
        OTHER = 'other'

    AUTO_LOGGING_SCOPE = True
    DUCT_TYPE = None
    PROTOCOLS = None

    def __init__(self, protocol=None, name=None, registry=None, remote=None,
                 host=None, port=None, username=None, password=None, cache=None,
                 cache_namespace=None):
        """
        protocol (str, None): Name of protocol (used by Duct registries to inform
            Duct instances of how they were instantiated).
        name (str, None): The name to used by the `Duct` instance (defaults to
            class name if not specified).
        registry (DuctRegistry, None): The registry to use to lookup remote
            and/or cache instance specified by name.
        remote (str, RemoteClient): The remote by which the ducted service
            should be contacted.
        host (str): The hostname of the service to be used by this client.
        port (int): The port of the service to be used by this client.
        username (str, bool, None): The username to authenticate with if necessary.
            If True, then users will be prompted at runtime for credentials.
        password (str, bool, None): The password to authenticate with if necessary.
            If True, then users will be prompted at runtime for credentials.
        cache(Cache, None): The cache client to be attached to this instance.
            Cache will only used by specific methods as configured by the client.
        cache_namespace(str, None): The namespace to use by default when writing
            to the cache.
        """

        check_dependencies([protocol])

        self.protocol = protocol
        self.name = name or self.__class__.__name__
        self.registry = registry
        self.remote = remote
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.cache = cache
        self.cache_namespace = cache_namespace

        self.connection_fields = ('host', 'port', 'remote', 'username', 'password')
        self.prepared_fields = ('_host', '_port', '_username', '_password')

        atexit.register(self.disconnect)
        self.__prepared = False
        self.__getting = False
        self.__disconnecting = False
        self.__cached_auth = {}
        self.__prepreparation_values = {}

    @property
    def __prepare_triggers(self):
        return (
            ('cache',)
            + object.__getattribute__(self, 'connection_fields')
        )

    @classmethod
    def __init_with_kwargs__(cls, self, kwargs, **fallbacks):
        if not hasattr(self, '_Duct__inited_using_kwargs'):
            self._Duct__inited_using_kwargs = {}
        for cls_parent in reversed([parent for parent in inspect.getmro(cls) if issubclass(parent, Duct) and parent not in self._Duct__inited_using_kwargs and '__init__' in parent.__dict__]):
            self._Duct__inited_using_kwargs[cls_parent] = True
            if six.PY3:
                argspec = inspect.getfullargspec(cls_parent.__init__)
                keys = argspec.args[1:] + argspec.kwonlyargs
            else:
                keys = inspect.getargspec(cls_parent.__init__).args[1:]
            params = {}
            for key in keys:
                if key in kwargs:
                    params[key] = kwargs.pop(key)
                elif key in fallbacks:
                    params[key] = fallbacks[key]
            cls_parent.__init__(self, **params)

    @classmethod
    def for_protocol(cls, protocol):
        """
        Retrieve a `Duct` subclass for a given protocol.

        Args:
            protocol (str): The protocol of interest.

        Returns:
            functools.partial object: The appropriate class for the provided,
                partially constructed with the `protocol` keyword argument
                set appropriately.

        Raises:
            DuctProtocolUnknown: If no class has been defined that offers the
                named protocol.
        """
        return functools.partial(cls._for_protocol(protocol), protocol=protocol)

    def __getattribute__(self, key):
        try:
            if (not object.__getattribute__(self, '_Duct__prepared')
                    and not object.__getattribute__(self, '_Duct__getting')
                    and not object.__getattribute__(self, '_Duct__disconnecting')
                    and key in object.__getattribute__(self, '_Duct__prepare_triggers')):
                object.__setattr__(self, '_Duct__getting', True)
                object.__getattribute__(self, 'prepare')()
                object.__setattr__(self, '_Duct__getting', False)
        except AttributeError:
            pass
        except Exception as e:
            object.__setattr__(self, '_Duct__getting', False)
            raise_with_traceback(e)
        return object.__getattribute__(self, key)

    def __setattr__(self, key, value):
        try:
            if (getattr(self, '_Duct__prepared', False)
                    and getattr(self, 'connection_fields', None)
                    and key in self.connection_fields
                    and self.is_connected()):
                logger.warn('Disconnecting prior to changing field that connection is based on: {}.'.format(key))
                self.disconnect()
                self.__prepared = False
        except AttributeError:
            pass
        object.__setattr__(self, key, value)

    @quirk_docs('_prepare')
    def prepare(self):
        """
        Prepare a Duct subclass for use (if not already prepared).

        This method is called before the value of any of the fields referenced
        in `self.connection_fields` are retrieved. The fields include, by
        default: 'host', 'port', 'remote', 'cache', 'username', and 'password'.
        Subclasses may add or subtract from these special fields.

        When called, it first checks whether the instance has already been
        prepared, and if not calls `_prepare` and then records that the instance
        has been successfully prepared.
        """
        if not self.__prepared:
            self._prepare()
            self.__prepared = True

    def _prepare(self):
        """
        This method may be overridden by subclasses, but provides the following
        default behaviour:

         - Ensures `self.registry`, `self.remote` and `self.cache` values are
           instances of the right types.
         - It replaces string values of `self.remote` and `self.cache` with
           remotes and caches looked up using `self.registry.lookup`.
         - It looks through each of the fields nominated in `self.prepared_fields`
           and, if the corresponding value is callable, sets the value of that
           field to result of calling that value with a reference to `self`. By
           default, `prepared_fields` contains '_host', '_port', '_username',
           and '_password'.
         - Ensures value of self.port is an integer (or None).
        """

        # Import necessary classes lazily (to prevent dependency cycles)
        from omniduct.registry import DuctRegistry
        from omniduct.caches.base import Cache
        from omniduct.remotes.base import RemoteClient

        # Check registry is of an appropriate type (if present)
        assert (self.registry is None) or isinstance(self.registry, DuctRegistry), "Provided registry is not an instance of `omniduct.registry.DuctRegistry`."

        # If registry is present, lookup remotes and caches if necessary
        if self.registry is not None:
            if self.remote and isinstance(self.remote, six.string_types):
                self.__prepreparation_values['remote'] = self.remote
                self.remote = self.registry.lookup(self.remote, kind=Duct.Type.REMOTE)
            if self.cache and isinstance(self.cache, six.string_types):
                self.__prepreparation_values['cache'] = self.cache
                self.cache = self.registry.lookup(self.cache, kind=Duct.Type.CACHE)

        # Check if remote and cache objects are of correct type (if present)
        assert (self.remote is None) or isinstance(self.remote, RemoteClient), "Provided remote is not an instance of `omniduct.remotes.base.RemoteClient`."
        assert (self.cache is None) or isinstance(self.cache, Cache), "Provided cache is not an instance of `omniduct.caches.base.Cache`."

        # Replace prepared fields with the result of calling existing values
        # with a reference to `self`.
        for field in self.prepared_fields:
            value = getattr(self, field)
            if hasattr(value, '__call__'):
                self.__prepreparation_values[field] = value
                setattr(self, field, value(self))

        if isinstance(self._host, (list, tuple)):
            if '_host' not in self.__prepreparation_values:
                self.__prepreparation_values['_host'] = self._host
            self._host = naive_load_balancer(self._host, port=self._port)

        # If host has a port included in it, override the value of self._port
        if self._host is not None and re.match(r'[^\:]+:[0-9]{1,5}', self._host):
            self._host, self._port = self._host.split(':')

        # Ensure port is an integer value
        self.port = int(self._port) if self._port else None

    def reset(self):
        """
        Reset this `Duct` instance to its pre-preparation state.

        This method disconnects from the service, resets any temporary
        authentication and restores the values of the attributes listed in
        `prepared_fields` to their values as of when `Duct.prepare` was called.

        Returns:
            `Duct` instance: A reference to this object.
        """
        self.disconnect()
        self.__cached_auth = {}

        for key, value in self.__prepreparation_values.items():
            setattr(self, key, value)
        self.__prepreparation_values = {}
        self.__prepared = False

        return self

    @property
    def host(self):
        """
        str: The host name providing the service, or '127.0.0.1' if `self.remote` is
        not `None`, whereupon the service will be port-forwarded locally. You can
        view the remote hostname using `duct._host`, and change the remote host
        at runtime using: `duct.host = '<host>'`.
        """
        if self.remote:
            return '127.0.0.1'  # TODO: Make this configurable.
        return self._host

    @host.setter
    def host(self, host):
        self._host = host

    @property
    def port(self):
        """
        int: The local port for the service. If `self.remote` is not `None`, the
        port will be port-forwarded from the remote host. To see the port used on
        the remote host refer to `duct._port`. You can change the remote port
        at runtime using: `duct.port = <port>`.
        """
        if self.remote:
            return self.remote.port_forward('{}:{}'.format(self._host, self._port))
        return self._port

    @port.setter
    def port(self, port):
        self._port = port

    @property
    def username(self):
        """
        str: Some services require authentication in order to connect to the
        service, in which case the appropriate username can be specified. If not
        specified at instantiation, your local login name will be used. If `True`
        was provided, you will be prompted to type your username at runtime as
        necessary. If `False` was provided, then `None` will be returned. You can
        specify a different username at runtime using: `duct.username = '<username>'`.
        """
        if self._username is True:
            if 'username' not in self.__cached_auth:
                self.__cached_auth['username'] = input("Enter username for '{}':".format(self.name))
            return self.__cached_auth['username']
        elif self._username is False:
            return None
        elif not self._username:
            try:
                username = os.getlogin()
            except OSError:
                username = pwd.getpwuid(os.geteuid()).pw_name
            return username
        return self._username

    @username.setter
    def username(self, username):
        self._username = username

    @property
    def password(self):
        """
        str: Some services require authentication in order to connect to the
        service, in which case the appropriate password can be specified. If
        `True` was provided at instantiation, you will be prompted to type your
        password at runtime when necessary. If `False` was provided, then
        `None` will be returned. You can specify a different password at runtime
        using: `duct.password = '<password>'`.
        """
        if self._password is True:
            if 'password' not in self.__cached_auth:
                self.__cached_auth['password'] = getpass.getpass("Enter password for '{}':".format(self.name))
            return self.__cached_auth['password']
        elif self._password is False:
            return None
        return self._password

    @password.setter
    def password(self, password):
        self._password = password

    def __assert_server_reachable(self):
        if self.host is not None or self.port is not None:
            if self.host is None:
                raise ValueError("Port specified but no host provided.")
            if self.port is None:
                raise ValueError("Host specified but no port specified.")
        else:
            return

        if not is_port_bound(self.host, self.port):
            if self.remote and not self.remote.is_port_bound(self._host, self._port):
                self.disconnect()
                raise DuctServerUnreachable(
                    "Remote '{}' cannot connect to '{}:{}'. Please check your settings before trying again.".format(
                        self.remote.name, self._host, self._port))
            elif not self.remote:
                self.disconnect()
                raise DuctServerUnreachable(
                    "Cannot connect to '{}:{}' on your current connection. Please check your connection before trying again.".format(
                        self.host, self.port))

    # Connection
    @logging_scope("Connecting")
    @quirk_docs('_connect')
    def connect(self):
        """
        Connect to the service backing this client.

        It is not normally necessary for a user to manually call this function,
        since when a connection is required, it is automatically created.

        Returns:
            `Duct` instance: A reference to the current object.
        """
        if self.host:
            logger.info(
                "Connecting to {host}:{port}{remote}.".format(
                    host=self._host,
                    port=self._port,
                    remote="on {}".format(self.remote.host) if self.remote else ""
                )
            )
        self.__assert_server_reachable()
        if not self.is_connected():
            try:
                self._connect()
            except Exception as e:
                self.reset()
                raise_with_traceback(e)
        if self.host:
            logger.info(
                "Connected to {host}:{port}{remote}.".format(
                    host=self._host,
                    port=self._port,
                    remote="on {}".format(self.remote.host) if self.remote else ""
                )
            )
        return self

    @abstractmethod
    def _connect(self):
        raise NotImplementedError

    @quirk_docs('_is_connected')
    def is_connected(self):
        """
        Check whether this `Duct` instances is currently connected.

        This method checks to see whether a `Duct` instance is currently
        connected. This is performed by verifying that the remote host and port
        are still accessible, and then by calling `Duct._is_connected`, which
        should be implemented by subclasses.

        Returns:
            bool: Whether this `Duct` instance is currently connected.
        """
        if not self.__prepared:
            return False

        if self.remote:
            if not self.remote.has_port_forward(self._host, self._port):
                return False
            elif not is_port_bound(self.host, self.port):
                self.disconnect()
                return False

        return self._is_connected()

    @abstractmethod
    def _is_connected(self):
        raise NotImplementedError

    @quirk_docs('_disconnect')
    def disconnect(self):
        """
        Disconnect this client from backing service.

        This method is automatically called during reconnections and/or at
        Python interpreter shutdown. It first calls `Duct._disconnect` (which
        should be implemented by subclasses) and then notifies the
        `RemoteClient` subclass, if present, to stop port-forwarding the remote
        service.

        Returns:
            `Duct` instance: A reference to this object.
        """
        if not self.__prepared:
            return

        self.__disconnecting = True

        try:
            self._disconnect()

            if self.remote and self.remote.has_port_forward(self._host, self._port):
                logger.info('Freeing up local port {0}...'.format(self.port))
                self.remote.port_forward_stop(local_port=self.port)
        finally:
            self.__disconnecting = False

        return self

    @abstractmethod
    def _disconnect(self):
        raise NotImplementedError

    def reconnect(self):
        """
        Disconnects, and then reconnects, this client.

        Note: This is equivalent to `duct.disconnect().connect()`.

        Returns:
            `Duct` instance: A reference to this object.
        """
        return self.disconnect().connect()
