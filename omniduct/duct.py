from __future__ import annotations

import atexit
import functools
import getpass
import inspect
import os
import pwd
import re
from abc import abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

from interface_meta import InterfaceMeta, inherit_docs
from typing_extensions import Self

from omniduct.errors import DuctProtocolUnknown, DuctServerUnreachable
from omniduct.utils.debug import logger, logging_scope
from omniduct.utils.dependencies import check_dependencies
from omniduct.utils.ports import is_port_bound, naive_load_balancer

if TYPE_CHECKING:
    from omniduct.caches.base import Cache
    from omniduct.registry import DuctRegistry
    from omniduct.remotes.base import RemoteClient


class Duct(metaclass=InterfaceMeta):
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

        Additional attributes including `host`, `port`, `username` and `password` are
        documented inline.

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
    __doc_cls_attrs__: str | None = None

    INTERFACE_SKIPPED_NAMES: set[str] | None = {"__init__", "_init"}

    class Type(Enum):
        """
        The `Duct.Type` enum specifies all of the permissible values of
        `Duct.DUCT_TYPE`. Also determines the order in which ducts are loaded by DuctRegistry.
        """

        REMOTE = "remotes"
        FILESYSTEM = "filesystems"
        CACHE = "caches"
        RESTFUL = "rest_clients"
        DATABASE = "databases"
        OTHER = "other"

    AUTO_LOGGING_SCOPE: bool = True
    DUCT_TYPE: Type | None = None
    PROTOCOLS: list[str] | None = None

    # Prepared fields
    _host: str | None
    _port: int | None
    _username: str | bool | None
    _password: str | bool | None

    def __init__(
        self,
        protocol: str | None = None,
        name: str | None = None,
        registry: DuctRegistry | None = None,
        remote: RemoteClient | str | None = None,
        host: str | None = None,
        port: int | None = None,
        username: str | bool | None = None,
        password: str | bool | None = None,
        cache: Cache | str | None = None,
        cache_namespace: str | None = None,
    ) -> None:
        """
        protocol: Name of protocol (used by Duct registries to inform
            Duct instances of how they were instantiated).
        name: The name to used by the `Duct` instance (defaults to
            class name if not specified).
        registry: The registry to use to lookup remote
            and/or cache instance specified by name.
        remote: The remote by which the ducted service
            should be contacted.
        host: The hostname of the service to be used by this client.
        port: The port of the service to be used by this client.
        username: The username to authenticate with if necessary.
            If True, then users will be prompted at runtime for credentials.
        password: The password to authenticate with if necessary.
            If True, then users will be prompted at runtime for credentials.
        cache: The cache client to be attached to this instance.
            Cache will only used by specific methods as configured by the client.
        cache_namespace: The namespace to use by default when writing
            to the cache.
        """

        if protocol is not None:
            check_dependencies([protocol])

        self.protocol: str | None = protocol
        self.name: str = name or self.__class__.__name__
        self.registry: DuctRegistry | None = registry
        self.remote: RemoteClient | str | None = remote
        self.host: str | None = host
        self.port: int | None = port
        self.username: str | bool | None = username
        self.password: str | bool | None = password
        self.cache: Cache | str | None = cache
        self.cache_namespace: str | None = cache_namespace

        self.connection_fields: tuple[str, ...] = (
            "host",
            "port",
            "remote",
            "username",
            "password",
        )
        self.prepared_fields: tuple[str, ...] = (
            "_host",
            "_port",
            "_username",
            "_password",
        )

        atexit.register(self.disconnect)
        self.__prepared: bool = False
        self.__getting: bool = False
        self.__connected: bool = False
        self.__disconnecting: bool = False
        self.__cached_auth: dict[str, str] = {}
        self.__prepreparation_values: dict[str, Any] = {}

    @classmethod
    def __register_implementation__(cls) -> None:
        if not hasattr(cls, "_protocols"):
            cls._protocols = {}

        cls._protocols[cls.__name__] = cls

        registry_keys = getattr(cls, "PROTOCOLS", []) or []
        if registry_keys:
            for key in registry_keys:
                if (
                    key in cls._protocols
                    and cls.__name__ != cls._protocols[key].__name__
                ):
                    logger.info(
                        f"Ignoring attempt by class `{cls.__name__}` to register "
                        f"key '{key}', which is already registered for class "
                        f"`{cls._protocols[key].__name__}`."
                    )
                else:
                    cls._protocols[key] = cls

    @classmethod
    def for_protocol(cls, protocol: str) -> functools.partial[Duct]:
        """
        Retrieve a `Duct` subclass for a given protocol.

        Args:
            protocol: The protocol of interest.

        Returns:
            The appropriate class for the provided protocol, partially
                constructed with the `protocol` keyword argument set
                appropriately.

        Raises:
            DuctProtocolUnknown: If no class has been defined that offers the
                named protocol.
        """
        if protocol not in cls._protocols:
            raise DuctProtocolUnknown(
                f"Missing `Duct` implementation for protocol: '{protocol}'."
            )
        return functools.partial(cls._protocols[protocol], protocol=protocol)

    @property
    def __prepare_triggers(self) -> tuple[str, ...]:
        return ("cache",) + cast(
            tuple[str, ...], object.__getattribute__(self, "connection_fields")
        )

    @classmethod
    def __init_with_kwargs__(
        cls, self: Duct, kwargs: dict[str, Any], **fallbacks: Any
    ) -> None:
        if not hasattr(self, "_Duct__inited_using_kwargs"):
            self._Duct__inited_using_kwargs = {}
        for cls_parent in reversed(
            [
                parent
                for parent in inspect.getmro(cls)
                if issubclass(parent, Duct)
                and parent not in self._Duct__inited_using_kwargs
                and "__init__" in parent.__dict__
            ]
        ):
            self._Duct__inited_using_kwargs[cls_parent] = True
            argspec = inspect.getfullargspec(cls_parent.__init__)
            keys = argspec.args[1:] + argspec.kwonlyargs
            params = {}
            for key in keys:
                if key in kwargs:
                    params[key] = kwargs.pop(key)
                elif key in fallbacks:
                    params[key] = fallbacks[key]
            cls_parent.__init__(self, **params)

    def __getattribute__(self, key: str) -> Any:
        try:
            if (
                not object.__getattribute__(self, "_Duct__prepared")
                and not object.__getattribute__(self, "_Duct__getting")
                and not object.__getattribute__(self, "_Duct__disconnecting")
                and key in object.__getattribute__(self, "_Duct__prepare_triggers")
            ):
                object.__setattr__(self, "_Duct__getting", True)
                object.__getattribute__(self, "prepare")()
                object.__setattr__(self, "_Duct__getting", False)
        except AttributeError:
            pass
        except:
            object.__setattr__(self, "_Duct__getting", False)
            raise
        return object.__getattribute__(self, key)

    def __setattr__(self, key: str, value: Any) -> None:
        try:
            if (
                object.__getattribute__(self, "_Duct__prepared")
                and object.__getattribute__(self, "connection_fields")
                and key in self.connection_fields
                and self.is_connected()
            ):
                logger.warn(
                    f"Disconnecting prior to changing field that connection is based on: {key}."
                )
                self.disconnect()
                self.__prepared = False
        except AttributeError:
            pass
        object.__setattr__(self, key, value)

    @inherit_docs("_prepare")
    def prepare(self) -> None:
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

    def _prepare(self) -> None:
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
        from omniduct.caches.base import Cache
        from omniduct.registry import DuctRegistry
        from omniduct.remotes.base import RemoteClient

        # Check registry is of an appropriate type (if present)
        if self.registry is not None and not isinstance(self.registry, DuctRegistry):
            raise TypeError(
                "Provided registry is not an instance of `omniduct.registry.DuctRegistry`."
            )

        # If registry is present, lookup remotes and caches if necessary
        if self.registry is not None:
            if self.remote and isinstance(self.remote, str):
                self.__prepreparation_values["remote"] = self.remote
                self.remote = cast(
                    "RemoteClient",
                    self.registry.lookup(self.remote, kind=Duct.Type.REMOTE),
                )
            if self.cache and isinstance(self.cache, str):
                self.__prepreparation_values["cache"] = self.cache
                self.cache = cast(
                    "Cache", self.registry.lookup(self.cache, kind=Duct.Type.CACHE)
                )

        # Check if remote and cache objects are of correct type (if present)
        if self.remote is not None and not isinstance(self.remote, RemoteClient):
            raise TypeError(
                "Provided remote is not an instance of `omniduct.remotes.base.RemoteClient`."
            )
        if self.cache is not None and not isinstance(self.cache, Cache):
            raise TypeError(
                "Provided cache is not an instance of `omniduct.caches.base.Cache`."
            )

        # Replace prepared fields with the result of calling existing values
        # with a reference to `self`.
        for field in self.prepared_fields:
            value = getattr(self, field)
            if hasattr(value, "__call__"):
                self.__prepreparation_values[field] = value
                setattr(self, field, value(self))

        _host_raw = getattr(self, "_host")
        if isinstance(_host_raw, (list, tuple)):
            if "_host" not in self.__prepreparation_values:
                self.__prepreparation_values["_host"] = _host_raw
            self._host = naive_load_balancer(
                cast(list[str], _host_raw), port=cast(int, self._port)
            )

        # If host has a port included in it, override the value of self._port
        if self._host is not None and re.match(r"[^\:]+:[0-9]{1,5}", self._host):
            _host_part, _port_part = self._host.split(":")
            self._host = _host_part
            self._port = int(_port_part)

        # Ensure port is an integer value
        self.port = int(self._port) if self._port else None

    def reset(self) -> Self:
        """
        Reset this `Duct` instance to its pre-preparation state.

        This method disconnects from the service, resets any temporary
        authentication and restores the values of the attributes listed in
        `prepared_fields` to their values as of when `Duct.prepare` was called.

        Returns:
            A reference to this object.
        """
        self.disconnect()
        self.__cached_auth = {}

        for key, value in self.__prepreparation_values.items():
            setattr(self, key, value)
        self.__prepreparation_values = {}
        self.__prepared = False

        return self

    @property
    def host(self) -> str | None:
        """
        The host name providing the service, or '127.0.0.1' if `self.remote` is
        not `None`, whereupon the service will be port-forwarded locally. You can
        view the remote hostname using `duct._host`, and change the remote host
        at runtime using: `duct.host = '<host>'`.
        """
        if self.remote:
            return "127.0.0.1"  # TODO: Make this configurable.
        return self._host

    @host.setter
    def host(self, host: str | None) -> None:
        self._host = host

    @property
    def port(self) -> int | None:
        """
        The local port for the service. If `self.remote` is not `None`, the
        port will be port-forwarded from the remote host. To see the port used on
        the remote host refer to `duct._port`. You can change the remote port
        at runtime using: `duct.port = <port>`.
        """
        if self.remote:
            return cast("RemoteClient", self.remote).port_forward(  # type: ignore[no-any-return]
                f"{self._host}:{self._port}"
            )
        return self._port

    @port.setter
    def port(self, port: int | None) -> None:
        self._port = port

    @property
    def username(self) -> str | None:
        """
        Some services require authentication in order to connect to the
        service, in which case the appropriate username can be specified. If not
        specified at instantiation, your local login name will be used. If `True`
        was provided, you will be prompted to type your username at runtime as
        necessary. If `False` was provided, then `None` will be returned. You can
        specify a different username at runtime using: `duct.username = '<username>'`.
        """
        if self._username is True:
            if "username" not in self.__cached_auth:
                self.__cached_auth["username"] = input(
                    f"Enter username for '{self.name}':"
                )
            return self.__cached_auth["username"]
        if self._username is False:
            return None
        if not self._username:
            try:
                username = os.getlogin()
            except OSError:
                username = pwd.getpwuid(os.geteuid()).pw_name
            return username
        return self._username

    @username.setter
    def username(self, username: str | bool | None) -> None:
        self._username = username

    @property
    def password(self) -> str | None:
        """
        Some services require authentication in order to connect to the
        service, in which case the appropriate password can be specified. If
        `True` was provided at instantiation, you will be prompted to type your
        password at runtime when necessary. If `False` was provided, then
        `None` will be returned. You can specify a different password at runtime
        using: `duct.password = '<password>'`.
        """
        if self._password is True:
            if "password" not in self.__cached_auth:
                self.__cached_auth["password"] = getpass.getpass(
                    f"Enter password for '{self.name}':"
                )
            return self.__cached_auth["password"]
        if self._password is False:
            return None
        return self._password

    @password.setter
    def password(self, password: str | bool | None) -> None:
        self._password = password

    def __assert_server_reachable(self) -> None:
        if self.host is not None or self.port is not None:
            if self.host is None:
                raise ValueError("Port specified but no host provided.")
            if self.port is None:
                raise ValueError("Host specified but no port specified.")
        else:
            return

        host: str = self.host  # narrowed: None case already raised above
        port: int = self.port  # narrowed: None case already raised above
        if not is_port_bound(host, port):
            if self.remote:
                remote = cast("RemoteClient", self.remote)
                if not remote.is_port_bound(self._host, self._port):
                    self.disconnect()
                    raise DuctServerUnreachable(
                        f"Remote '{remote.name}' cannot connect to "
                        f"'{self._host}:{self._port}'. Please check your settings "
                        "before trying again."
                    )
            else:
                self.disconnect()
                raise DuctServerUnreachable(
                    f"Cannot connect to '{host}:{port}' on your current "
                    "connection. Please check your connection before trying again."
                )

    # Connection
    @logging_scope("Connecting")
    @inherit_docs("_connect")
    def connect(self) -> Self:
        """
        Connect to the service backing this client.

        It is not normally necessary for a user to manually call this function,
        since when a connection is required, it is automatically created.

        Returns:
            A reference to the current object.
        """
        if self.host:
            _remote = cast("RemoteClient", self.remote) if self.remote else None
            _via = f" on {_remote.host}" if _remote else ""
            logger.info(f"Connecting to {self._host}:{self._port}{_via}.")
        self.__assert_server_reachable()
        if not self.is_connected():
            try:
                self._connect()
            except:
                self.reset()
                raise
        self.__connected = True
        if self.host:
            _remote = cast("RemoteClient", self.remote) if self.remote else None
            _via = f" on {_remote.host}" if _remote else ""
            logger.info(f"Connected to {self._host}:{self._port}{_via}.")
        return self

    @abstractmethod
    def _connect(self) -> None:
        raise NotImplementedError

    @inherit_docs("_is_connected")
    def is_connected(self) -> bool:
        """
        Check whether this `Duct` instances is currently connected.

        This method checks to see whether a `Duct` instance is currently
        connected. This is performed by verifying that the remote host and port
        are still accessible, and then by calling `Duct._is_connected`, which
        should be implemented by subclasses.

        Returns:
            Whether this `Duct` instance is currently connected.
        """
        if not self.__connected:
            return False

        if self.remote:
            remote = cast("RemoteClient", self.remote)
            if not remote.has_port_forward(self._host, self._port):
                return False
            if not is_port_bound(self.host, self.port):  # type: ignore[arg-type]
                self.disconnect()
                return False

        return self._is_connected()

    @abstractmethod
    def _is_connected(self) -> bool:
        raise NotImplementedError

    @inherit_docs("_disconnect")
    def disconnect(self) -> Self | None:
        """
        Disconnect this client from backing service.

        This method is automatically called during reconnections and/or at
        Python interpreter shutdown. It first calls `Duct._disconnect` (which
        should be implemented by subclasses) and then notifies the
        `RemoteClient` subclass, if present, to stop port-forwarding the remote
        service.

        Returns:
            A reference to this object, or None if the instance was never prepared.
        """
        if not self.__prepared:
            return None
        self.__disconnecting = True
        self.__connected = False

        try:
            self._disconnect()

            if self.remote:
                remote = cast("RemoteClient", self.remote)
                if remote.has_port_forward(self._host, self._port):
                    logger.info(f"Freeing up local port {self.port}...")
                    remote.port_forward_stop(local_port=self.port)
        finally:
            self.__disconnecting = False

        return self

    @abstractmethod
    def _disconnect(self) -> None:
        raise NotImplementedError

    def reconnect(self) -> Self:
        """
        Disconnects, and then reconnects, this client.

        Note: This is equivalent to `duct.disconnect().connect()`.

        Returns:
            A reference to this object.
        """
        self.disconnect()
        return self.connect()  # type: ignore[no-any-return]
