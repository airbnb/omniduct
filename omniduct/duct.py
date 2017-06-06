import atexit
import functools
import getpass
import inspect
import os
import pwd
from abc import ABCMeta, abstractmethod
from builtins import input
from enum import Enum

import six
from future.utils import raise_with_traceback, with_metaclass

from omniduct.errors import DuctConnectionError, DuctServerUnreachable
from omniduct.utils.debug import logger, logging_scope
from omniduct.utils.dependencies import check_dependencies
from omniduct.utils.ports import is_port_bound


class ProtocolRegisteringABCMeta(ABCMeta):

    def __init__(cls, name, bases, dct):
        ABCMeta.__init__(cls, name, bases, dct)

        if not hasattr(cls, '_protocols'):
            cls._protocols = {}

        registry_keys = getattr(cls, 'PROTOCOLS', []) or []
        if registry_keys:
            for key in registry_keys:
                if key in cls._protocols and cls.__name__ != cls._protocols[key].__name__:
                    logger.info("Ignoring attempt by class `{}` to register key '{}', which is already registered for class `{}`.".format(cls.__name__, key, cls._protocols[key].__name__))
                else:
                    cls._protocols[key] = cls

    def _for_protocol(cls, key):
        return cls._protocols[key]


class Duct(with_metaclass(ProtocolRegisteringABCMeta, object)):

    class Type(Enum):
        REMOTE = 'remotes'
        FILESYSTEM = 'filesystems'
        DATABASE = 'databases'
        CACHE = 'caches'

    AUTO_LOGGING_SCOPE = True
    DUCT_TYPE = None
    PROTOCOLS = None

    def __init__(self, protocol=None, name=None, registry=None, remote=None, host='localhost', port=None, username=None, password=None, cache=None):
        check_dependencies(self.PROTOCOLS)

        self.protocol = protocol
        self.name = name or self.__class__.__name__
        self.registry = registry
        self.remote = remote
        self.host = host
        self.port = int(port) if port else None
        self.__cached_auth = {}
        self.username = username
        self.password = password
        self.cache = cache

        self.connection_fields = ('host', 'port', 'remote', 'cache', 'username', 'password')
        self.prepared_fields = ('_host', '_port', '_username', '_password')

        atexit.register(self.disconnect)
        self.__prepared = False
        self.__getting = False
        self.__disconnecting = False

    def __init_with_kwargs__(self, kwargs, **fallbacks):
        keys = inspect.getargspec(Duct.__init__).args[1:]
        params = {}
        for key in keys:
            if key in kwargs:
                params[key] = kwargs.pop(key)
            elif key in fallbacks:
                params[key] = fallbacks[key]
        Duct.__init__(self, **params)

    @classmethod
    def for_protocol(cls, key):
        return functools.partial(cls._for_protocol(key), protocol=key)

    def __getattribute__(self, key):
        try:
            if (not object.__getattribute__(self, '_Duct__prepared')
                    and not object.__getattribute__(self, '_Duct__getting')
                    and not object.__getattribute__(self, '_Duct__disconnecting')
                    and key in object.__getattribute__(self, 'connection_fields')):
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

    def prepare(self):
        if not self.__prepared:
            self._prepare()
            self.__prepared = True

    def _prepare(self):
        if self.remote and isinstance(self.remote, six.string_types):
            self.remote = self.registry.lookup(self.remote, kind=Duct.Type.REMOTE)
        if self.cache and isinstance(self.cache, six.string_types):
            self.cache = self.registry.lookup(self.cache, kind=Duct.Type.CACHE)
        for field in self.prepared_fields:
            value = getattr(self, field)
            if hasattr(value, '__call__'):
                setattr(self, field, value(self.registry))

    @property
    def host(self):
        if self.remote:
            return '127.0.0.1'
        return self._host

    @host.setter
    def host(self, host):
        self._host = host

    @property
    def port(self):
        if self.remote:
            return self.remote.port_forward('{}:{}'.format(self._host, self._port))
        return self._port

    @port.setter
    def port(self, port):
        self._port = port

    @property
    def username(self):
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
    def connect(self):
        """
        If a connection to the filesystem does not already exist, calling
        this method creates it.

        NOTE: It is not normally necessary for a user to manually call this function,
        since when a connection is required, it is automatically made.
        """
        if self.remote:
            logger.info("Connecting to {}:{} on {}.".format(self._host, self._port, self.remote.host))
        else:
            logger.info("Connecting to {}:{}.".format(self.host, self.port))
        self.__assert_server_reachable()
        if not self.is_connected():
            try:
                self._connect()
            except Exception as e:
                self.reset()
                raise_with_traceback(e)
        if self.remote:
            logger.info("Connected to {}:{} on {}.".format(self._host, self._port, self.remote.host))
        else:
            logger.info("Connected to {}:{}.".format(self.host, self.port))
        return self

    def is_connected(self):
        """
        Return `True` if the filesystem client is connected to the data source, and
        `False` otherwise.
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

    def disconnect(self):
        """
        Disconnects this client
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

    def reconnect(self):
        """
        Disconnects, and then reconnects, this client
        """
        self.disconnect()
        self.connect()

    def reset(self):
        self.disconnect()
        self.__cached_auth = {}

    @abstractmethod
    def _connect(self):
        raise NotImplementedError

    @abstractmethod
    def _is_connected(self):
        raise NotImplementedError

    @abstractmethod
    def _disconnect(self):
        raise NotImplementedError
