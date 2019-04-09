import getpass
import re
from abc import abstractmethod

import six
from interface_meta import quirk_docs, override
from future.utils import raise_with_traceback

from omniduct.duct import Duct
from omniduct.errors import DuctAuthenticationError, DuctServerUnreachable
from omniduct.filesystems.base import FileSystemClient
from omniduct.utils.decorators import require_connection
from omniduct.utils.ports import get_free_local_port, is_local_port_free

try:  # Python 3
    from urllib.parse import urlparse, urlunparse
except ImportError:  # Python 2
    from urlparse import urlparse, urlunparse


class PortForwardingRegister(object):
    """
    A register of all port forwards initiated by a particular Duct.
    """

    def __init__(self):
        self._register = {}

    def lookup(self, remote_host, remote_port):
        """
        Look up a previously forwarded remote port.

        Args:
            remote_host (str): The remote host.
            remote_port (int): The remote port.

        Returns:
            tuple, None: A tuple of local port and implementation-specific
                connection artifact, if it exists, and `None` otherwise.
        """
        return self._register.get('{}:{}'.format(remote_host, remote_port))

    def lookup_port(self, remote_host, remote_port):
        """
        Look up the local port bound to a remote port.

        Args:
            remote_host (str): The remote host.
            remote_port (int): The remote port.

        Returns:
            int, None: The local port use in the port forward, or `None` if
                port forward does not exist.
        """
        entry = self.lookup(remote_host, remote_port)
        if entry is not None:
            return entry[0]

    def reverse_lookup(self, local_port):
        """
        Look up a remote host / port associated with a local port.

        Args:
            local_port (int): The local port.

        Returns:
            list: A list of remote hostname, remote port, and
                implementation-specific connection artifact.
        """
        for key, (port, connection) in self._register.items():
            if port == local_port:
                return key.split(':') + [connection]
        return None

    def register(self, remote_host, remote_port, local_port, connection):
        """
        Register a port-forward connection.

        Args:
            remote_host (str): The remote host.
            remote_port (int): The remote port.
            local_port (int): The local port.
            connection (object): Implementation-specific connection artifact.
        """
        key = '{}:{}'.format(remote_host, remote_port)
        if key in self._register:
            raise RuntimeError("Remote host/port combination ({}) is already registered.".format(key))
        self._register[key] = (local_port, connection)

    def deregister(self, remote_host, remote_port):
        """
        Deregister a port-forward connection.

        Args:
            remote_host (str): The remote host.
            remote_port (int): The remote port.

        Returns:
            tuple: A tuple of local port and implementation-specific
                connection artifact, if it exists, and `None` otherwise.
        """
        return self._register.pop('{}:{}'.format(remote_host, remote_port))


class RemoteClient(FileSystemClient):
    """
    An abstract class providing the common API for all remote clients.

    Attributes:
        smartcard (dict): Mapping of smartcard names to system libraries
            compatible with `ssh-add -s '<system library>' ...`.
    """
    __doc_attrs = """
    smartcard (dict): Mapping of smartcard names to system libraries
        compatible with `ssh-add -s '<system library>' ...`.
    """

    DUCT_TYPE = Duct.Type.REMOTE
    DEFAULT_PORT = None

    @quirk_docs('_init', mro=True)
    def __init__(self, smartcards=None, **kwargs):
        """
        Args:
            smartcards (dict): Mapping of smartcard names to system libraries
                compatible with `ssh-add -s '<system library>' ...`.
        """

        self.smartcards = smartcards
        self.__port_forwarding_register = PortForwardingRegister()

        FileSystemClient.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)

        # Note: self._init is called by FileSystemClient constructor.

    @override
    @abstractmethod
    def _init(self):
        raise NotImplementedError

    # SSH commands
    @override
    def connect(self):
        """
        Connect to the remote server.

        It is not normally necessary for a user to manually call this function,
        since when a connection is required, it is automatically created.

        Compared to base `Duct.connect`, this method will automatically catch
        the first `DuctAuthenticationError` error triggered by `Duct.connect`,
        and (if smartcards have been configured) attempt to re-initialise the
        smartcards before trying once more.

        Returns:
            `Duct` instance: A reference to the current object.
        """
        try:
            Duct.connect(self)
        except DuctServerUnreachable as e:
            raise_with_traceback(e)
        except DuctAuthenticationError as e:
            if self.smartcards and self.prepare_smartcards():
                Duct.connect(self)
            else:
                raise_with_traceback(e)
        return self

    def prepare_smartcards(self):
        """
        Prepare smartcards for use in authentication.

        This method checks attempts to ensure that the each of the nominated
        smartcards is available and prepared for use. This may result in
        interactive requests for pin confirmation, depending on the card.

        Returns:
            bool: Returns `True` if at least one smartcard was activated, and
                `False` otherwise.
        """

        smartcard_added = False

        for name, filename in self.smartcards.items():
            smartcard_added |= self._prepare_smartcard(name, filename)

        return smartcard_added

    def _prepare_smartcard(self, name, filename):
        import pexpect

        remover = pexpect.spawn('ssh-add -e "{}"'.format(filename))
        i = remover.expect(["Card removed:", "Could not remove card", pexpect.TIMEOUT])
        if i == 2:
            raise RuntimeError("Unable to reset card using ssh-agent. Output of ssh-agent was: \n{}\n\n"
                               "Please report this error!".format(remover.before))

        adder = pexpect.spawn('ssh-add -s "{}" -t 14400'.format(filename))
        i = adder.expect(['Enter passphrase for PKCS#11:', pexpect.TIMEOUT])
        if i == 0:
            adder.sendline(getpass.getpass('Please enter your passcode to unlock your "{}" smartcard: '.format(name)))
        else:
            raise RuntimeError("Unable to add card using ssh-agent. Output of ssh-agent was: \n{}\n\n"
                               "Please report this error!".format(remover.before))
        i = adder.expect(['Card added:', pexpect.TIMEOUT])
        if i != 0:
            raise RuntimeError("Unexpected error while adding card. Check your passcode and try again.")

        return True

    @quirk_docs('_execute')
    @require_connection
    def execute(self, cmd, **kwargs):
        """
        Execute a command on the remote server.

        Args:
            cmd (str): The command to run on the remote associated with this
                instance.
            **kwargs (dict): Additional keyword arguments to be passed on to
                `._execute`.

        Returns:
            SubprocessResults: The result of the execution.
        """
        return self._execute(cmd, **kwargs)

    @abstractmethod
    def _execute(self, cmd, **kwargs):
        raise NotImplementedError

    # Port forwarding code

    def _extract_host_and_ports(self, remote_host, remote_port, local_port):
        assert remote_host is None or isinstance(remote_host, six.string_types), "Remote host, if specified, must be a string of form 'hostname(:port)'."
        assert remote_port is None or isinstance(remote_port, int), "Remote port, if specified, must be an integer."
        assert local_port is None or isinstance(local_port, int), "Local port, if specified, must be an integer."

        host = port = None
        if remote_host is not None:
            m = re.match(r'(?P<host>[a-zA-Z0-9\-.]+)(?::(?P<port>[0-9]+))?', remote_host)
            assert m, "Host not valid: {}. Must be a string of form 'hostname(:port)'.".format(remote_host)

            host = m.group('host')
            port = m.group('port') or remote_port
        return host, port, local_port

    @quirk_docs('_port_forward_start')
    @require_connection
    def port_forward(self, remote_host, remote_port=None, local_port=None):
        """
        Initiate a port forward connection.

        This method establishes a local port forwarding from a local port `local`
        to remote port `remote`. If `local` is `None`, an available local port is
        automatically chosen. If the remote port is already forwarded, a new
        connection is not established.

        Args:
            remote_host (str): The hostname of the remote host in form:
                'hostname(:port)'.
            remote_port (int, None): The remote port of the service.
            local_port (int, None): The port to use locally (automatically
                determined if not specified).

        Returns:
            int: The local port which is port forwarded to the remote service.
        """

        # Hostname and port extraction
        remote_host, remote_port, local_port = self._extract_host_and_ports(remote_host, remote_port, local_port)
        assert remote_host is not None, "Remote host must be specified."
        assert remote_port is not None, "Remote port must be specified."

        # Actual port forwarding
        registered_port = self.__port_forwarding_register.lookup_port(remote_host, remote_port)
        if registered_port is not None:
            if local_port is not None and registered_port != local_port:
                self.port_forward_stop(registered_port)
            else:
                return registered_port

        if local_port is None:
            local_port = get_free_local_port()
        else:
            assert is_local_port_free(local_port), "Specified local port is in use, and cannot be used."

        if not self.is_port_bound(remote_host, remote_port):
            raise DuctServerUnreachable("Server specified for port forwarding ({}:{}) is unreachable via '{}' ({}).".format(remote_host, remote_port, self.name, self.__class__.__name__))
        connection = self._port_forward_start(local_port, remote_host, remote_port)
        self.__port_forwarding_register.register(remote_host, remote_port, local_port, connection)

        return local_port

    def has_port_forward(self, remote_host=None, remote_port=None, local_port=None):
        """
        Check whether a port forward connection exists.

        Args:
            remote_host (str): The hostname of the remote host in form:
                'hostname(:port)'.
            remote_port (int, None): The remote port of the service.
            local_port (int, None): The port used locally.

        Returns:
            bool: Whether a port-forward for this remote service exists, or if
                local port is specified, whether that port is locally used for
                port forwarding.
        """
        # Hostname and port extraction
        remote_host, remote_port, local_port = self._extract_host_and_ports(remote_host, remote_port, local_port)

        assert remote_host is not None and remote_port is not None or local_port is not None, "Either remote host and port must be specified, or the local port must be specified."

        if remote_host is not None and remote_port is not None:
            return self.__port_forwarding_register.lookup(remote_host, remote_port) is not None
        else:
            return self.__port_forwarding_register.reverse_lookup(local_port) is not None

    @quirk_docs('_port_forward_stop')
    def port_forward_stop(self, local_port=None, remote_host=None, remote_port=None):
        """
        Disconnect an existing port forward connection.

        If a local port is provided, then the forwarding (if any) associated
        with that port is found and stopped; otherwise any established port
        forwarding associated with the nominated remote service is stopped.

        Args:
            remote_host (str): The hostname of the remote host in form:
                'hostname(:port)'.
            remote_port (int, None): The remote port of the service.
            local_port (int, None): The port used locally.
        """
        # Hostname and port extraction
        remote_host, remote_port, local_port = self._extract_host_and_ports(remote_host, remote_port, local_port)

        assert remote_host is not None and remote_port is not None or local_port is not None, "Either remote host and port must be specified, or the local port must be specified."

        if remote_host is not None and remote_port is not None:
            local_port, connection = self.__port_forwarding_register.lookup(remote_host, remote_port)
        else:
            remote_host, remote_port, connection = self.__port_forwarding_register.reverse_lookup(local_port)

        self._port_forward_stop(local_port, remote_host, remote_port, connection)
        self.__port_forwarding_register.deregister(remote_host, remote_port)

    def port_forward_stopall(self):
        """
        Disconnect all existing port forwarding connections.
        """
        for remote_host in self.__port_forwarding_register._register.copy():
            self.port_forward_stop(remote_host=remote_host)

    def get_local_uri(self, uri):
        """
        Convert a remote uri to a local one.

        This method takes a remote service uri accessible to the remote host and
        returns a local uri accessible directly on the local host, establishing
        any necessary port forwarding in the process.

        Args:
            uri (str): The remote uri to be made local.

        Returns:
            str: A local uri that tunnels all traffic to the remote host.
        """
        parsed_uri = urlparse(uri)
        return urlunparse(parsed_uri._replace(netloc='localhost:{}'.format(self.port_forward(parsed_uri.netloc))))

    def show_port_forwards(self):
        """
        Print to stdout the active port forwards associated with this client.
        """
        if len(self.__port_forwarding_register._register) == 0:
            print("No port forwards currently in use.")
        for remote_host, (local_port, _) in self.__port_forwarding_register._register.items():
            print("localhost:{}".format(local_port), "->", remote_host, "(on {})".format(self._host))

    @abstractmethod
    def _port_forward_start(self, local_port, remote_host, remote_port):
        raise NotImplementedError

    @abstractmethod
    def _port_forward_stop(self, local_port, remote_host, remote_port, connection):
        raise NotImplementedError

    @quirk_docs('_is_port_bound')
    @require_connection
    def is_port_bound(self, host, port):
        """
        Check whether a port on a remote host is accessible.

        This method checks to see whether a particular port is active on a
        given host by attempting to establish a connection with it.

        Args:
            host (str): The hostname of the target service.
            port (int): The port of the target service.

        Returns:
            bool: Whether the port is active and accepting connections.
        """
        return self._is_port_bound(host, port)

    @abstractmethod
    def _is_port_bound(self, host, port):
        pass
