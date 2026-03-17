from __future__ import annotations

import getpass
import re
from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from interface_meta import inherit_docs, override

from omniduct.duct import Duct
from omniduct.errors import DuctAuthenticationError, DuctServerUnreachable
from omniduct.filesystems.base import FileSystemClient
from omniduct.utils.decorators import require_connection
from omniduct.utils.ports import get_free_local_port, is_local_port_free

if TYPE_CHECKING:
    from omniduct.utils.processes import SubprocessResults

from urllib.parse import urlparse, urlunparse


class PortForwardingRegister:
    """
    A register of all port forwards initiated by a particular Duct.
    """

    def __init__(self) -> None:
        self._register: dict[str, tuple[int, Any]] = {}

    def lookup(self, remote_host: str, remote_port: int) -> tuple[int, Any] | None:
        """
        Look up a previously forwarded remote port.

        Args:
            remote_host: The remote host.
            remote_port: The remote port.

        Returns:
            A tuple of local port and implementation-specific
                connection artifact, if it exists, and `None` otherwise.
        """
        return self._register.get(f"{remote_host}:{remote_port}")

    def lookup_port(self, remote_host: str, remote_port: int) -> int | None:
        """
        Look up the local port bound to a remote port.

        Args:
            remote_host: The remote host.
            remote_port: The remote port.

        Returns:
            The local port use in the port forward, or `None` if
                port forward does not exist.
        """
        entry = self.lookup(remote_host, remote_port)
        if entry is not None:
            return entry[0]
        return None

    def reverse_lookup(self, local_port: int) -> tuple[str, int, Any] | None:
        """
        Look up a remote host / port associated with a local port.

        Args:
            local_port: The local port.

        Returns:
            A tuple of remote hostname, remote port, and
                implementation-specific connection artifact.
        """
        for key, (port, connection) in self._register.items():
            if port == local_port:
                host, port_str = key.rsplit(":", 1)
                return (host, int(port_str), connection)
        return None

    def register(
        self,
        remote_host: str,
        remote_port: int,
        local_port: int,
        connection: Any,
    ) -> None:
        """
        Register a port-forward connection.

        Args:
            remote_host: The remote host.
            remote_port: The remote port.
            local_port: The local port.
            connection: Implementation-specific connection artifact.
        """
        key = f"{remote_host}:{remote_port}"
        if key in self._register:
            raise RuntimeError(
                f"Remote host/port combination ({key}) is already registered."
            )
        self._register[key] = (local_port, connection)

    def deregister(self, remote_host: str, remote_port: int) -> tuple[int, Any]:
        """
        Deregister a port-forward connection.

        Args:
            remote_host: The remote host.
            remote_port: The remote port.

        Returns:
            A tuple of local port and implementation-specific
                connection artifact, if it exists, and `None` otherwise.
        """
        return self._register.pop(f"{remote_host}:{remote_port}")


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
    DEFAULT_PORT: int | None = None

    smartcards: dict[str, str] | None

    @inherit_docs("_init", mro=True)
    def __init__(self, smartcards: dict[str, str] | None = None, **kwargs: Any) -> None:
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
    def _init(self) -> None:
        raise NotImplementedError

    # SSH commands
    @override
    def connect(self) -> RemoteClient:
        """
        Connect to the remote server.

        It is not normally necessary for a user to manually call this function,
        since when a connection is required, it is automatically created.

        Compared to base `Duct.connect`, this method will automatically catch
        the first `DuctAuthenticationError` error triggered by `Duct.connect`,
        and (if smartcards have been configured) attempt to re-initialise the
        smartcards before trying once more.

        Returns:
            A reference to the current object.
        """
        try:
            Duct.connect(self)
        except DuctAuthenticationError:
            if self.smartcards and self.prepare_smartcards():
                Duct.connect(self)
            raise
        return self

    def prepare_smartcards(self) -> bool:
        """
        Prepare smartcards for use in authentication.

        This method checks attempts to ensure that the each of the nominated
        smartcards is available and prepared for use. This may result in
        interactive requests for pin confirmation, depending on the card.

        Returns:
            Returns `True` if at least one smartcard was activated, and
                `False` otherwise.
        """

        smartcard_added = False

        for name, filename in (self.smartcards or {}).items():
            smartcard_added |= self._prepare_smartcard(name, filename)

        return smartcard_added

    def _prepare_smartcard(self, name: str, filename: str) -> bool:
        import pexpect

        remover = pexpect.spawn(f'ssh-add -e "{filename}"')
        i = remover.expect(["Card removed:", "Could not remove card", pexpect.TIMEOUT])
        if i == 2:
            raise RuntimeError(
                f"Unable to reset card using ssh-agent. Output of ssh-agent was: \n{remover.before}\n\nPlease report this error!"
            )

        adder = pexpect.spawn(f'ssh-add -s "{filename}" -t 14400')
        i = adder.expect(["Enter passphrase for PKCS#11:", pexpect.TIMEOUT])
        if i == 0:
            adder.sendline(
                getpass.getpass(
                    f'Please enter your passcode to unlock your "{name}" smartcard: '
                )
            )
        else:
            raise RuntimeError(
                f"Unable to add card using ssh-agent. Output of ssh-agent was: \n{remover.before}\n\nPlease report this error!"
            )
        i = adder.expect(["Card added:", pexpect.TIMEOUT])
        if i != 0:
            raise RuntimeError(
                "Unexpected error while adding card. Check your passcode and try again."
            )

        return True

    @inherit_docs("_execute")
    @require_connection
    def execute(self, cmd: str, **kwargs: Any) -> SubprocessResults:
        """
        Execute a command on the remote server.

        Args:
            cmd: The command to run on the remote associated with this
                instance.
            **kwargs: Additional keyword arguments to be passed on to
                `._execute`.

        Returns:
            The result of the execution.
        """
        return self._execute(cmd, **kwargs)

    @abstractmethod
    def _execute(self, cmd: str, **kwargs: Any) -> SubprocessResults:
        raise NotImplementedError

    # Port forwarding code

    def _extract_host_and_ports(
        self,
        remote_host: str | None,
        remote_port: int | None,
        local_port: int | None,
    ) -> tuple[str | None, int | None, int | None]:
        if remote_host is not None and not isinstance(remote_host, str):
            raise TypeError(
                "Remote host, if specified, must be a string of form 'hostname(:port)'."
            )
        if remote_port is not None and not isinstance(remote_port, int):
            raise TypeError("Remote port, if specified, must be an integer.")
        if local_port is not None and not isinstance(local_port, int):
            raise TypeError("Local port, if specified, must be an integer.")

        host: str | None = None
        port: int | None = None
        if remote_host is not None:
            m = re.match(
                r"(?P<host>[a-zA-Z0-9\-.]+)(?::(?P<port>[0-9]+))?", remote_host
            )
            if not m:
                raise ValueError(
                    f"Host not valid: {remote_host}. Must be a string of form 'hostname(:port)'."
                )

            host = m.group("host")
            port_str = m.group("port")
            port = int(port_str) if port_str is not None else remote_port
        return host, port, local_port

    @inherit_docs("_port_forward_start")
    @require_connection
    def port_forward(
        self,
        remote_host: str,
        remote_port: int | None = None,
        local_port: int | None = None,
    ) -> int:
        """
        Initiate a port forward connection.

        This method establishes a local port forwarding from a local port `local`
        to remote port `remote`. If `local` is `None`, an available local port is
        automatically chosen. If the remote port is already forwarded, a new
        connection is not established.

        Args:
            remote_host: The hostname of the remote host in form:
                'hostname(:port)'.
            remote_port: The remote port of the service.
            local_port: The port to use locally (automatically
                determined if not specified).

        Returns:
            The local port which is port forwarded to the remote service.
        """

        # Hostname and port extraction
        remote_host, remote_port, local_port = self._extract_host_and_ports(  # type: ignore[assignment]
            remote_host, remote_port, local_port
        )
        if remote_host is None:
            raise ValueError("Remote host must be specified.")
        if remote_port is None:
            raise ValueError("Remote port must be specified.")

        # Actual port forwarding
        registered_port = self.__port_forwarding_register.lookup_port(
            remote_host, remote_port
        )
        if registered_port is not None:
            if local_port is not None and registered_port != local_port:
                self.port_forward_stop(registered_port)
            else:
                return registered_port

        if local_port is None:
            local_port = get_free_local_port()
        elif not is_local_port_free(local_port):
            raise RuntimeError("Specified local port is in use, and cannot be used.")

        if not self.is_port_bound(remote_host, remote_port):
            raise DuctServerUnreachable(
                f"Server specified for port forwarding ({remote_host}:{remote_port}) is unreachable via '{self.name}' ({self.__class__.__name__})."
            )
        connection = self._port_forward_start(local_port, remote_host, remote_port)
        self.__port_forwarding_register.register(
            remote_host, remote_port, local_port, connection
        )

        return local_port

    def has_port_forward(
        self,
        remote_host: str | None = None,
        remote_port: int | None = None,
        local_port: int | None = None,
    ) -> bool:
        """
        Check whether a port forward connection exists.

        Args:
            remote_host: The hostname of the remote host in form:
                'hostname(:port)'.
            remote_port: The remote port of the service.
            local_port: The port used locally.

        Returns:
            Whether a port-forward for this remote service exists, or if
                local port is specified, whether that port is locally used for
                port forwarding.
        """
        # Hostname and port extraction
        remote_host, remote_port, local_port = self._extract_host_and_ports(
            remote_host, remote_port, local_port
        )

        if not (
            remote_host is not None
            and remote_port is not None
            or local_port is not None
        ):
            raise ValueError(
                "Either remote host and port must be specified, or the local port must be specified."
            )

        if remote_host is not None and remote_port is not None:
            return (
                self.__port_forwarding_register.lookup(remote_host, remote_port)
                is not None
            )
        return self.__port_forwarding_register.reverse_lookup(local_port) is not None  # type: ignore[arg-type]

    @inherit_docs("_port_forward_stop")
    def port_forward_stop(
        self,
        local_port: int | None = None,
        remote_host: str | None = None,
        remote_port: int | None = None,
    ) -> None:
        """
        Disconnect an existing port forward connection.

        If a local port is provided, then the forwarding (if any) associated
        with that port is found and stopped; otherwise any established port
        forwarding associated with the nominated remote service is stopped.

        Args:
            remote_host: The hostname of the remote host in form:
                'hostname(:port)'.
            remote_port: The remote port of the service.
            local_port: The port used locally.
        """
        # Hostname and port extraction
        remote_host, remote_port, local_port = self._extract_host_and_ports(
            remote_host, remote_port, local_port
        )

        if not (
            remote_host is not None
            and remote_port is not None
            or local_port is not None
        ):
            raise ValueError(
                "Either remote host and port must be specified, or the local port must be specified."
            )

        if remote_host is not None and remote_port is not None:
            entry = self.__port_forwarding_register.lookup(remote_host, remote_port)
            if entry is None:
                raise RuntimeError(
                    f"No port forwarding registered for {remote_host}:{remote_port}."
                )
            local_port, connection = entry
        else:
            if local_port is None:
                raise RuntimeError("Local port must be specified.")
            rev = self.__port_forwarding_register.reverse_lookup(local_port)
            if rev is None:
                raise RuntimeError(
                    f"No port forwarding registered for local port {local_port}."
                )
            remote_host, remote_port, connection = rev

        if remote_host is None or remote_port is None:
            raise RuntimeError("remote_host and remote_port could not be determined.")
        self._port_forward_stop(local_port, remote_host, remote_port, connection)
        self.__port_forwarding_register.deregister(remote_host, remote_port)

    def port_forward_stopall(self) -> None:
        """
        Disconnect all existing port forwarding connections.
        """
        for remote_host in self.__port_forwarding_register._register.copy():
            self.port_forward_stop(remote_host=remote_host)

    def get_local_uri(self, uri: str) -> str:
        """
        Convert a remote uri to a local one.

        This method takes a remote service uri accessible to the remote host and
        returns a local uri accessible directly on the local host, establishing
        any necessary port forwarding in the process.

        Args:
            uri: The remote uri to be made local.

        Returns:
            A local uri that tunnels all traffic to the remote host.
        """
        parsed_uri = urlparse(uri)
        return urlunparse(
            parsed_uri._replace(
                netloc=f"localhost:{self.port_forward(parsed_uri.netloc)}"
            )
        )

    def show_port_forwards(self) -> None:
        """
        Print to stdout the active port forwards associated with this client.
        """
        if len(self.__port_forwarding_register._register) == 0:
            print("No port forwards currently in use.")
        for remote_host, (
            local_port,
            _,
        ) in self.__port_forwarding_register._register.items():
            print(
                f"localhost:{local_port}",
                "->",
                remote_host,
                f"(on {self._host})",
            )

    @abstractmethod
    def _port_forward_start(
        self, local_port: int, remote_host: str, remote_port: int
    ) -> Any:
        raise NotImplementedError

    @abstractmethod
    def _port_forward_stop(
        self,
        local_port: int,
        remote_host: str,
        remote_port: int,
        connection: Any,
    ) -> None:
        raise NotImplementedError

    @inherit_docs("_is_port_bound")
    @require_connection
    def is_port_bound(self, host: str, port: int) -> bool:
        """
        Check whether a port on a remote host is accessible.

        This method checks to see whether a particular port is active on a
        given host by attempting to establish a connection with it.

        Args:
            host: The hostname of the target service.
            port: The port of the target service.

        Returns:
            Whether the port is active and accepting connections.
        """
        return self._is_port_bound(host, port)

    @abstractmethod
    def _is_port_bound(self, host: str, port: int) -> bool:
        pass
