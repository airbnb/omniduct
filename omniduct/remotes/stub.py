from __future__ import annotations

from typing import TYPE_CHECKING, Any

from omniduct.remotes.base import RemoteClient

if TYPE_CHECKING:
    from omniduct.utils.processes import SubprocessResults


class StubFsClient(RemoteClient):
    PROTOCOLS: list[str] = []

    def _init(self) -> None:
        pass

    # Connection management

    def _connect(self) -> None:
        raise NotImplementedError

    def _is_connected(self) -> bool:
        raise NotImplementedError

    def _disconnect(self) -> None:
        raise NotImplementedError

    def _execute(self, cmd: str, **kwargs: Any) -> SubprocessResults:
        raise NotImplementedError

    def _port_forward_start(
        self, local_port: int, remote_host: str, remote_port: int
    ) -> Any:
        raise NotImplementedError

    def _port_forward_stop(
        self, local_port: int, remote_host: str, remote_port: int, connection: Any
    ) -> None:
        raise NotImplementedError

    def _is_port_bound(self, host: str, port: int) -> bool:
        raise NotImplementedError

    # Path properties and helpers

    def _path_home(self) -> str:
        raise NotImplementedError

    def _path_separator(self) -> str:
        raise NotImplementedError

    # File node properties

    def _exists(self, path: str) -> bool:
        raise NotImplementedError

    def _isdir(self, path: str) -> bool:
        raise NotImplementedError

    # Directory handling and enumeration

    def _dir(self, path: str) -> Any:
        raise NotImplementedError

    def _mkdir(self, path: str, recursive: bool, exist_ok: bool) -> None:
        raise NotImplementedError

    # File handling

    # Either re-implement _open, or implement the _file_*_ methods below.
    # def _open(path, mode):
    #     raise NotImplementedError

    def _file_read_(
        self, path: str, size: int = -1, offset: int = 0, binary: bool = False
    ) -> str | bytes:
        raise NotImplementedError

    def _file_write_(self, path: str, s: str | bytes, binary: bool) -> Any:
        raise NotImplementedError

    def _file_append_(self, path: str, s: str | bytes, binary: bool) -> Any:
        raise NotImplementedError
