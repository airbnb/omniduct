from __future__ import annotations

from collections.abc import Generator
from typing import Any

from omniduct.filesystems.base import FileSystemClient, FileSystemFileDesc


class StubFsClient(FileSystemClient):
    PROTOCOLS: list[str] = []
    DEFAULT_PORT: int | None = None

    def _init(self) -> None:
        pass

    # Connection management

    def _connect(self) -> None:
        raise NotImplementedError

    def _is_connected(self) -> bool:
        raise NotImplementedError

    def _disconnect(self) -> None:
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

    def _dir(self, path: str) -> Generator[FileSystemFileDesc, None, None]:
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
