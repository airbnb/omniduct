from __future__ import annotations

import builtins
import datetime
import errno
import os
import shutil
from collections.abc import Generator
from typing import IO, Any

from interface_meta import override

from .base import FileSystemClient, FileSystemFileDesc


class LocalFsClient(FileSystemClient):
    """
    `LocalFsClient` is a `Duct` that implements the `FileSystemClient` common
    API, and exposes the local filesystem.

    Unlike most other filesystems, `LocalFsClient` defaults to the current
    working directory on the local machine, rather than the home directory
    as used on remote filesystems. To change this, you can always execute:
    ```
    local_fs.path_cwd = local_fs.path_home
    ```
    """

    PROTOCOLS = ["localfs"]

    @override
    def _init(self) -> None:
        self._path_cwd = self._path_cwd or os.getcwd()

    @override
    def _prepare(self) -> None:
        if self.remote is not None:
            raise ValueError(
                "LocalFsClient cannot be used in conjunction with a remote client."
            )
        super()._prepare()

    @override
    def _connect(self) -> None:
        pass

    @override
    def _is_connected(self) -> bool:
        return True

    @override
    def _disconnect(self) -> None:
        pass

    # File enumeration
    @override
    def _path_home(self) -> str:
        return os.path.expanduser("~")

    @override
    def _path_separator(self) -> str:
        return os.path.sep

    @override
    def _exists(self, path: str) -> bool:
        return os.path.exists(path)

    @override
    def _isdir(self, path: str) -> bool:
        return os.path.isdir(path)

    @override
    def _isfile(self, path: str) -> bool:
        return os.path.isfile(path)

    @override
    def _dir(self, path: str) -> Generator[FileSystemFileDesc, None, None]:
        if not os.path.isdir(path):
            raise RuntimeError("No such folder.")
        for f in os.listdir(path):
            f_path = os.path.join(path, f)

            attrs: dict[str, Any] = {}

            if os.name == "posix":
                import grp
                import pwd

                stat = os.stat(f_path)

                attrs.update(
                    {
                        "owner": pwd.getpwuid(stat.st_uid).pw_name,
                        "group": grp.getgrgid(stat.st_gid).gr_name,
                        "permissions": oct(stat.st_mode),
                        "created": str(datetime.datetime.fromtimestamp(stat.st_ctime)),
                        "last_modified": str(
                            datetime.datetime.fromtimestamp(stat.st_mtime)
                        ),
                        "last_accessed": str(
                            datetime.datetime.fromtimestamp(stat.st_atime)
                        ),
                    }
                )

            yield FileSystemFileDesc(
                fs=self,
                path=f_path,
                name=f,
                type="directory" if os.path.isdir(f_path) else "file",
                bytes=os.path.getsize(f_path),
                **attrs,
            )

    @override
    def _walk(
        self, path: str
    ) -> Generator[tuple[str, list[str], list[str]], None, None]:
        yield from os.walk(path)

    @override
    def _mkdir(self, path: str, recursive: bool, exist_ok: bool) -> None:
        try:
            os.makedirs(path) if recursive else os.mkdir(path)
        except OSError as exc:  # Python >2.5
            if exc.errno != errno.EEXIST or not exist_ok or not os.path.isdir(path):
                raise

    @override
    def _remove(self, path: str, recursive: bool) -> None:
        if recursive and self.isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)

    # File opening
    @override
    def _open(self, path: str, mode: str) -> IO[Any]:
        return builtins.open(path, mode=mode, encoding=None if "b" in mode else "utf-8")
