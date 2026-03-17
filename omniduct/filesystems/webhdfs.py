from __future__ import annotations

import posixpath
import random
from collections.abc import Callable, Generator
from functools import partial
from typing import Any

from interface_meta import override

from omniduct.remotes.base import RemoteClient

from .base import FileSystemClient, FileSystemFileDesc
from .local import LocalFsClient


class WebHdfsClient(FileSystemClient):
    """
    This Duct connects to an Apache WebHDFS server using the `pywebhdfs` library.

    Attributes:
        namenodes: A list of hosts that are acting as namenodes for
            the HDFS cluster in form "<hostname>:<port>".
    """

    PROTOCOLS = ["webhdfs"]
    DEFAULT_PORT = 50070

    namenodes: list[str] | None

    @override
    def _init(
        self,
        namenodes: list[str] | None = None,
        auto_conf: bool = False,
        auto_conf_cluster: str | None = None,
        auto_conf_path: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        namenodes: A list of hosts that are acting as namenodes for
            the HDFS cluster in form "<hostname>:<port>".
        auto_conf: Whether to automatically extract host, port and
            namenode information from Cloudera configuration files. If True,
            automatically extracted values will override other passed values.
        auto_conf_cluster: The name of the cluster for which to extract
            configuration.
        auto_conf_path: The path of the `hdfs-site.xml` file in which
            the HDFS configuration is stored (on the remote filesystem if
            `remote` is specified, and on the local filesystem otherwise).
            Defaults to '/etc/hadoop/conf.cloudera.hdfs2/hdfs-site.xml'.
        **kwargs: Additional arguments to pass onto the WebHdfs client.
        """
        self.namenodes = namenodes

        if auto_conf:
            from ._webhdfs_helpers import CdhHdfsConfParser

            if auto_conf_cluster is None:
                raise ValueError(
                    "You must specify a cluster via `auto_conf_cluster` for auto-detection to work."
                )

            def get_host_and_set_namenodes(
                duct: WebHdfsClient, cluster: str, conf_path: str | None
            ) -> str:
                conf_parser = CdhHdfsConfParser(
                    duct.remote
                    if isinstance(duct.remote, FileSystemClient)
                    else LocalFsClient(),
                    conf_path=conf_path,
                )
                duct.namenodes = conf_parser.namenodes(cluster)
                return random.choice(duct.namenodes)  # noqa: S311

            self._host: str | Callable[..., str] = partial(  # type: ignore[assignment]
                get_host_and_set_namenodes,
                cluster=auto_conf_cluster,
                conf_path=auto_conf_path,
            )
        elif not self._host and namenodes:
            self._host = random.choice(self.namenodes or [])  # noqa: S311

        self.__webhdfs: Any = None
        self.__webhdfs_kwargs: dict[str, Any] = kwargs
        self.prepared_fields += ("namenodes",)

    @override
    def _connect(self) -> None:
        from ._webhdfs_helpers import OmniductPyWebHdfsClient

        self.__webhdfs = OmniductPyWebHdfsClient(
            host=self._host,
            port=self._port,
            remote=self.remote,
            namenodes=self.namenodes,
            user_name=self.username,
            **self.__webhdfs_kwargs,
        )

    @override
    def _is_connected(self) -> bool:
        try:
            if isinstance(self.remote, RemoteClient) and not self.remote.is_connected():
                return False
            return self.__webhdfs is not None
        except:
            return False

    @override
    def _disconnect(self) -> None:
        self.__webhdfs = None

    # Path properties and helpers
    @override
    def _path_home(self) -> str:
        return self.__webhdfs.get_home_directory()  # type: ignore[no-any-return]

    @override
    def _path_separator(self) -> str:
        return "/"

    # File node properties
    @override
    def _exists(self, path: str) -> bool:
        from pywebhdfs.errors import FileNotFound

        try:
            self.__webhdfs.get_file_dir_status(path)
            return True
        except FileNotFound:
            return False

    @override
    def _isdir(self, path: str) -> bool:
        from pywebhdfs.errors import FileNotFound

        try:
            stats = self.__webhdfs.get_file_dir_status(path)
            return stats["FileStatus"]["type"] == "DIRECTORY"  # type: ignore[no-any-return]
        except FileNotFound:
            return False

    @override
    def _isfile(self, path: str) -> bool:
        from pywebhdfs.errors import FileNotFound

        try:
            stats = self.__webhdfs.get_file_dir_status(path)
            return stats["FileStatus"]["type"] == "FILE"  # type: ignore[no-any-return]
        except FileNotFound:
            return False

    # Directory handling and enumeration
    @override
    def _dir(self, path: str) -> Generator[FileSystemFileDesc, None, None]:
        files = self.__webhdfs.list_dir(path)
        for f in files["FileStatuses"]["FileStatus"]:
            yield FileSystemFileDesc(
                fs=self,
                path=posixpath.join(path, f["pathSuffix"]),
                name=f["pathSuffix"],
                type=f["type"].lower(),
                bytes=f["length"],
                owner=f["owner"],
                group=f["group"],
                last_modified=f["modificationTime"],
                last_accessed=f["accessTime"],
                permissions=f["permission"],
                replication=f["replication"],
            )

    @override
    def _mkdir(self, path: str, recursive: bool, exist_ok: bool) -> None:
        if not recursive and not self._isdir(self.path_basename(path)):
            raise OSError(f"No parent directory found for {path}.")
        if not exist_ok and self._exists(path):
            raise OSError(f"Path already exists at {path}.")
        self.__webhdfs.make_dir(path)

    @override
    def _remove(self, path: str, recursive: bool) -> Any:
        return self.__webhdfs.delete_file_dir(path, recursive)

    # File handling
    @override
    def _file_read_(
        self, path: str, size: int = -1, offset: int = 0, binary: bool = False
    ) -> str | bytes:
        if not self.isfile(path):
            raise FileNotFoundError(f"File `{path}` does not exist.")

        read: str | bytes = self.__webhdfs.read_file(
            path, offset=offset, length="null" if size < 0 else size
        )
        if not binary:
            read = read.decode("utf-8") if isinstance(read, bytes) else read
        return read

    @override
    def _file_append_(self, path: str, s: str | bytes, binary: bool) -> Any:
        return self.__webhdfs.append_file(path, s)

    @override
    def _file_write_(self, path: str, s: str | bytes, binary: bool) -> Any:
        return self.__webhdfs.create_file(path, s, overwrite=True)
