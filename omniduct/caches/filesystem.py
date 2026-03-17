from __future__ import annotations

from typing import IO, Any, cast

import yaml
from interface_meta import override

from omniduct.filesystems.base import FileSystemClient
from omniduct.filesystems.local import LocalFsClient

from .base import Cache


class FileSystemCache(Cache):
    """
    An implementation of `Cache` that wraps around a `FilesystemClient`.
    """

    PROTOCOLS = ["filesystem_cache"]

    fs: FileSystemClient

    @override
    def _init(self, path: str, fs: FileSystemClient | str | None = None) -> None:
        """
        path: The top-level path of the cache in the filesystem.
        fs: The filesystem client to use as the datastore of this cache. If not
            specified, this will default to the local filesystem using
            `LocalFsClient`. If specified as a string, and connected to a
            `DuctRegistry`, upon first use an attempt will be made to look up a
            `FileSystemClient` instance in the registry by this name.
        """
        self._fs: FileSystemClient | str = fs or LocalFsClient()
        self.path: str = path
        # Currently config is not used, but will be in future versions
        self._config: dict[str, Any] | None = None
        self.connection_fields += ("fs",)

    @override
    def _prepare(self) -> None:
        Cache._prepare(self)

        if isinstance(self._fs, FileSystemClient):
            self.fs = self._fs
        else:
            if self.registry is None:
                raise RuntimeError(
                    f"Cache is configured to use a filesystem client named {self._fs!r}, but no registry is available to look this up from."
                )
            self.fs = cast(
                FileSystemClient,
                self.registry.lookup(
                    cast(str, self._fs), kind=FileSystemCache.Type.FILESYSTEM
                ),
            )
        if not isinstance(self.fs, FileSystemClient):
            raise TypeError(
                "Provided cache is not an instance of `omniduct.filesystems.base.FileSystemClient`."
            )

        self._prepare_cache()

    def _prepare_cache(self) -> dict[str, Any]:
        config_path = self.fs.path_join(self.path, "config")
        if self.fs.exists(config_path):
            with self.fs.open(config_path) as fh:
                try:
                    return yaml.safe_load(fh)  # type: ignore[no-any-return]
                except yaml.error.YAMLError as e:
                    raise RuntimeError(
                        f"Path nominated for cache ('{self.path}') has a corrupt "
                        "configuration. Please manually empty or delete this path "
                        "cache, and try again."
                    ) from e

        # Cache needs initialising
        if self.fs.exists(self.path):
            if not self.fs.isdir(self.path):
                raise RuntimeError(
                    f"Path nominated for cache ('{self.path}') is not a directory."
                )
            if self.fs.listdir(self.path):
                raise RuntimeError(
                    f"Cache directory ({self.path}) needs to be initialised, and is not empty. Please manually delete and/or empty this path, and try again."
                )
        else:  # Create cache directory
            self.fs.mkdir(self.path, recursive=True, exist_ok=True)

        # Write config file to mark cache as initialised
        with self.fs.open(config_path, "w") as fh:
            yaml.safe_dump({"version": 1}, fh, default_flow_style=False)
        return {"version": 1}

    @override
    def _connect(self) -> None:
        self.fs.connect()

    @override
    def _is_connected(self) -> bool:
        return self.fs.is_connected()  # type: ignore[no-any-return]

    @override
    def _disconnect(self) -> None:
        self.fs.disconnect()

    # Implementations for abstract methods in Cache
    @override
    def _namespace(self, namespace: str | None) -> str:
        if namespace is None:
            return "__default__"
        if not isinstance(namespace, str) or namespace == "config":
            raise ValueError(
                f"Invalid namespace {namespace!r}: must be a non-empty string and cannot be 'config'."
            )
        return namespace

    @override
    def _get_namespaces(self) -> list[str]:
        return [d for d in self.fs.listdir(self.path) if d != "config"]

    @override
    def _has_namespace(self, namespace: str) -> bool:
        return self.fs.exists(self.fs.path_join(self.path, namespace))  # type: ignore[no-any-return]

    @override
    def _remove_namespace(self, namespace: str) -> None:
        self.fs.remove(self.fs.path_join(self.path, namespace), recursive=True)

    @override
    def _get_keys(self, namespace: str) -> list[str]:
        return self.fs.listdir(self.fs.path_join(self.path, namespace))

    @override
    def _has_key(self, namespace: str, key: str) -> bool:
        return self.fs.exists(self.fs.path_join(self.path, namespace, key))  # type: ignore[no-any-return]

    @override
    def _remove_key(self, namespace: str, key: str) -> None:
        self.fs.remove(self.fs.path_join(self.path, namespace, key), recursive=True)

    @override
    def _get_bytecount_for_key(self, namespace: str, key: str) -> int:
        path = self.fs.path_join(self.path, namespace, key)
        return sum(f.bytes for f in self.fs.dir(path))

    @override
    def _get_stream_for_key(
        self,
        namespace: str,
        key: str,
        stream_name: str,
        mode: str,
        create: bool,
    ) -> IO[Any]:
        path = self.fs.path_join(self.path, namespace, key)

        if create:
            self.fs.mkdir(path, recursive=True, exist_ok=True)

        return self.fs.open(self.fs.path_join(path, stream_name), mode=mode)  # type: ignore[no-any-return]
