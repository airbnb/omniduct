import six
import yaml
from interface_meta import override

from omniduct.filesystems.base import FileSystemClient
from omniduct.filesystems.local import LocalFsClient

from .base import Cache


class FileSystemCache(Cache):
    """
    An implementation of `Cache` that wraps around a `FilesystemClient`.
    """

    PROTOCOLS = ['filesystem_cache']

    @override
    def _init(self, path, fs=None):
        """
        path (str): The top-level path of the cache in the filesystem.
        fs (FileSystemClient, str): The filesystem client to use as the
            datastore of this cache. If not specified, this will default to the
            local filesystem using `LocalFsClient`. If specified as a string,
            and connected to a `DuctRegistry`, upon first use an attempt will be
            made to look up a `FileSystemClient` instance in the registry by
            this name.
        """
        self.fs = fs or LocalFsClient()
        self.path = path
        # Currently config is not used, but will be in future versions
        self._config = None
        self.connection_fields += ('fs',)

    @override
    def _prepare(self):
        Cache._prepare(self)

        if self.registry is not None:
            if isinstance(self.fs, six.string_types):
                self.fs = self.registry.lookup(self.fs, kind=FileSystemCache.Type.FILESYSTEM)
        assert isinstance(self.fs, FileSystemClient), "Provided cache is not an instance of `omniduct.filesystems.base.FileSystemClient`."

        self._prepare_cache()

    def _prepare_cache(self):
        config_path = self.fs.path_join(self.path, 'config')
        if self.fs.exists(config_path):
            with self.fs.open(config_path) as fh:
                try:
                    return yaml.safe_load(fh)
                except yaml.error.YAMLError:
                    raise RuntimeError(
                        "Path nominated for cache ('{}') has a corrupt "
                        "configuration. Please manually empty or delete this "
                        "path cache, and try again.".format(self.path)
                    )

        # Cache needs initialising
        if self.fs.exists(self.path):
            if not self.fs.isdir(self.path):
                raise RuntimeError(
                    "Path nominated for cache ('{}') is not a directory.".format(self.path)
                )
            elif self.fs.listdir(self.path):
                raise RuntimeError(
                    "Cache directory ({}) needs to be initialised, and is not "
                    "empty. Please manually delete and/or empty this path, and "
                    "try again.".format(self.path)
                )
        else:  # Create cache directory
            self.fs.mkdir(self.path, recursive=True, exist_ok=True)

        # Write config file to mark cache as initialised
        with self.fs.open(config_path, 'w') as fh:
            yaml.safe_dump({'version': 1}, fh, default_flow_style=False)
        return {'version': 1}

    @override
    def _connect(self):
        self.fs.connect()

    @override
    def _is_connected(self):
        return self.fs.is_connected()

    @override
    def _disconnect(self):
        return self.fs.disconnect()

    # Implementations for abstract methods in Cache
    @override
    def _namespace(self, namespace):
        if namespace is None:
            return '__default__'
        assert isinstance(namespace, str) and namespace != 'config'
        return namespace

    @override
    def _get_namespaces(self):
        return [d for d in self.fs.listdir(self.path) if d != 'config']

    @override
    def _has_namespace(self, namespace):
        return self.fs.exists(self.fs.path_join(self.path, namespace))

    @override
    def _remove_namespace(self, namespace):
        return self.fs.remove(self.fs.path_join(self.path, namespace), recursive=True)

    @override
    def _get_keys(self, namespace):
        return self.fs.listdir(self.fs.path_join(self.path, namespace))

    @override
    def _has_key(self, namespace, key):
        return self.fs.exists(self.fs.path_join(self.path, namespace, key))

    @override
    def _remove_key(self, namespace, key):
        return self.fs.remove(self.fs.path_join(self.path, namespace, key), recursive=True)

    @override
    def _get_bytecount_for_key(self, namespace, key):
        path = self.fs.path_join(self.path, namespace, key)
        return sum([
            f.bytes
            for f in self.fs.dir(path)
        ])

    @override
    def _get_stream_for_key(self, namespace, key, stream_name, mode, create):
        path = self.fs.path_join(self.path, namespace, key)

        if create:
            self.fs.mkdir(path, recursive=True, exist_ok=True)

        return self.fs.open(self.fs.path_join(path, stream_name), mode=mode)
