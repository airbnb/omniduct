from omniduct.filesystems.local import LocalFsClient

from .base import Cache


class FileSystemCache(Cache):

    PROTOCOLS = ['filesystem_cache']

    def _init(self, path, fs=None):
        """
        path (str): The top-level path of the cache in the filesystem.
        fs (FileSystemClient): The filesystem client to use as the datastore of
            this cache. If not specified, this will default to the local filesystem
            using `LocalFsClient`.
        """
        self.fs = fs or LocalFsClient()
        self.path = path

    def _connect(self):
        self.fs.connect()

    def _is_connected(self):
        return self.fs.is_connected()

    def _disconnect(self):
        return self.fs.disconnect()

    # Implementations for abstract methods in Cache

    def _namespace(self, namespace):
        if namespace is None:
            return '__default__'
        assert isinstance(namespace, str)
        return namespace

    def _get_namespaces(self):
        return self.fs.listdir(self.path)

    def _has_namespace(self, namespace):
        return self.fs.exists(self.fs.path_join(self.path, namespace))

    def _remove_namespace(self, namespace):
        return self.fs.remove(self.fs.path_join(self.path, namespace), recursive=True)

    def _get_keys(self, namespace):
        return self.fs.listdir(self.fs.path_join(self.path, namespace))

    def _has_key(self, namespace, key):
        return self.fs.exists(self.fs.path_join(self.path, namespace, key))

    def _remove_key(self, namespace, key):
        return self.fs.remove(self.fs.path_join(self.path, namespace, key), recursive=True)

    def _get_stream_for_key(self, namespace, key, stream_name, mode, create):
        path = self.fs.path_join(self.path, namespace, key)

        if create:
            self.fs.mkdir(path, recursive=True)

        return self.fs.open(self.fs.path_join(path, stream_name), mode=mode)
