import posixpath

import pandas

from .base import FileSystemClient


class WebHdfsClient(FileSystemClient):

    PROTOCOLS = ['webhdfs']
    DEFAULT_PORT = 50070

    def _init(self, namenodes=None, global_writes=False, **kwargs):
        self._namenodes = namenodes
        self.global_writes = global_writes

        self.__webhdfs = None
        self.__webhdfs_kwargs = kwargs
        self.__home_directory = None
        self.prepared_fields += ('_namenodes',)

    def _connect(self):
        from .webhdfs_helpers import OmniductPyWebHdfsClient
        self.__webhdfs = OmniductPyWebHdfsClient(host=self._host, port=self._port, remote=self.remote, namenodes=self._namenodes, user_name=self.username, **self.__webhdfs_kwargs)
        self.__home_directory = self.__webhdfs.get_home_directory()

    def _is_connected(self):
        try:
            if self.remote and not self.remote.is_connected():
                return False
            return self.__webhdfs is not None
        except:
            return False

    def _disconnect(self):
        self.__webhdfs = None
        self.__home_directory = None

    # File enumeration

    def __get_path(self, path):
        return posixpath.abspath(posixpath.join(self.__home_directory, path or ''))

    def __in_home_directory(self, path):
        return posixpath.abspath(path).startswith(self.__home_directory)

    def _exists(self, path):
        path = self.__get_path(path)
        from pywebhdfs.errors import FileNotFound
        try:
            self.__webhdfs.get_file_dir_status(path)
            return True
        except FileNotFound:
            return False

    def _isdir(self, path):
        path = self.__get_path(path)
        from pywebhdfs.errors import FileNotFound
        try:
            stats = self.__webhdfs.get_file_dir_status(path)
            return stats['FileStatus']['type'] == 'DIRECTORY'
        except FileNotFound:
            return False

    def _isfile(self, path):
        path = self.__get_path(path)
        from pywebhdfs.errors import FileNotFound
        try:
            stats = self.__webhdfs.get_file_dir_status(path)
            return stats['FileStatus']['type'] == 'FILE'
        except FileNotFound:
            return False

    def _listdir(self, path, detailed=False):
        path = self.__get_path(path)
        files = self.__webhdfs.list_dir(path)
        return [f['pathSuffix'] for f in files['FileStatuses']['FileStatus']]

    def _showdir(self, path):
        path = self.__get_path(path)
        files = self.__webhdfs.list_dir(path)
        return pandas.DataFrame(files['FileStatuses']['FileStatus'])[[
            'type',
            'pathSuffix',
            'childrenNum',
            'blockSize',
            'accessTime',
            'modificationTime',
            'permission',
            'owner',
            'group',
            'length',
            'replication',
        ]].sort_values(['type', 'pathSuffix']).reset_index(drop=True)

    # File opening

    def _file_read_(self, path, size=-1, offset=0, binary=False):
        path = self.__get_path(path)
        read = self.__webhdfs.read_file(path, offset=offset, length='null' if size < 0 else size)
        if not binary:
            read = read.decode()
        return read

    def _file_append_(self, path, s, binary):
        path = self.__get_path(path)
        return self.__webhdfs.append_file(path, s)

    def _file_write_(self, path, s, binary):
        path = self.__get_path(path)
        if not self.global_writes and not self.__in_home_directory(path):
            raise RuntimeError("Attempting to write outside of home directory without setting '{name}.global_writes' to True.".format(name=self.name))
        return self.__webhdfs.create_file(path, s, overwrite=True)
