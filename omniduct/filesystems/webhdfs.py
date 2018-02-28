import posixpath

from .base import FileSystemClient, FileSystemFileDesc


class WebHdfsClient(FileSystemClient):
    """
    This Duct connects to an Apache WebHDFS server using the `pywebhdfs` library.

    Parameters:
        namenodes (list<str>): A list of hosts that are acting as namenodes for
            the HDFS cluster in form "<hostname>:<port>".
    """

    PROTOCOLS = ['webhdfs']
    DEFAULT_PORT = 50070

    def _init(self, namenodes=None, **kwargs):
        """
        namenodes (list<str>): A list of hosts that are acting as namenodes for
            the HDFS cluster in form "<hostname>:<port>".
        **kwargs (dict): Additional arguments to pass onto the WebHdfs client.
        """
        self.namenodes = namenodes

        self.__webhdfs = None
        self.__webhdfs_kwargs = kwargs
        self.prepared_fields += ('namenodes',)

    def _connect(self):
        from .webhdfs_helpers import OmniductPyWebHdfsClient
        self.__webhdfs = OmniductPyWebHdfsClient(
            host=self._host,
            port=self._port,
            remote=self.remote,
            namenodes=self.namenodes,
            user_name=self.username,
            **self.__webhdfs_kwargs
        )

    def _is_connected(self):
        try:
            if self.remote and not self.remote.is_connected():
                return False
            return self.__webhdfs is not None
        except:
            return False

    def _disconnect(self):
        self.__webhdfs = None

    # Path properties and helpers

    def _path_home(self):
        return self.__webhdfs.get_home_directory()

    def _path_separator(self):
        return '/'

    # File node properties

    def _exists(self, path):
        from pywebhdfs.errors import FileNotFound
        try:
            self.__webhdfs.get_file_dir_status(path)
            return True
        except FileNotFound:
            return False

    def _isdir(self, path):
        from pywebhdfs.errors import FileNotFound
        try:
            stats = self.__webhdfs.get_file_dir_status(path)
            return stats['FileStatus']['type'] == 'DIRECTORY'
        except FileNotFound:
            return False

    def _isfile(self, path):
        from pywebhdfs.errors import FileNotFound
        try:
            stats = self.__webhdfs.get_file_dir_status(path)
            return stats['FileStatus']['type'] == 'FILE'
        except FileNotFound:
            return False

    # Directory handling and enumeration

    def _dir(self, path):
        files = self.__webhdfs.list_dir(path)
        for f in files['FileStatuses']['FileStatus']:
            yield FileSystemFileDesc(
                fs=self,
                path=posixpath.join(path, f['pathSuffix']),
                name=f['pathSuffix'],
                type=f['type'].lower(),
                bytes=f['length'],
                owner=f['owner'],
                group=f['group'],
                last_modified=f['modificationTime'],
                last_accessed=f['accessTime'],
                permissions=f['permission'],
                replication=f['replication']
            )

    def _mkdir(self, path, recursive):
        raise NotImplementedError

    # File handling

    def _file_read_(self, path, size=-1, offset=0, binary=False):
        read = self.__webhdfs.read_file(path, offset=offset, length='null' if size < 0 else size)
        if not binary:
            read = read.decode()
        return read

    def _file_append_(self, path, s, binary):
        return self.__webhdfs.append_file(path, s)

    def _file_write_(self, path, s, binary):
        return self.__webhdfs.create_file(path, s, overwrite=True)
