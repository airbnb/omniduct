import os

from .base import FileSystemClient, FileSystemFile


class LocalFsClient(FileSystemClient):

    PROTOCOLS = ['localfs']
    DEFAULT_PORT = 22

    def _init(self):
        pass

    def _connect(self):
        pass

    def _is_connected(self):
        return True

    def _disconnect(self):
        pass

    # File enumeration

    def _exists(self, path):
        return os.path.exists(path)

    def _isdir(self, path):
        return os.path.isdir(path)

    def _isfile(self, path):
        return os.path.isfile(path)

    def _listdir(self, path):
        return os.listdir(path)

    def _showdir(self, path):
        raise NotImplementedError

    # File opening

    def _file_read_(self, path, size=-1, offset=0, binary=False):
        if self.remote:
            read = self.remote.execute('cat {}'.format(path)).stdout
            if not binary:
                read = read.decode()
            return read
        else:
            with open(path, 'r{}'.format('b' if binary else '')) as f:
                return f.read()

    def _file_append_(self, path, s, binary):
        raise NotImplementedError

    def _file_write_(self, path, s, binary):
        with open(path, 'w' + ('b' if binary else '')) as f:
            return f.write(s)
