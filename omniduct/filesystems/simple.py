import os

from .base import FileSystemClient, FileSystemFile


class SimpleFsClient(FileSystemClient):

    PROTOCOLS = ['simplefs']
    DEFAULT_PORT = 22

    def _init(self):
        pass

    def _connect(self):
        pass

    def _is_connected(self):
        if self.remote:
            return self.remote.is_connected()
        else:
            return True

    def _disconnect(self):
        pass

    # File enumeration

    def _exists(self, path):
        raise NotImplementedError

    def _isdir(self, path):
        raise NotImplementedError

    def _isfile(self, path):
        raise NotImplementedError

    def _listdir(self, path):
        if self.remote:
            return self.remote.execute('ls {}'.format(path)).stdout.decode().split('\n')
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

    def _file_append_(self, path, s):
        raise NotImplementedError

    def _file_write_(self, path, s):
        raise NotImplementedError
