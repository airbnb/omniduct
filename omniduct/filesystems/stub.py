from .base import FileSystemClient


class StubFsClient(FileSystemClient):

    PROTOCOLS = []
    DEFAULT_PORT = None

    def _init(self):
        pass

    # Connection management

    def _connect(self):
        raise NotImplementedError

    def _is_connected(self):
        raise NotImplementedError

    def _disconnect(self):
        raise NotImplementedError

    # Path properties and helpers

    def _path_home(self):
        return NotImplementedError

    def _path_separator(self):
        raise NotImplementedError

    # File node properties

    def _exists(self, path):
        raise NotImplementedError

    def _isdir(self, path):
        raise NotImplementedError

    # Directory handling and enumeration

    def _dir(self, path):
        raise NotImplementedError

    def _mkdir(self, path, recursive):
        raise NotImplementedError

    # File handling

    def _file_read_(self, path, size=-1, offset=0, binary=False):
        raise NotImplementedError

    def _file_write_(self, path, s, binary):
        raise NotImplementedError

    def _file_append_(self, path, s, binary):
        raise NotImplementedError
