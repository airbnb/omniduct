import io
from abc import abstractmethod

from omniduct.duct import Duct
from omniduct.utils.magics import MagicsProvider, process_line_arguments


class FileSystemClient(Duct, MagicsProvider):

    DUCT_TYPE = Duct.Type.FILESYSTEM
    DEFAULT_PORT = None

    def __init__(self, *args, **kwargs):
        '''
        This is a shim __init__ function that passes all arguments onto
        `self._init`, which is implemented by subclasses. This allows subclasses
        to instantiate themselves with arbitrary parameters.
        '''
        Duct.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)
        self._init(*args, **kwargs)

    @abstractmethod
    def _init(self):
        pass

    # Filesystem accessors

    def exists(self, path):
        return self.connect()._exists(path)

    @abstractmethod
    def _exists(self, path):
        raise NotImplementedError

    def isdir(self, path):
        return self.connect()._isdir(path)

    @abstractmethod
    def _isdir(self, path):
        raise NotImplementedError

    def isfile(self, path):
        return self.connect()._isfile(path)

    @abstractmethod
    def _isfile(self, path):
        raise NotImplementedError

    def listdir(self, path=None):
        return self.connect()._listdir(path)

    @abstractmethod
    def _listdir(self, path):
        raise NotImplementedError

    def showdir(self, path=None):
        return self.connect()._showdir(path)

    @abstractmethod
    def _showdir(self, path):
        raise NotImplementedError

    # Directory handling

    def mkdir(self, path, parents=True):
        pass

    # File opening

    def open(self, path, mode='rt'):
        return FileSystemFile(self, path, mode)

    def _file_read(self, path, size=-1, offset=0, binary=False):
        return self.connect()._file_read_(path, size=size, binary=binary)

    @abstractmethod
    def _file_read_(self, path, size=-1, offset=0, binary=False):
        raise NotImplementedError

    def _file_write(self, path, s):
        return self.connect()._file_write_(path, s)

    @abstractmethod
    def _file_write_(self, path, s):
        raise NotImplementedError

    def _file_append(self, path, s):
        return self.connect()._file_append_(path, s)

    @abstractmethod
    def _file_append_(self, path, s):
        raise NotImplementedError

    def _register_magics(self, base_name):
        from IPython.core.magic import register_line_magic, register_cell_magic

        @register_cell_magic(base_name)
        @process_line_arguments
        def test(cell, path):
            return self._file_write(path, cell)

        @register_line_magic(base_name)
        @process_line_arguments
        def test(path):
            return self._file_read(path)


# TODO: properly implement file modes and raise correct errors (consistent with other file types)
class FileSystemFile(object):

    def __init__(self, fs, path, mode='r'):
        self.fs = fs
        self.path = path
        self.mode = mode
        self.offset = 0
        self.closed = False
        self.__modified = False

        if self.binary_mode:
            self.__io_buffer = io.BytesIO()
        else:
            self.__io_buffer = io.StringIO()

        if 'w' not in self.mode:
            self.__io_buffer.write(self.fs._file_read(self.path))
            if 'a' not in self.mode:
                self.__io_buffer.seek(0)

    @property
    def mode(self):
        return self.__mode

    @mode.setter
    def mode(self, mode):
        assert len(set(mode)) == len(mode)
        assert sum(l in mode for l in ['r', 'w', 'a', '+', 't', 'b']) == len(mode)
        assert sum(l in mode for l in ['r', 'w', 'a']) == 1
        assert sum(l in mode for l in ['t', 'b']) < 2
        self.__mode = mode

    @property
    def readable(self):
        return 'r' in self.mode or '+' in self.mode

    @property
    def writeable(self):
        return 'w' in self.mode or 'a' in self.mode or '+' in self.mode

    @property
    def appending(self):
        return 'a' in self.mode

    @property
    def binary_mode(self):
        return 'b' in self.mode

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def close(self):
        self.flush()
        self.closed = True

    def flush(self):
        if not self.writeable or not self.__modified:
            return
        # For the time being, just write out entire buffer. We can consider something cleverer later
        offset = self.tell()

        self.seek(0)
        self.fs._file_write(self.path, self.read())

        self.seek(offset)

    def isatty(self):
        return self.__io_buffer.isatty()

    def read(self, size=-1):
        assert self.readable
        return self.__io_buffer.read(size)

    def readline(self, size=-1):
        assert self.readable
        return self.__io_buffer.readline(size)

    def readlines(self, hint=-1):
        assert self.readable
        return self.__io_buffer.readlines(hint)

    def seek(self, pos, whence=0):
        self.__io_buffer.seek(pos)

    def tell(self):
        return self.__io_buffer.tell()

    def write(self, s):
        assert self.writeable
        self.__io_buffer.write(s)
        self.__modified = True
