import io
from abc import abstractmethod
from collections import namedtuple, OrderedDict

import pandas as pd
from omniduct.duct import Duct
from omniduct.utils.magics import MagicsProvider, process_line_arguments


class FileSystemClient(Duct, MagicsProvider):
    """
    `FileSystemClient` is an abstract subclass of `Duct` that provides a common
    API for all filesystem clients, which in turn will be subclasses of this
    class.

    Class Attributes:
        DUCT_TYPE (`Duct.Type`): The type of `Duct` protocol implemented by this class.
        DEFAULT_PORT (int): The default port for the filesystem service (defined
            by subclasses).
    """

    DUCT_TYPE = Duct.Type.FILESYSTEM
    DEFAULT_PORT = None

    def __init__(self, *args, **kwargs):
        """
        This is a shim __init__ function that passes all arguments onto
        `self._init`, which is implemented by subclasses. This allows subclasses
        to instantiate themselves with arbitrary parameters.
        """
        Duct.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)
        self._init(*args, **kwargs)

    @abstractmethod
    def _init(self):
        pass

    # Path properties and helpers

    @property
    def path_home(self):
        """
        str: The default path prefix to use for all non-absolute path references
        on this filesystem.
        """
        return self._path_home()

    @abstractmethod
    def _path_home(self):
        return NotImplementedError

    @property
    def path_separator(self):
        return self._path_separator()

    @abstractmethod
    def _path_separator(self):
        raise NotImplementedError

    def path_join(self, path, *components):
        for component in components:
            if component.startswith(self.path_separator) or component.startswith(self.path_home):  # It may be that some filesystems use special characters to denote home, like '~'
                path = component
            else:
                path = '{}{}{}'.format(path, self.path_separator if not path.endswith(self.path_separator) else '', component)
        return path

    def path_basename(self, path):
        return self._path(path).split(self.path_separator)[-1]

    def path_dirname(self, path):
        return self.path_separator.join(self._path(path).split(self.path_separator)[:-1])

    def _path(self, path=None):
        return self.path_home if path is None else self.path_join(self.path_home, path)

    # Filesystem accessors

    def exists(self, path):
        """
        This method checks whether a file (or folder) exists at the given path,
        relative (as appropriate) to the current working directory of home folder.

        Parameters:
            path (str): The path for which to check existence.

        Returns:
            bool: `True` if file/folder exists at nominated path, and `False`
            otherwise.
        """
        return self.connect()._exists(self._path(path))

    @abstractmethod
    def _exists(self, path):
        raise NotImplementedError

    def isdir(self, path):
        """
        This method checks to see whether a folder/directory exists at the given
        path.

        Parameters:
            path (str): The path for which to check directory nature.

        Returns:
            bool: `True` if folder exists at nominated path, and `False`
            otherwise.
        """
        return self.connect()._isdir(self._path(path))

    @abstractmethod
    def _isdir(self, path):
        raise NotImplementedError

    def isfile(self, path):
        """
        This method checks to see whether a file (not a directory) exists at the given
        path.

        Parameters:
            path (str): The path for which to check file nature.

        Returns:
            bool: `True` if a file exists at nominated path, and `False`
            otherwise.
        """
        return self.connect()._isfile(self._path(path))

    def _isfile(self, path):
        return not self._isdir(path)

    # Directory handling

    @abstractmethod
    def _dir(self, path):
        """
        This method should return a generator over `FileSystemFileDesc` objects.
        """
        raise NotImplementedError

    def dir(self, path=None):
        """
        This method returns a generator over `FileSystemFileDesc` objects that
        represent the files/directories that a present as children of the
        nominated path. If `path` is not a directory, an exception is raised.

        :todo:`Which exception class should be raised.`

        Parameters:
            path (str): The path to examine for children.

        Returns:
            generator<FileSystemFileDesc>: The children of `path` represented as
            `FileSystemFileDesc` objects.
        """
        assert self.isdir(path), "'{}' is not a valid directory.".format(path)
        return self.connect()._dir(self._path(path))

    def listdir(self, path=None):
        """
        This method inspects the contents of a directory, and returns the names
        of child members as strings.

        Parameters:
            path (str): The path of the directory from which to enumerate filenames.

        Returns:
            list<str>: The names of all children of the nominated directory.
        """
        return [f.name for f in self.dir(self._path(path))]

    def showdir(self, path=None):
        """
        This method returns a pandas DataFrame representation of the contents of
        a path. Some columns may be unique to a particular protocol, but
        the returned DataFrame will at least have the columns: .... :todo:`asdasd`
        """
        assert self.isdir(path), "'{}' is not a valid directory.".format(path)
        return self.connect()._showdir(self._path(path))

    def _showdir(self, path):
        data = [f.as_dict() for f in self._dir(path)]
        if len(data) > 0:
            return (
                pd.DataFrame(data)
                .sort_values(['type', 'name'])
                .reset_index(drop=True)
                .dropna(axis='columns', how='all')
                .drop(axis=1, labels=['fs', 'path'])
            )
        else:
            return "Directory has no contents."

    def walk(self, path=None):
        """
        This method recursively walks over all paths that are children of
        `path`, return a geneator over tuples, one for each directory, of form:
        (<path name>, [<directory 1>, ...], [<file 1>, ...])

        Parameters:
            path (str): The path of the directory from which to enumerate
                contents.

        Returns:
            generator<tuple>: A generator of tuples, each associated with a
                directory descendent of `path`.
        """
        assert self.isdir(path), "'{}' is not a valid directory.".format(path)
        return self.connect()._walk(self._path(path))

    def _walk(self, path):
        dirs = []
        files = []
        for f in self._dir(path):
            if f.type == 'directory':
                dirs.append(f.name)
            else:
                files.append(f.name)
        yield (path, dirs, files)

        for dir in dirs:
            for walked in self._walk(self._path(self.path_join(path, dir))):  # Note: using _walk directly here, which may fail if disconnected during walk.
                yield walked

    def find(self, path_prefix=None, **attrs):
        """
        This method searches for files or folders which satisfy certain
        constraints on the attributes of the file (as encoded into
        `FileSystemFileDesc`). Note that without an attribute constraints,
        this method will function identically to `self.dir`.

        Parameters:
            path_prefix (str): The path under which files/directories should be
                found.
            **attrs (dict): Constraints on the fields of the `FileSystemFileDesc`
                objects associated with this filesystem, as constant values or
                callable objects (in which case the object will be called and
                should return True if attribute value is match, and False
                otherwise).

        Returns:
            generator<FileSystemFileDesc>: A generator over `FileSystemFileDesc`
                objects that are descendents of `path_prefix` and which statisfy
                provided constraints.
        """
        assert self.isdir(path_prefix), "'{0}' is not a valid directory. Did you mean `.find(name='{0}')`?".format(path_prefix)
        return self.connect()._find(self._path(path_prefix), **attrs)

    def _find(self, path_prefix, **attrs):

        def is_match(f):
            for attr, value in attrs.items():
                if hasattr(value, '__call__') and not value(f.as_dict().get(attr)):
                    return False
                elif value != f.as_dict().get(attr):
                    return False
            return True

        dirs = []
        for f in self._dir(path_prefix):
            if f.type == 'directory':
                dirs.append(f.name)
            if is_match(f):
                yield f

        for dir in dirs:
            for match in self._find(self._path(self.path_join(path_prefix, dir)), **attrs):  # Note: using _find directly here, which may fail if disconnected during find.
                yield match

    def mkdir(self, path, recursive=True):
        """
        This method creates a directory at the specified path, recursively
        creating any parents as needed unless `recursive` is set to `False`.

        Parameters:
            path (str): The path of the directory to create.
            recursive (bool): Whether to recursively create any parents of this
                path if they do not already exist.
        """
        return self.connect()._mkdir(self._path(path), recursive)

    @abstractmethod
    def _mkdir(self, path, recursive):
        raise NotImplementedError

    # File handling

    def open(self, path, mode='rt'):
        """
        This method opens the file at the given path for reading and/or writing
        operations. The object returned is programmatically interchangeable with
        any other Python file-like object, including file modes. If the file is
        opened in write mode, changes will only be flushed to the source filesystem
        when the file is closed.

        Parameters:
            path (str): The path of the file to open.
            mode (str): All standard Python file modes.

        Returns:
            FileSystemFile: An opened file-like object.
        """
        return FileSystemFile(self, self._path(path), mode)

    def _file_read(self, path, size=-1, offset=0, binary=False):
        return self.connect()._file_read_(self._path(path), size=size, offset=offset, binary=binary)

    @abstractmethod
    def _file_read_(self, path, size=-1, offset=0, binary=False):
        raise NotImplementedError

    def _file_write(self, path, s, binary=False):
        return self.connect()._file_write_(self._path(path), s, binary)

    @abstractmethod
    def _file_write_(self, path, s, binary):
        raise NotImplementedError

    def _file_append(self, path, s, binary=False):
        return self.connect()._file_append_(self._path(path), s, binary)

    @abstractmethod
    def _file_append_(self, path, s, binary):
        raise NotImplementedError

    # File transfer

    def download(self, source, dest=None, overwrite=False, fs=None):
        """
        This method downloads a file/folder from path `source` on this filesystem
        to the path `dest` on filesytem `fs`, overwriting any existing file if
        `overwrite` is `True`.

        Parameters:
            source (str): The path on this filesystem of the file to download to
                the nominated filesystem (`fs`). If `source` ends
                with '/' then contents of the the `source` directory will be
                copied into destination folder, and will throw an error if path
                does not resolve to a directory.
            dest (str): The destination path on filesystem (`fs`). If not
                specified, the file/folder is uploaded into the default path,
                usually one's home folder. If `dest` ends with '/',
                and corresponds to a directory, the contents of source will be
                copied instead of copying the entire folder. If `dest` is
                otherwise a directory, an exception will be raised.
            overwrite (bool): `True` if the contents of any existing file by the
                same name should be overwritten, `False` otherwise.
            fs (FileSystemClient): The FileSystemClient into which the nominated
                file/folder `source` should be downloaded. If not specified,
                defaults to the local filesystem.
        """
        source = self._path(source)
        if fs is None:
            from .local import LocalFsClient
            fs = LocalFsClient()

        if dest.endswith(fs.path_separator):
            assert fs.isdir(dest), "No such directory `{}`".format(dest)
            if not source.endswith(self.path_separator):
                dest = fs.path_join(fs._path(dest), self.path_basename(source))

        # A mapping of source to dest paths on the respective filesystems
        # In format: (source, dest, isdir?)
        targets = []

        if self.isdir(source):
            target_prefix = (
                source if source.endswith(self.path_separator) else source + self.path_separator
            )

            for path, dirs, files in self.walk(source):
                for dir in dirs:
                    target_source = self.path_join(path, dir)
                    targets.append((
                        target_source,
                        fs.path_join(dest, *target_source[len(target_prefix):].split(self.path_separator)),
                        True
                    ))
                for file in files:
                    target_source = self.path_join(path, file)
                    targets.append((
                        target_source,
                        fs.path_join(dest, *target_source[len(target_prefix):].split(self.path_separator)),
                        False
                    ))
        else:
            targets.append((source, dest, False))

        for target in targets:
            if target[2]:
                fs.mkdir(target[1])
            else:
                self.connect()._download(target[0], target[1], overwrite, fs)

    def _download(self, source, dest, overwrite, fs):
        if not overwrite and self.exists(dest):
            raise RuntimeError("File already exists on filesystem.")
        with fs.open(dest, 'wb') as f:
            assert f.binary_mode is True, dest
            f.write(self._file_read(source, binary=True))

    def upload(self, source, dest=None, overwrite=False, fs=None):
        """
        This method uploads a file/folder from path `source` on filesystem `fs`
        to the path `dest` on this filesytem, overwriting any existing file if
        `overwrite` is `True`. This is equivalent to `fs.download(..., fs=self)`.

        Parameters:
            source (str): The path on the specified filesystem (`fs`) of the
                file to upload to this filesystem. If `source` ends with '/',
                and corresponds to a directory, the contents of source will be
                copied instead of copying the entire folder.
            dest (str): The destination path on this filesystem. If not
                specified, the file/folder is uploaded into the default path,
                usually one's home folder, on this filesystem. If `dest` ends
                with '/' then file will be copied into destination folder, and
                will throw an error if path does not resolve to a directory.
            overwrite (bool): `True` if the contents of any existing file by the
                same name should be overwritten, `False` otherwise.
            fs (FileSystemClient): The FileSystemClient from which to load the
                file/folder at `source`. If not specified, defaults to the local
                filesystem.
        """
        if fs is None:
            from .local import LocalFsClient
            fs = LocalFsClient()
        return fs.download(source, dest, overwrite, self)

    # Magics

    def _register_magics(self, base_name):
        from IPython.core.magic import register_line_magic, register_cell_magic

        @register_line_magic("{}.listdir".format(base_name))
        @process_line_arguments
        def listdir(path=''):
            return self.listdir(path)

        @register_line_magic("{}.showdir".format(base_name))
        @process_line_arguments
        def showdir(path=''):
            return self.showdir(path)

        @register_line_magic("{}.read".format(base_name))
        @process_line_arguments
        def read_file(path):
            return self._file_read(path)

        @register_cell_magic("{}.write".format(base_name))
        @process_line_arguments
        def write_file(cell, path):
            return self._file_write(path, cell)


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
            self.__io_buffer.write(self.fs._file_read(self.path, binary=self.binary_mode))
            if not self.appending:
                self.__io_buffer.seek(0)

    @property
    def mode(self):
        return self.__mode

    @mode.setter
    def mode(self, mode):
        try:
            assert len(set(mode)) == len(mode)
            assert sum(l in mode for l in ['r', 'w', 'a', '+', 't', 'b']) == len(mode)
            assert sum(l in mode for l in ['r', 'w', 'a']) == 1
            assert sum(l in mode for l in ['t', 'b']) < 2
        except AssertionError:
            raise ValueError("invalid mode: '{}'".format(mode))
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

        # For the time being, just write out entire buffer. We can consider something cleverer later.
        offset = self.__io_buffer.tell()
        self.__io_buffer.seek(0)
        self.fs._file_write(self.path, self.__io_buffer.read(), binary=self.binary_mode)
        self.__io_buffer.seek(offset)

        self.__modified = False

    def isatty(self):
        return self.__io_buffer.isatty()

    def read(self, size=-1):
        if not self.readable:
            raise io.UnsupportedOperation("File not open for reading.")
        return self.__io_buffer.read(size)

    def readline(self, size=-1):
        if not self.readable:
            raise io.UnsupportedOperation("File not open for reading.")
        return self.__io_buffer.readline(size)

    def readlines(self, hint=-1):
        if not self.readable:
            raise io.UnsupportedOperation("File not open for reading.")
        return self.__io_buffer.readlines(hint)

    def seek(self, pos, whence=0):
        self.__io_buffer.seek(pos)

    def tell(self):
        return self.__io_buffer.tell()

    def write(self, s):
        if not self.writeable:
            raise io.UnsupportedOperation("File not open for writing.")
        self.__io_buffer.write(s)
        self.__modified = True

    def __iter__(self):
        return self

    def __next__(self):
        return self.readline()

    next = __next__  # Python 2


class FileSystemFileDesc(namedtuple('Node', [
    'fs',
    'path',
    'name',
    'type',
    'bytes',
    'owner',
    'group',
    'permissions',
    'created',
    'last_modified',
    'last_accessed',
    'extra',
])):

    __slots__ = ()

    def __new__(cls, fs, path, name, type, bytes=None, owner=None,
                group=None, permissions=None, created=None, last_modified=None,
                last_accessed=None, **extra):
        assert type in ('directory', 'file')
        return (
            super(FileSystemFileDesc, cls)
            .__new__(cls,
                     fs=fs,
                     path=path,
                     name=name,
                     type=type,
                     bytes=bytes,
                     owner=owner,
                     group=group,
                     permissions=permissions,
                     created=created,
                     last_modified=last_modified,
                     last_accessed=last_accessed,
                     extra=extra)
        )

    def as_dict(self):
        d = OrderedDict([
            ('fs', self.fs),
            ('path', self.path),
            ('type', self.type),
            ('name', self.name),
            ('bytes', self.bytes),
            ('owner', self.owner),
            ('group', self.group),
            ('permissions', self.permissions),
            ('created', self.created),
            ('last_modified', self.last_modified),
            ('last_accessed', self.last_accessed),
        ])
        d.update(self.extra)
        return d

    def open(self, mode='rt'):
        return self.fs.open(self.path, mode=mode)
