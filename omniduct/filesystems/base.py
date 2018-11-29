import io
from abc import abstractmethod
from collections import OrderedDict, namedtuple

import pandas as pd

from omniduct.duct import Duct
from omniduct.utils.docs import quirk_docs
from omniduct.utils.magics import MagicsProvider, process_line_arguments


class FileSystemClient(Duct, MagicsProvider):
    """
    An abstract class providing the common API for all filesystem clients.

    Class Attributes:
        DUCT_TYPE (`Duct.Type`): The type of `Duct` protocol implemented by this class.
        DEFAULT_PORT (int): The default port for the filesystem service (defined
            by subclasses).
    """

    DUCT_TYPE = Duct.Type.FILESYSTEM
    DEFAULT_PORT = None

    @quirk_docs('_init', mro=True)
    def __init__(self, cwd=None, global_writes=False, **kwargs):
        """
        cwd (None, str): The path prefix to use as the current working directory
            (if None, the user's home directory is used where that makes sense).
        global_writes (bool): Whether to allow writes outside of the user's home
            folder.
        **kwargs (dict): Additional keyword arguments to passed on to subclasses.
        """
        Duct.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)
        self._path_cwd = cwd
        self.__path_home = None
        self.global_writes = global_writes
        self._init(**kwargs)

    @abstractmethod
    def _init(self):
        pass

    # Path properties and helpers

    @property
    @quirk_docs('_path_home')
    def path_home(self):
        """
        str: The default path prefix to use for all non-absolute path references
        on this filesystem. This is assumed not to change between connections,
        and so will not be updated on client reconnections.
        """
        if not self.__path_home:
            self.__path_home = self.connect()._path_home()
        return self.__path_home

    @abstractmethod
    def _path_home(self):
        return NotImplementedError

    @property
    def path_cwd(self):
        """
        str: The path prefix associated with the current working directory.
        """
        return self._path_cwd or self.path_home

    @path_cwd.setter
    def path_cwd(self, path_cwd):
        assert self.isdir(self._path(path_cwd)), "Specified path does not exist."
        self._path_cwd = path_cwd

    @property
    @quirk_docs('_path_separator')
    def path_separator(self):
        """
        str: The character(s) to use in separating path components. Typically
        this will be '/'.
        """
        return self._path_separator()

    @abstractmethod
    def _path_separator(self):
        raise NotImplementedError

    def path_join(self, path, *components):
        """
        Generate a new path by joining together multiple paths.

        If any component starts with `self.path_separator` or '~', then all
        previous path components are discarded, and the effective base path
        becomes that component (with '~' expanding to `self.path_home`). Note
        that this method does *not* simplify paths components like '..'. Use
        `self.path_normpath` for this purpose.

        Args:
            path (str): The base path to which components should be joined.
            *components (str): Any additional components to join to the base
                path.

        Returns:
            str: The path resulting from joining all of the components nominated,
            in order, to the base path.
        """
        for component in components:
            if component.startswith('~'):
                path = self.path_home + component[1:]
            elif component.startswith(self.path_separator):
                path = component
            else:
                path = '{}{}{}'.format(path, self.path_separator if not path.endswith(self.path_separator) else '', component)
        return path

    def path_basename(self, path):
        """
        Extract the last component of a given path.

        Components are determined by splitting by `self.path_separator`.
        Note that if a path ends with a path separator, the basename will be
        the empty string.

        Args:
            path (str): The path from which the basename should be extracted.

        Returns:
            str: The extracted basename.
        """
        return self._path(path).split(self.path_separator)[-1]

    def path_dirname(self, path):
        """
        Extract the parent directory for provided path.

        This method returns the entire path except for the basename (the last
        component), where components are determined by splitting by
        `self.path_separator`.

        Args:
            path (str): The path from which the directory path should be
                extracted.

        Returns:
            str: The extracted directory path.
        """
        return self.path_separator.join(self._path(path).split(self.path_separator)[:-1])

    def path_normpath(self, path):
        """
        Normalise a pathname.

        This method returns the normalised (absolute) path corresponding to `path`
        on this filesystem.

        Args:
            path (str): The path to normalise (make absolute).

        Returns:
            str: The normalised path.
        """
        components = self._path(path).split(self.path_separator)
        out_path = []
        for component in components:
            if component == '' and len(out_path) > 0:
                continue
            if component == '.':
                continue
            elif component == '..':
                if len(out_path) > 1:
                    out_path.pop()
                else:
                    raise RuntimeError("Cannot access parent directory of filesystem root.")
            else:
                out_path.append(component)
        if len(out_path) == 1 and out_path[0] == '':
            return '/'
        return self.path_separator.join(out_path)

    def _path(self, path=None):
        return self.path_cwd if path is None else self.path_join(self.path_cwd, path)

    def _path_in_home_dir(self, path):
        return self.path_normpath(path).startswith(self.path_home)

    @property
    def global_writes(self):
        """
        bool: Whether writes should be permitted outside of home directory. This
        write-lock is designed to prevent inadvertent scripted writing in
        potentially dangerous places.
        """
        return self._global_writes

    @global_writes.setter
    def global_writes(self, global_writes):
        self._global_writes = global_writes

    # Filesystem accessors

    @quirk_docs('_exists')
    def exists(self, path):
        """
        Check whether nominated path exists on this filesytem.

        Args:
            path (str): The path for which to check existence.

        Returns:
            bool: `True` if file/folder exists at nominated path, and `False`
                otherwise.
        """
        return self.connect()._exists(self._path(path))

    @abstractmethod
    def _exists(self, path):
        raise NotImplementedError

    @quirk_docs('_isdir')
    def isdir(self, path):
        """
        Check whether a nominated path is directory.

        Args:
            path (str): The path for which to check directory nature.

        Returns:
            bool: `True` if folder exists at nominated path, and `False`
            otherwise.
        """
        return self.connect()._isdir(self._path(path))

    @abstractmethod
    def _isdir(self, path):
        raise NotImplementedError

    @quirk_docs('_isfile')
    def isfile(self, path):
        """
        Check whether a nominated path is a file.

        Args:
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

    @quirk_docs('_dir')
    def dir(self, path=None):
        """
        Retrieve information about the children of a nominated directory.

        This method returns a generator over `FileSystemFileDesc` objects that
        represent the files/directories that a present as children of the
        nominated path. If `path` is not a directory, an exception is raised.
        The path is interpreted as being relative to the current working
        directory (on remote filesytems, this will typically be the home
        folder).

        Args:
            path (str): The path to examine for children.

        Returns:
            generator<FileSystemFileDesc>: The children of `path` represented as
            `FileSystemFileDesc` objects.
        """
        assert self.isdir(path), "'{}' is not a valid directory.".format(path)
        return self.connect()._dir(self._path(path))

    def listdir(self, path=None):
        """
        Retrieve the names of the children of a nomianted directory.

        This method inspects the contents of a directory using `.dir(path)`, and
        returns the names of child members as strings. `path` is interpreted
        relative to the current working directory (on remote filesytems, this
        will typically be the home folder).

        Args:
            path (str): The path of the directory from which to enumerate filenames.

        Returns:
            list<str>: The names of all children of the nominated directory.
        """
        return [f.name for f in self.dir(self._path(path))]

    def showdir(self, path=None):
        """
        Return a dataframe representation of a directory.

        This method returns a `pandas.DataFrame` representation of the contents of
        a path, which are retrieved using `.dir(path)`. The exact columns will
        vary from filesystem to filesystem, depending on the fields returned
        by `.dir()`, but the returned DataFrame is guaranteed to at least have
        the columns: 'name' and 'type'.

        Args:
            path (str): The path of the directory from which to show contents.

        Returns:
            pandas.DataFrame: A DataFrame representation of the contents of the
            nominated directory.
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

    @quirk_docs('_walk')
    def walk(self, path=None):
        """
        Explore the filesystem tree starting at a nominated path.

        This method returns a generator which recursively walks over all paths
        that are children of `path`, one result for each directory, of form:
        (<path name>, [<directory 1>, ...], [<file 1>, ...])

        Args:
            path (str): The path of the directory from which to enumerate
                contents.

        Returns:
            generator<tuple>: A generator of tuples, each tuple being associated
            with one directory that is either `path` or one of its descendants.
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

    @quirk_docs('_find')
    def find(self, path_prefix=None, **attrs):
        """
        Find a file or directory based on certain attributes.

        This method searches for files or folders which satisfy certain
        constraints on the attributes of the file (as encoded into
        `FileSystemFileDesc`). Note that without attribute constraints,
        this method will function identically to `self.dir`.

        Args:
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

    @quirk_docs('_mkdir')
    def mkdir(self, path, recursive=True, exist_ok=False):
        """
        Create a directory at the given path.

        Args:
            path (str): The path of the directory to create.
            recursive (bool): Whether to recursively create any parents of this
                path if they do not already exist.

        Note: `exist_ok` is passed onto subclass implementations of `_mkdir`
        rather that implementing the existence check using `.exists` so that
        they can avoid the overhead associated with multiple operations, which
        can be costly in some cases.
        """
        if not self.global_writes and not self._path_in_home_dir(path):
            raise RuntimeError("Attempt to write outside of home directory without setting {}.global_writes to True.".format(self.name))
        return self.connect()._mkdir(self._path(path), recursive, exist_ok)

    @abstractmethod
    def _mkdir(self, path, recursive, exist_ok):
        raise NotImplementedError

    @quirk_docs('_remove')
    def remove(self, path, recursive=False):
        """
        Remove file(s) at a nominated path.

        Directories (and their contents) will not be removed unless `recursive`
        is set to `True`.

        Args:
            path (str): The path of the file/directory to be removed.
            recursive (bool): Whether to remove directories and all of their
                contents.
        """
        if not self.global_writes and not self._path_in_home_dir(path):
            raise RuntimeError("Attempt to write outside of home directory without setting {}.global_writes to True.".format(self.name))
        if not self.exists(path):
            raise IOError("No file(s) exist at path '{}'.".format(path))
        if self.isdir(path) and not recursive:
            raise IOError("Attempt to remove directory '{}' without passing `recursive=True`.".format(path))
        return self.connect()._remove(self._path(path), recursive)

    @abstractmethod
    def _remove(self, path, recursive):
        raise NotImplementedError

    # File handling

    @quirk_docs('_open')
    def open(self, path, mode='rt'):
        """
        Open a file for reading and/or writing.

        This method opens the file at the given path for reading and/or writing
        operations. The object returned is programmatically interchangeable with
        any other Python file-like object, including specification of file
        modes. If the file is opened in write mode, changes will only be flushed
        to the source filesystem when the file is closed.

        Args:
            path (str): The path of the file to open.
            mode (str): All standard Python file modes.

        Returns:
            FileSystemFile or file-like: An opened file-like object.
        """
        return self.connect()._open(self._path(path), mode=mode)

    def _open(self, path, mode):
        return FileSystemFile(self, path, mode)

    @quirk_docs('_file_read_')
    def _file_read(self, path, size=-1, offset=0, binary=False):
        """
        This method is used by `FileSystemFile` to read the contents of files.
        `._file_read_` may be left unimplemented if `.open()` returns a different
        kind of file handle.

        Args:
            path (str): The path of the file to be read.
            size (int): The number of bytes to read at a time (-1 for max possible).
            offset (int): The offset in bytes from the start of the file.
            binary (bool): Whether to read the file in binary mode.

        Returns:
            str or bytes: The contents of the file.
        """
        return self.connect()._file_read_(self._path(path), size=size, offset=offset, binary=binary)

    def _file_read_(self, path, size=-1, offset=0, binary=False):
        raise NotImplementedError

    @quirk_docs('_file_write_')
    def _file_write(self, path, s, binary=False):
        """
        This method is used by `FileSystemFile` to write to files.
        `._file_write_` may be left unimplemented if `.open()` returns a different
        kind of file handle.

        Args:
            path (str): The path of the file to be read.
            s (str, bytes): The content to be written to the file.
            binary (bool): Whether to read the file in binary mode.

        Returns:
            int: Number of bytes/characters written.
        """
        if not self.global_writes and not self._path_in_home_dir(path):
            raise RuntimeError("Attempt to write outside of home directory without setting {}.global_writes to True.".format(self.name))
        return self.connect()._file_write_(self._path(path), s, binary)

    def _file_write_(self, path, s, binary):
        raise NotImplementedError

    @quirk_docs('_file_append_')
    def _file_append(self, path, s, binary=False):
        """
        This method is used by `FileSystemFile` to append content to files.
        `._file_append_` may be left unimplemented if `.open()` returns a different
        kind of file handle.

        Args:
            path (str): The path of the file to be read.
            s (str, bytes): The content to be appended to the file.
            binary (bool): Whether to read the file in binary mode.

        Returns:
            int: Number of bytes/characters written.
        """
        if not self.global_writes and not self._path_in_home_dir(path):
            raise RuntimeError("Attempt to write outside of home directory without setting {}.global_writes to True.".format(self.name))
        return self.connect()._file_append_(self._path(path), s, binary)

    def _file_append_(self, path, s, binary):
        raise NotImplementedError

    # File transfer

    @quirk_docs('_download')
    def download(self, source, dest=None, overwrite=False, fs=None):
        """
        Download files to another filesystem.

        This method (recursively) downloads a file/folder from path `source` on
        this filesystem to the path `dest` on filesytem `fs`, overwriting any
        existing file if `overwrite` is `True`.

        Args:
            source (str): The path on this filesystem of the file to download to
                the nominated filesystem (`fs`). If `source` ends
                with '/' then contents of the the `source` directory will be
                copied into destination folder, and will throw an error if path
                does not resolve to a directory.
            dest (str): The destination path on filesystem (`fs`). If not
                specified, the file/folder is downloaded into the default path,
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

        # TODO: Consider integration with `odo` for optimised data transfers.

        if fs is None:
            from .local import LocalFsClient
            fs = LocalFsClient()

        source = self._path(source)
        dest = fs._path(dest or self.path_basename(source))

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
            targets.append((source, dest, True))

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
            if target[2] and not fs.isdir(target[1]):
                fs.mkdir(target[1], exist_ok=True)
            elif not target[2]:
                self.connect()._download(target[0], target[1], overwrite, fs)

    def _download(self, source, dest, overwrite, fs):
        if not overwrite and fs.exists(dest):
            raise RuntimeError("File already exists on filesystem.")
        with self.open(source, 'rb') as f_src:
            with fs.open(dest, 'wb') as f_dest:
                f_dest.write(f_src.read())

    def upload(self, source, dest=None, overwrite=False, fs=None):
        """
        Upload files from another filesystem.

        This method (recursively) uploads a file/folder from path `source` on
        filesystem `fs` to the path `dest` on this filesytem, overwriting any
        existing file if `overwrite` is `True`. This is equivalent to
        `fs.download(..., fs=self)`.

        Args:
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
            with self.open(path) as f:
                return f.read()

        @register_cell_magic("{}.write".format(base_name))
        @process_line_arguments
        def write_file(cell, path):
            with self.open(path, 'w') as f:
                f.write(cell)


class FileSystemFile(object):
    """
    A file-like implementation that is interchangeable with native Python file
    objects, allowing remote files to be treated identically to local files
    both by omniduct, the user and other libraries.
    """

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
    def name(self):
        return self.path

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

    @property
    def newlines(self):
        return '\n'  # TODO: Support non-Unix newlines?

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
        line = self.readline()
        if line:
            return line
        else:
            raise StopIteration

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
    """
    A representation of a file/directory stored within an Omniduct
    FileSystemClient.
    """

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

    # Convenience methods

    def open(self, mode='rt'):
        assert self.type == 'file', "`.open(...)` is only appropriate for files."
        return self.fs.open(self.path, mode=mode)

    def dir(self):
        assert self.type == 'directory', "`.dir(...)` is only appropriate for directories."
        return self.fs.dir(self.path)

    def listdir(self):
        assert self.type == 'directory', "`.listdir(...)` is only appropriate for directories."
        return self.fs.listdir(self.path)

    def showdir(self):
        assert self.type == 'directory', "`.showdir(...)` is only appropriate for directories."
        return self.fs.showdir(self.path)

    def find(self, **attrs):
        assert self.type == 'directory', "`.find(...)` is only appropriate for directories."
        return self.fs.find(self.path, **attrs)

    def download(self, dest=None, overwrite=False, fs=None):
        return self.fs.download(self.path, dest=dest, overwrite=overwrite, fs=fs)
