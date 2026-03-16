from __future__ import annotations

import io
from abc import abstractmethod
from collections import OrderedDict, namedtuple
from collections.abc import Generator
from typing import Any, cast

import pandas as pd
from interface_meta import inherit_docs, override

from omniduct.duct import Duct
from omniduct.utils.decorators import require_connection
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
    DEFAULT_PORT: int | None = None

    @inherit_docs("_init", mro=True)
    def __init__(
        self,
        cwd: str | None = None,
        home: str | None = None,
        read_only: bool = False,
        global_writes: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        cwd: The path prefix to use as the current working directory
            (if None, the user's home directory is used where that makes sense).
        home: The path prefix to use as the current users' home
            directory. If not specified, it will default to an implementation-
            specific value (often '/').
        read_only: Whether the filesystem should only be able to perform
            read operations.
        global_writes: Whether to allow writes outside of the user's home
            folder.
        **kwargs: Additional keyword arguments to passed on to subclasses.
        """
        Duct.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)
        self._path_cwd: str | None = cwd
        self.__path_home: str | None = home
        self.read_only = read_only
        self.global_writes = global_writes
        self._init(**kwargs)

    @abstractmethod
    def _init(self) -> None:
        pass

    # Path properties and helpers

    @property
    @inherit_docs("_path_home")
    @require_connection
    def path_home(self) -> str:
        """
        str: The path prefix to use as the current users' home directory. Unless
        `cwd` is set, this will be the prefix to use for all non-absolute path
        references on this filesystem. This is assumed not to change between
        connections, and so will not be updated on client reconnections. Unless
        `global_writes` is set to `True`, this will be the only folder into
        which this client is permitted to write.
        """
        if not self.__path_home:
            self.__path_home = self._path_home()
        return self.__path_home

    @path_home.setter
    def path_home(self, path_home: str | None) -> None:
        if path_home is not None and not path_home.startswith(self.path_separator):
            raise ValueError(
                f"The home path must be absolute. Received: '{path_home}'."
            )
        self.__path_home = path_home

    @abstractmethod
    def _path_home(self) -> str:
        raise NotImplementedError

    @property
    def path_cwd(self) -> str:
        """
        str: The path prefix associated with the current working directory. If
        not otherwise set, it will be the users' home directory, and will be the
        prefix used by all non-absolute path references on this filesystem.
        """
        return self._path_cwd or self.path_home

    @path_cwd.setter
    def path_cwd(self, path_cwd: str) -> None:
        path_cwd = self._path(path_cwd)
        if not self.isdir(path_cwd):
            raise ValueError("Specified path does not exist.")
        self._path_cwd = path_cwd

    @property
    @inherit_docs("_path_separator")
    def path_separator(self) -> str:
        """
        str: The character(s) to use in separating path components. Typically
        this will be '/'.
        """
        return self._path_separator()

    @abstractmethod
    def _path_separator(self) -> str:
        raise NotImplementedError

    def path_join(self, path: str, *components: str) -> str:
        """
        Generate a new path by joining together multiple paths.

        If any component starts with `self.path_separator` or '~', then all
        previous path components are discarded, and the effective base path
        becomes that component (with '~' expanding to `self.path_home`). Note
        that this method does *not* simplify paths components like '..'. Use
        `self.path_normpath` for this purpose.

        Args:
            path: The base path to which components should be joined.
            *components: Any additional components to join to the base
                path.

        Returns:
            The path resulting from joining all of the components nominated,
            in order, to the base path.
        """
        for component in components:
            if component.startswith("~"):
                path = self.path_home + component[1:]
            elif component.startswith(self.path_separator):
                path = component
            else:
                path = f"{path}{self.path_separator if not path.endswith(self.path_separator) else ''}{component}"
        return path

    def path_basename(self, path: str) -> str:
        """
        Extract the last component of a given path.

        Components are determined by splitting by `self.path_separator`.
        Note that if a path ends with a path separator, the basename will be
        the empty string.

        Args:
            path: The path from which the basename should be extracted.

        Returns:
            The extracted basename.
        """
        return self._path(path).split(self.path_separator)[-1]

    def path_dirname(self, path: str) -> str:
        """
        Extract the parent directory for provided path.

        This method returns the entire path except for the basename (the last
        component), where components are determined by splitting by
        `self.path_separator`.

        Args:
            path: The path from which the directory path should be
                extracted.

        Returns:
            The extracted directory path.
        """
        return cast(
            str,
            self.path_separator.join(self._path(path).split(self.path_separator)[:-1]),
        )

    def path_normpath(self, path: str) -> str:
        """
        Normalise a pathname.

        This method returns the normalised (absolute) path corresponding to `path`
        on this filesystem.

        Args:
            path: The path to normalise (make absolute).

        Returns:
            The normalised path.
        """
        components = self._path(path).split(self.path_separator)
        out_path: list[str] = []
        for component in components:
            if component == "" and len(out_path) > 0:
                continue
            if component == ".":
                continue
            if component == "..":
                if len(out_path) > 1:
                    out_path.pop()
                else:
                    raise RuntimeError(
                        "Cannot access parent directory of filesystem root."
                    )
            else:
                out_path.append(component)
        if len(out_path) == 1 and out_path[0] == "":
            return cast(str, self.path_separator)
        return cast(str, self.path_separator.join(out_path))

    def _path(self, path: str | None = None) -> str:
        return self.path_cwd if path is None else self.path_join(self.path_cwd, path)

    def _path_in_home_dir(self, path: str) -> bool:
        return self.path_normpath(path).startswith(self.path_home)

    @property
    def read_only(self) -> bool:
        """
        bool: Whether this filesystem client should be permitted to attempt any
        write operations.
        """
        return self._read_only

    @read_only.setter
    def read_only(self, read_only: bool) -> None:
        self._read_only = read_only

    @property
    def global_writes(self) -> bool:
        """
        bool: Whether writes should be permitted outside of home directory. This
        write-lock is designed to prevent inadvertent scripted writing in
        potentially dangerous places.
        """
        return self._global_writes

    @global_writes.setter
    def global_writes(self, global_writes: bool) -> None:
        self._global_writes = global_writes

    def _assert_path_is_writable(self, path: str) -> bool:
        if self.read_only:
            raise RuntimeError(
                f"This filesystem client is configured for read-only access. Set `{self.name}`.`read_only` to `False` to override."
            )
        if not self.global_writes and not self._path_in_home_dir(path):
            raise RuntimeError(
                f"Attempt to write outside of home directory without setting `{self.name}`.`global_writes` to `True`."
            )
        return True

    # Filesystem accessors

    @inherit_docs("_exists")
    @require_connection
    def exists(self, path: str) -> bool:
        """
        Check whether nominated path exists on this filesytem.

        Args:
            path: The path for which to check existence.

        Returns:
            `True` if file/folder exists at nominated path, and `False`
                otherwise.
        """
        return self._exists(self._path(path))

    @abstractmethod
    def _exists(self, path: str) -> bool:
        raise NotImplementedError

    @inherit_docs("_isdir")
    @require_connection
    def isdir(self, path: str) -> bool:
        """
        Check whether a nominated path is directory.

        Args:
            path: The path for which to check directory nature.

        Returns:
            `True` if folder exists at nominated path, and `False`
            otherwise.
        """
        return self._isdir(self._path(path))

    @abstractmethod
    def _isdir(self, path: str) -> bool:
        raise NotImplementedError

    @inherit_docs("_isfile")
    @require_connection
    def isfile(self, path: str) -> bool:
        """
        Check whether a nominated path is a file.

        Args:
            path: The path for which to check file nature.

        Returns:
            `True` if a file exists at nominated path, and `False`
            otherwise.
        """
        return self._isfile(self._path(path))

    def _isfile(self, path: str) -> bool:
        return not self._isdir(path)

    # Directory handling

    @abstractmethod
    def _dir(self, path: str) -> Generator[FileSystemFileDesc, None, None]:
        """
        This method should return a generator over `FileSystemFileDesc` objects.
        """
        raise NotImplementedError

    @inherit_docs("_dir")
    @require_connection
    def dir(self, path: str | None = None) -> Generator[FileSystemFileDesc, None, None]:
        """
        Retrieve information about the children of a nominated directory.

        This method returns a generator over `FileSystemFileDesc` objects that
        represent the files/directories that a present as children of the
        nominated path. If `path` is not a directory, an exception is raised.
        The path is interpreted as being relative to the current working
        directory (on remote filesytems, this will typically be the home
        folder).

        Args:
            path: The path to examine for children.

        Returns:
            The children of `path` represented as `FileSystemFileDesc` objects.
        """
        if not self.isdir(path):
            raise ValueError(f"'{path}' is not a valid directory.")
        return self._dir(self._path(path))

    def listdir(self, path: str | None = None) -> list[str]:
        """
        Retrieve the names of the children of a nomianted directory.

        This method inspects the contents of a directory using `.dir(path)`, and
        returns the names of child members as strings. `path` is interpreted
        relative to the current working directory (on remote filesytems, this
        will typically be the home folder).

        Args:
            path: The path of the directory from which to enumerate filenames.

        Returns:
            The names of all children of the nominated directory.
        """
        return [f.name for f in self.dir(self._path(path))]

    @require_connection
    def showdir(self, path: str | None = None) -> pd.DataFrame | str:
        """
        Return a dataframe representation of a directory.

        This method returns a `pandas.DataFrame` representation of the contents of
        a path, which are retrieved using `.dir(path)`. The exact columns will
        vary from filesystem to filesystem, depending on the fields returned
        by `.dir()`, but the returned DataFrame is guaranteed to at least have
        the columns: 'name' and 'type'.

        Args:
            path: The path of the directory from which to show contents.

        Returns:
            A DataFrame representation of the contents of the
            nominated directory.
        """
        if not self.isdir(path):
            raise ValueError(f"'{path}' is not a valid directory.")
        return self._showdir(self._path(path))

    def _showdir(self, path: str) -> pd.DataFrame | str:
        data = [f.as_dict() for f in self._dir(path)]
        if len(data) > 0:
            return (  # type: ignore[no-any-return]
                pd.DataFrame(data)
                .sort_values(["type", "name"])
                .reset_index(drop=True)
                .dropna(axis="columns", how="all")
                .drop(axis=1, labels=["fs", "path"])
            )
        return "Directory has no contents."

    @inherit_docs("_walk")
    @require_connection
    def walk(
        self, path: str | None = None
    ) -> Generator[tuple[str, list[str], list[str]], None, None]:
        """
        Explore the filesystem tree starting at a nominated path.

        This method returns a generator which recursively walks over all paths
        that are children of `path`, one result for each directory, of form:
        (<path name>, [<directory 1>, ...], [<file 1>, ...])

        Args:
            path: The path of the directory from which to enumerate
                contents.

        Returns:
            A generator of tuples, each tuple being associated
            with one directory that is either `path` or one of its descendants.
        """
        if not self.isdir(path):
            raise ValueError(f"'{path}' is not a valid directory.")
        return self._walk(self._path(path))

    def _walk(
        self, path: str
    ) -> Generator[tuple[str, list[str], list[str]], None, None]:
        dirs: list[str] = []
        files: list[str] = []
        for f in self._dir(path):
            if f.type == "directory":
                dirs.append(f.name)
            else:
                files.append(f.name)
        yield (path, dirs, files)

        for dirname in dirs:
            yield from self._walk(
                self._path(self.path_join(path, dirname))
            )  # Note: using _walk directly here, which may fail if disconnected during walk.

    @inherit_docs("_find")
    @require_connection
    def find(
        self, path_prefix: str | None = None, **attrs: Any
    ) -> Generator[FileSystemFileDesc, None, None]:
        """
        Find a file or directory based on certain attributes.

        This method searches for files or folders which satisfy certain
        constraints on the attributes of the file (as encoded into
        `FileSystemFileDesc`). Note that without attribute constraints,
        this method will function identically to `self.dir`.

        Args:
            path_prefix: The path under which files/directories should be
                found.
            **attrs: Constraints on the fields of the `FileSystemFileDesc`
                objects associated with this filesystem, as constant values or
                callable objects (in which case the object will be called and
                should return True if attribute value is match, and False
                otherwise).

        Returns:
            A generator over `FileSystemFileDesc`
                objects that are descendents of `path_prefix` and which statisfy
                provided constraints.
        """
        if not self.isdir(path_prefix):
            raise ValueError(
                f"'{path_prefix}' is not a valid directory. Did you mean `.find(name='{path_prefix}')`?"
            )
        return self._find(self._path(path_prefix), **attrs)

    def _find(
        self, path_prefix: str, **attrs: Any
    ) -> Generator[FileSystemFileDesc, None, None]:
        def is_match(f: FileSystemFileDesc) -> bool:
            for attr, value in attrs.items():
                if hasattr(value, "__call__") and not value(f.as_dict().get(attr)):
                    return False
                if value != f.as_dict().get(attr):
                    return False
            return True

        dirs: list[str] = []
        for f in self._dir(path_prefix):
            if f.type == "directory":
                dirs.append(f.name)
            if is_match(f):
                yield f

        for dirname in dirs:
            yield from self._find(
                self._path(self.path_join(path_prefix, dirname)), **attrs
            )  # Note: using _find directly here, which may fail if disconnected during find.

    @inherit_docs("_mkdir")
    @require_connection
    def mkdir(self, path: str, recursive: bool = True, exist_ok: bool = False) -> None:
        """
        Create a directory at the given path.

        Args:
            path: The path of the directory to create.
            recursive: Whether to recursively create any parents of this
                path if they do not already exist.

        Note: `exist_ok` is passed onto subclass implementations of `_mkdir`
        rather that implementing the existence check using `.exists` so that
        they can avoid the overhead associated with multiple operations, which
        can be costly in some cases.
        """
        self._assert_path_is_writable(path)
        return self._mkdir(self._path(path), recursive, exist_ok)

    @abstractmethod
    def _mkdir(self, path: str, recursive: bool, exist_ok: bool) -> None:
        raise NotImplementedError

    @inherit_docs("_remove")
    @require_connection
    def remove(self, path: str, recursive: bool = False) -> None:
        """
        Remove file(s) at a nominated path.

        Directories (and their contents) will not be removed unless `recursive`
        is set to `True`.

        Args:
            path: The path of the file/directory to be removed.
            recursive: Whether to remove directories and all of their
                contents.
        """
        self._assert_path_is_writable(path)
        if not self.exists(path):
            raise OSError(f"No file(s) exist at path '{path}'.")
        if self.isdir(path) and not recursive:
            raise OSError(
                f"Attempt to remove directory '{path}' without passing `recursive=True`."
            )
        return self._remove(self._path(path), recursive)

    @abstractmethod
    def _remove(self, path: str, recursive: bool) -> None:
        raise NotImplementedError

    # File handling

    @inherit_docs("_open")
    @require_connection
    def open(self, path: str, mode: str = "rt") -> FileSystemFile:
        """
        Open a file for reading and/or writing.

        This method opens the file at the given path for reading and/or writing
        operations. The object returned is programmatically interchangeable with
        any other Python file-like object, including specification of file
        modes. If the file is opened in write mode, changes will only be flushed
        to the source filesystem when the file is closed.

        Args:
            path: The path of the file to open.
            mode: All standard Python file modes.

        Returns:
            An opened file-like object.
        """
        if "w" in mode or "a" in mode or "+" in mode:
            self._assert_path_is_writable(path)
        return self._open(self._path(path), mode=mode)

    def _open(self, path: str, mode: str) -> FileSystemFile:
        return FileSystemFile(self, path, mode)

    @inherit_docs("_file_read_")
    @require_connection
    def _file_read(
        self, path: str, size: int = -1, offset: int = 0, binary: bool = False
    ) -> str | bytes:
        """
        This method is used by `FileSystemFile` to read the contents of files.
        `._file_read_` may be left unimplemented if `.open()` returns a different
        kind of file handle.

        Args:
            path: The path of the file to be read.
            size: The number of bytes to read at a time (-1 for max possible).
            offset: The offset in bytes from the start of the file.
            binary: Whether to read the file in binary mode.

        Returns:
            The contents of the file.
        """
        return self._file_read_(
            self._path(path), size=size, offset=offset, binary=binary
        )

    def _file_read_(
        self, path: str, size: int = -1, offset: int = 0, binary: bool = False
    ) -> str | bytes:
        raise NotImplementedError

    @inherit_docs("_file_write_")
    @require_connection
    def _file_write(self, path: str, s: str | bytes, binary: bool = False) -> int:
        """
        This method is used by `FileSystemFile` to write to files.
        `._file_write_` may be left unimplemented if `.open()` returns a different
        kind of file handle.

        Args:
            path: The path of the file to be read.
            s: The content to be written to the file.
            binary: Whether to read the file in binary mode.

        Returns:
            Number of bytes/characters written.
        """
        self._assert_path_is_writable(path)
        return self._file_write_(self._path(path), s, binary)

    def _file_write_(self, path: str, s: str | bytes, binary: bool) -> int:
        raise NotImplementedError

    @inherit_docs("_file_append_")
    @require_connection
    def _file_append(self, path: str, s: str | bytes, binary: bool = False) -> int:
        """
        This method is used by `FileSystemFile` to append content to files.
        `._file_append_` may be left unimplemented if `.open()` returns a different
        kind of file handle.

        Args:
            path: The path of the file to be read.
            s: The content to be appended to the file.
            binary: Whether to read the file in binary mode.

        Returns:
            Number of bytes/characters written.
        """
        self._assert_path_is_writable(path)
        return self._file_append_(self._path(path), s, binary)

    def _file_append_(self, path: str, s: str | bytes, binary: bool) -> int:
        raise NotImplementedError

    # File transfer

    @inherit_docs("_download")
    def download(
        self,
        source: str,
        dest: str | None = None,
        overwrite: bool = False,
        fs: FileSystemClient | None = None,
    ) -> None:
        """
        Download files to another filesystem.

        This method (recursively) downloads a file/folder from path `source` on
        this filesystem to the path `dest` on filesytem `fs`, overwriting any
        existing file if `overwrite` is `True`.

        Args:
            source: The path on this filesystem of the file to download to
                the nominated filesystem (`fs`). If `source` ends
                with '/' then contents of the the `source` directory will be
                copied into destination folder, and will throw an error if path
                does not resolve to a directory.
            dest: The destination path on filesystem (`fs`). If not
                specified, the file/folder is downloaded into the default path,
                usually one's home folder. If `dest` ends with '/',
                and corresponds to a directory, the contents of source will be
                copied instead of copying the entire folder. If `dest` is
                otherwise a directory, an exception will be raised.
            overwrite: `True` if the contents of any existing file by the
                same name should be overwritten, `False` otherwise.
            fs: The FileSystemClient into which the nominated
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
            if not fs.isdir(dest):
                raise ValueError(f"No such directory `{dest}`")
            if not source.endswith(self.path_separator):
                dest = fs.path_join(fs._path(dest), self.path_basename(source))

        # A mapping of source to dest paths on the respective filesystems
        # In format: (source, dest, isdir?)
        targets: list[tuple[str, str, bool]] = []

        if self.isdir(source):
            target_prefix = (
                source
                if source.endswith(self.path_separator)
                else source + self.path_separator
            )
            targets.append((source, dest, True))

            for path, dirs, files in self.walk(source):
                for dirname in dirs:
                    target_source = self.path_join(path, dirname)
                    targets.append(
                        (
                            target_source,
                            fs.path_join(
                                dest,
                                *target_source[len(target_prefix) :].split(
                                    self.path_separator
                                ),
                            ),
                            True,
                        )
                    )
                for file in files:
                    target_source = self.path_join(path, file)
                    targets.append(
                        (
                            target_source,
                            fs.path_join(
                                dest,
                                *target_source[len(target_prefix) :].split(
                                    self.path_separator
                                ),
                            ),
                            False,
                        )
                    )
        else:
            targets.append((source, dest, False))

        for target in targets:
            if target[2] and not fs.isdir(target[1]):
                fs.mkdir(target[1], exist_ok=True)
            elif not target[2]:
                self._download(target[0], target[1], overwrite, fs)

    def _download(
        self, source: str, dest: str, overwrite: bool, fs: FileSystemClient
    ) -> None:
        if not overwrite and fs.exists(dest):
            raise RuntimeError("File already exists on filesystem.")
        with self.open(source, "rb") as f_src:
            with fs.open(dest, "wb") as f_dest:
                f_dest.write(f_src.read())

    def upload(
        self,
        source: str,
        dest: str | None = None,
        overwrite: bool = False,
        fs: FileSystemClient | None = None,
    ) -> None:
        """
        Upload files from another filesystem.

        This method (recursively) uploads a file/folder from path `source` on
        filesystem `fs` to the path `dest` on this filesytem, overwriting any
        existing file if `overwrite` is `True`. This is equivalent to
        `fs.download(..., fs=self)`.

        Args:
            source: The path on the specified filesystem (`fs`) of the
                file to upload to this filesystem. If `source` ends with '/',
                and corresponds to a directory, the contents of source will be
                copied instead of copying the entire folder.
            dest: The destination path on this filesystem. If not
                specified, the file/folder is uploaded into the default path,
                usually one's home folder, on this filesystem. If `dest` ends
                with '/' then file will be copied into destination folder, and
                will throw an error if path does not resolve to a directory.
            overwrite: `True` if the contents of any existing file by the
                same name should be overwritten, `False` otherwise.
            fs: The FileSystemClient from which to load the
                file/folder at `source`. If not specified, defaults to the local
                filesystem.
        """
        if fs is None:
            from .local import LocalFsClient

            fs = LocalFsClient()
        fs.download(source, dest, overwrite, self)

    # Magics
    @override
    def _register_magics(self, base_name: str) -> None:
        from IPython.core.magic import register_cell_magic, register_line_magic

        @register_line_magic(f"{base_name}.listdir")
        @process_line_arguments
        def listdir(path: str = "") -> list[str]:
            return self.listdir(path)

        @register_line_magic(f"{base_name}.showdir")
        @process_line_arguments
        def showdir(path: str = "") -> pd.DataFrame | str:
            return cast(pd.DataFrame | str, self.showdir(path))

        @register_line_magic(f"{base_name}.read")
        @process_line_arguments
        def read_file(path: str) -> str | bytes:
            with self.open(path) as f:
                return cast(str | bytes, f.read())

        @register_cell_magic(f"{base_name}.write")
        @process_line_arguments
        def write_file(cell: str, path: str) -> None:
            with self.open(path, "w") as f:
                f.write(cell)

    # PyArrow compat
    @property
    def pyarrow_fs(self) -> Any:
        from ._pyarrow_compat import OmniductFileSystem

        return OmniductFileSystem(self)


class FileSystemFile:
    """
    A file-like implementation that is interchangeable with native Python file
    objects, allowing remote files to be treated identically to local files
    both by omniduct, the user and other libraries.
    """

    fs: FileSystemClient
    path: str
    offset: int
    closed: bool

    def __init__(self, fs: FileSystemClient, path: str, mode: str = "r") -> None:
        self.fs = fs
        self.path = path
        self.mode = mode
        self.offset = 0
        self.closed = False
        self.__modified: bool = False

        if self.binary_mode:
            self.__io_buffer: io.BytesIO | io.StringIO = io.BytesIO()
        else:
            self.__io_buffer = io.StringIO()

        if "w" not in self.mode:
            self.__io_buffer.write(
                self.fs._file_read(self.path, binary=self.binary_mode)
            )
            if not self.appending:
                self.__io_buffer.seek(0)

    @property
    def name(self) -> str:
        return self.path

    @property
    def mode(self) -> str:
        return self.__mode

    @mode.setter
    def mode(self, mode: str) -> None:
        if (
            len(set(mode)) != len(mode)
            or sum(opt in mode for opt in ["r", "w", "a", "+", "t", "b"]) != len(mode)
            or sum(opt in mode for opt in ["r", "w", "a"]) != 1
            or sum(opt in mode for opt in ["t", "b"]) >= 2
        ):
            raise ValueError(f"invalid mode: '{mode}'")
        self.__mode = mode

    @property
    def readable(self) -> bool:
        return "r" in self.mode or "+" in self.mode

    @property
    def writable(self) -> bool:
        return "w" in self.mode or "a" in self.mode or "+" in self.mode

    @property
    def seekable(self) -> bool:
        return True

    @property
    def appending(self) -> bool:
        return "a" in self.mode

    @property
    def binary_mode(self) -> bool:
        return "b" in self.mode

    def __enter__(self) -> FileSystemFile:
        return self

    def __exit__(self, type: Any, value: Any, tb: Any) -> None:
        self.close()

    def close(self) -> None:
        self.flush()
        self.closed = True

    def __del__(self) -> None:
        self.close()

    def flush(self) -> None:
        if not self.writable or not self.__modified:
            return

        # For the time being, just write out entire buffer. We can consider something cleverer later.
        offset = self.__io_buffer.tell()
        self.__io_buffer.seek(0)
        self.fs._file_write(self.path, self.__io_buffer.read(), binary=self.binary_mode)
        self.__io_buffer.seek(offset)

        self.__modified = False

    def isatty(self) -> bool:
        return self.__io_buffer.isatty()

    @property
    def newlines(self) -> str:
        return "\n"  # TODO: Support non-Unix newlines?

    def read(self, size: int = -1) -> str | bytes:
        if not self.readable:
            raise io.UnsupportedOperation("File not open for reading.")
        return self.__io_buffer.read(size)

    def readline(self, size: int = -1) -> str | bytes:
        if not self.readable:
            raise io.UnsupportedOperation("File not open for reading.")
        return self.__io_buffer.readline(size)

    def readlines(self, hint: int = -1) -> list[str] | list[bytes]:
        if not self.readable:
            raise io.UnsupportedOperation("File not open for reading.")
        return self.__io_buffer.readlines(hint)

    def seek(self, pos: int, whence: int = 0) -> int:
        return self.__io_buffer.seek(pos, whence)

    def tell(self) -> int:
        return self.__io_buffer.tell()

    def write(self, s: str | bytes) -> None:
        if not self.writable:
            raise io.UnsupportedOperation("File not open for writing.")
        if isinstance(self.__io_buffer, io.BytesIO):
            self.__io_buffer.write(s.encode() if isinstance(s, str) else s)
        else:
            self.__io_buffer.write(s.decode() if isinstance(s, bytes) else s)
        self.__modified = True

    def __iter__(self) -> FileSystemFile:
        return self

    def __next__(self) -> str | bytes:
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    next = __next__  # Python 2

    # Additional methods from BufferedIOBase for compatibility

    def read1(self, size: int = -1) -> str | bytes:
        return self.read(size)

    def detach(self) -> None:
        raise io.UnsupportedOperation()

    def readinto(self, buffer: Any) -> int:
        data = self.read()
        buffer[: len(data)] = data
        return len(data)

    def readinto1(self, buffer: Any) -> int:
        return self.readinto(buffer)


class FileSystemFileDesc(
    namedtuple(
        "Node",
        [
            "fs",
            "path",
            "name",
            "type",
            "bytes",
            "owner",
            "group",
            "permissions",
            "created",
            "last_modified",
            "last_accessed",
            "extra",
        ],
    )
):
    """
    A representation of a file/directory stored within an Omniduct
    FileSystemClient.
    """

    __slots__ = ()

    def __new__(
        cls,
        fs: FileSystemClient,
        path: str,
        name: str,
        type: str,
        bytes: int | None = None,
        owner: str | None = None,
        group: str | None = None,
        permissions: str | None = None,
        created: Any | None = None,
        last_modified: Any | None = None,
        last_accessed: Any | None = None,
        **extra: Any,
    ) -> FileSystemFileDesc:
        if type not in ("directory", "file"):
            raise ValueError(f"Invalid type {type!r}: must be 'directory' or 'file'.")
        return super().__new__(
            cls,
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
            extra=extra,
        )

    def as_dict(self) -> OrderedDict:
        d = OrderedDict(
            [
                ("fs", self.fs),
                ("path", self.path),
                ("type", self.type),
                ("name", self.name),
                ("bytes", self.bytes),
                ("owner", self.owner),
                ("group", self.group),
                ("permissions", self.permissions),
                ("created", self.created),
                ("last_modified", self.last_modified),
                ("last_accessed", self.last_accessed),
            ]
        )
        d.update(self.extra)
        return d

    # Convenience methods

    def open(self, mode: str = "rt") -> io.IOBase:
        if self.type != "file":
            raise TypeError("`.open(...)` is only appropriate for files.")
        return self.fs.open(self.path, mode=mode)  # type: ignore[no-any-return]

    def dir(self) -> Generator[FileSystemFileDesc, None, None]:
        if self.type != "directory":
            raise TypeError("`.dir(...)` is only appropriate for directories.")
        return self.fs.dir(self.path)  # type: ignore[no-any-return]

    def listdir(self) -> list[str]:
        if self.type != "directory":
            raise TypeError("`.listdir(...)` is only appropriate for directories.")
        return self.fs.listdir(self.path)  # type: ignore[no-any-return]

    def showdir(self) -> pd.DataFrame | str:
        if self.type != "directory":
            raise TypeError("`.showdir(...)` is only appropriate for directories.")
        return self.fs.showdir(self.path)  # type: ignore[no-any-return]

    def find(self, **attrs: Any) -> Generator[FileSystemFileDesc, None, None]:
        if self.type != "directory":
            raise TypeError("`.find(...)` is only appropriate for directories.")
        return self.fs.find(self.path, **attrs)  # type: ignore[no-any-return]

    def download(
        self,
        dest: str | None = None,
        overwrite: bool = False,
        fs: FileSystemClient | None = None,
    ) -> None:
        self.fs.download(self.path, dest=dest, overwrite=overwrite, fs=fs)
