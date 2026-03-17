from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyarrow.filesystem import FileSystem
from pyarrow.util import _stringify_path, implements

if TYPE_CHECKING:
    import pandas as pd

    from omniduct.filesystems.base import FileSystemClient


class OmniductFileSystem(FileSystem):
    """
    Wraps Omniduct filesystem implementations for use with PyArrow.
    """

    fs: FileSystemClient

    def __init__(self, fs: FileSystemClient) -> None:
        self.fs = fs

    @implements(FileSystem.isdir)
    def isdir(self, path: Any) -> bool:
        return self.fs.isdir(_stringify_path(path))  # type: ignore[no-any-return]

    @implements(FileSystem.isfile)
    def isfile(self, path: Any) -> bool:
        return self.fs.isfile(_stringify_path(path))  # type: ignore[no-any-return]

    @implements(FileSystem._isfilestore)
    def _isfilestore(self) -> bool:
        return True

    @implements(FileSystem.delete)
    def delete(self, path: Any, recursive: bool = False) -> None:
        self.fs.remove(_stringify_path(path), recursive=recursive)

    @implements(FileSystem.exists)
    def exists(self, path: Any) -> bool:
        return self.fs.exists(_stringify_path(path))  # type: ignore[no-any-return]

    @implements(FileSystem.mkdir)
    def mkdir(self, path: Any, create_parents: bool = True) -> None:
        self.fs.mkdir(_stringify_path(path), recursive=create_parents)

    @implements(FileSystem.open)
    def open(self, path: Any, mode: str = "rb") -> Any:
        return self.fs.open(_stringify_path(path), mode=mode)

    @implements(FileSystem.ls)
    def ls(self, path: Any, detail: bool = False) -> list[str] | pd.DataFrame:
        path = _stringify_path(path)
        if detail:
            return self.showdir(path)  # type: ignore[no-any-return]
        return self.listdir(path)  # type: ignore[no-any-return]

    def walk(self, path: Any) -> Any:
        return self.fs.walk(_stringify_path(path))
