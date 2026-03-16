from __future__ import annotations

import pickle
from collections.abc import Generator
from typing import IO, Any

from omniduct.caches._serializers import Serializer


class CursorSerializer(Serializer):
    """
    Serializes and deserializes cursor objects for use with the Cache.
    """

    @property
    def file_extension(self) -> str:
        """str: The file extension to use when storing in the cache."""
        return ".pickled_cursor"

    def serialize(self, obj: Any, fh: IO[bytes]) -> None:
        """
        Serialize a cursor object into a nominated file handle.

        Args:
            obj: The cursor to serialize.
            fh: A file-like object opened in binary mode capable of being
                written into.
        """
        description = obj.description
        rows = obj.fetchall()
        pickle.dump((description, rows), fh)

    def deserialize(self, fh: IO[bytes]) -> CachedCursor:
        """
        Deserialize a cursor object into a DB-API 2.0 compatible cursor object.

        Args:
            fh: A file-like object from which serialized data can be read.

        Returns:
            A CachedCursor object representing a previously serialized cursor.
        """
        description, rows = pickle.load(fh)  # noqa: S301
        return CachedCursor(description, rows)


class CachedCursor:
    """
    A DB-API 2.0 cursor implementation atop of static data.

    This class is used to present reconstituted data cached from a cursor object
    in a form compatible with the original cursor object.
    """

    _description: Any
    _rows: list[Any]
    _iter: Generator[Any, None, None] | None

    arraysize: int = 1

    def __init__(self, description: Any, rows: list[Any]) -> None:
        self._description = description
        self._rows = rows

        self._iter = None

    @property
    def iter(self) -> Generator[Any, None, None]:
        if not getattr(self, "_iter"):
            self._iter = (row for row in self._rows)
        if self._iter is None:
            raise RuntimeError("Iterator could not be initialized.")
        return self._iter

    @property
    def description(self) -> Any:
        return self._description

    @property
    def row_count(self) -> int:
        return -1

    def close(self) -> None:
        pass

    def execute(self, operation: Any, parameters: Any = None) -> None:
        raise NotImplementedError(
            "Cached cursors are not connected to a database, and cannot be "
            "used for database operations."
        )

    def executemany(self, operation: Any, seq_of_parameters: Any = None) -> None:
        raise NotImplementedError(
            "Cached cursors are not connected to a database, and cannot be "
            "used for database operations."
        )

    def fetchone(self) -> Any:
        return next(self.iter)

    def fetchmany(self, size: int | None = None) -> list[Any]:
        size = size or self.arraysize
        return [self.fetchone() for _ in range(size)]

    def fetchall(self) -> list[Any]:
        return list(self.iter)

    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        pass
