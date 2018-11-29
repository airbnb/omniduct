import pickle

from omniduct.caches._serializers import Serializer


class CursorSerializer(Serializer):
    """
    Serializes and deserializes cursor objects for use with the Cache.
    """

    @property
    def file_extension(self):
        """str: The file extension to use when storing in the cache."""
        return ".pickled_cursor"

    def serialize(self, cursor, fh):
        """
        Serialize a cursor object into a nominated file handle.

        Args:
            cursor (DB-API 2.0 cursor): The cursor to serialize.
            fh (binary file handle): A file-like object opened in binary mode
                capable of being written into.
        """
        description = cursor.description
        rows = cursor.fetchall()
        pickle.dump((description, rows), fh)

    def deserialize(self, fh):
        """
        Deserialize a cursor object into a DB-API 2.0 compatible cursor object.

        Args:
            fh (binary file handle): A file-like object from which serialized
                data can be read.

        Returns:
            CachedCursor: A CacheCursor object representing a previously
                serialized cursor.
        """
        description, rows = pickle.load(fh)
        return CachedCursor(description, rows)


class CachedCursor(object):
    """
    A DB-API 2.0 cursor implementation atop of static data.

    This class is used to present reconstituted data cached from a cursor object
    in a form compatible with the original cursor object.
    """

    def __init__(self, description, rows):
        self._description = description
        self._rows = rows

        self._iter = None

    @property
    def iter(self):
        if not getattr(self, '_iter'):
            self._iter = (row for row in self._rows)
        return self._iter

    arraysize = 1

    @property
    def description(self):
        return self._description

    @property
    def row_count(self):
        return -1

    def close(self):
        pass

    def execute(operation, parameters=None):
        raise NotImplementedError(
            "Cached cursors are not connected to a database, and cannot be "
            "used for database operations."
        )

    def executemany(operation, seq_of_parameters=None):
        raise NotImplementedError(
            "Cached cursors are not connected to a database, and cannot be "
            "used for database operations."
        )

    def fetchone(self):
        return next(self.iter)

    def fetchmany(self, size=None):
        size = size or self.arraysize
        return [self.fetchone() for _ in range(size)]

    def fetchall(self):
        return list(self.iter)

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass
