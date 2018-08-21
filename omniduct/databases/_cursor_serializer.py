import pickle

from omniduct.caches._serializers import Serializer


class CursorSerializer(Serializer):

    @property
    def file_extension(self):
        return ".pickled_cursor"

    def serialize(self, cursor, fh):
        description = cursor.description
        rows = cursor.fetchall()
        return pickle.dump((description, rows), fh)

    def deserialize(self, fh):
        description, rows = pickle.load(fh)
        return CachedCursor(description, rows)


class CachedCursor(object):
    """
    A DBAPI2 compatible cursor for presenting reconstituted data cached from a
    cursor object, allowing downstream formatting to execute as normal.
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
