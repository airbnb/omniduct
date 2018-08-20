import hashlib

import pandas as pd
import pytest

from omniduct.databases.base import DatabaseClient


class DummyDatabaseClient(DatabaseClient):

    PROTOCOLS = []
    DEFAULT_PORT = None

    def _init(self):
        pass

    # Connection management

    def _connect(self):
        pass

    def _is_connected(self):
        return True

    def _disconnect(self):
        pass

    # Database operations

    def _execute(self, statement, cursor, wait, session_properties, **kwargs):
        return DummyCursor()

    def _table_list(self, namespace, **kwargs):
        raise NotImplementedError

    def _table_drop(self, table, **kwargs):
        pass

    def _table_exists(self, table, **kwargs):
        raise NotImplementedError

    def _table_desc(self, table, **kwargs):
        raise NotImplementedError

    def _table_head(self, table, n=10, **kwargs):
        raise NotImplementedError

    def _table_props(self, table, **kwargs):
        raise NotImplementedError


class DummyCursor(object):
    """
    This DBAPI2 compatible cursor wrapped around a Pandas DataFrame
    """

    def __init__(self):
        self.df = pd.DataFrame(
            {'field1': list(range(10)), 'field2': list('abcdefghij')}
        )
        self._df_iter = None

    @property
    def df_iter(self):
        if not getattr(self, '_df_iter'):
            self._df_iter = (tuple(row) for i, row in self.df.iterrows())
        return self._df_iter

    arraysize = 1

    @property
    def description(self):
        return tuple([
            (name, None, None, None, None, None, None)
            for name in self.df.columns
        ])

    @property
    def row_count(self):
        return -1

    def close(self):
        pass

    def execute(operation, parameters=None):
        pass

    def executemany(operation, seq_of_parameters=None):
        pass

    def fetchone(self):
        return next(self.df_iter)

    def fetchmany(self, size=None):
        size = size or self.arraysize
        return [self.fetchone() for _ in range(size)]

    def fetchall(self):
        return list(self.df_iter)

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass


class TestDatabaseClient:

    @pytest.fixture
    def db_client(self):
        return DummyDatabaseClient()

    def test_query(self, db_client):
        result = db_client.query("DUMMY QUERY")

        assert type(result) == pd.DataFrame
        assert list(result.columns) == ['field1', 'field2']

        assert all(db_client("DUMMY_QUERY") == result)

    def test_multiple_queries(self, mocker, db_client):
        mocked = mocker.spy(db_client, "_execute")
        db_client.query("DUMMY QUERY; DUMMY_QUERY")
        assert mocked.call_count == 2

    def test_statement_hash(self, db_client):
        statement = "DUMMY QUERY"
        assert db_client.statement_hash(statement) == hashlib.sha256(statement.encode()).hexdigest()

    def test_stream(self, db_client):
        stream = db_client.stream("DUMMY QUERY")
        row = next(stream)
        assert tuple(row) == (0, 'a')
        stream.close()

    def test_format(self, db_client):
        result = db_client.query("DUMMY QUERY", format='csv')
        assert result == "field1,field2\r\n0,a\r\n1,b\r\n2,c\r\n3,d\r\n4,e\r\n5,f\r\n6,g\r\n7,h\r\n8,i\r\n9,j\r\n"
