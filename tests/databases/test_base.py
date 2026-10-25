import csv
import hashlib
import io
import itertools

import pandas as pd
import pytest

from omniduct.databases._cursor_formatters import CsvCursorFormatter
from omniduct.databases.base import DatabaseClient
from omniduct.filesystems.local import LocalFsClient


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


class DummyCursor:
    """
    This DBAPI2 compatible cursor wrapped around a Pandas DataFrame
    """

    def __init__(self):
        self.df = pd.DataFrame(
            {"field1": list(range(10)), "field2": list("abcdefghij")}
        )
        self._df_iter = None

    @property
    def df_iter(self):
        if not getattr(self, "_df_iter"):
            self._df_iter = (tuple(row) for i, row in self.df.iterrows())
        return self._df_iter

    arraysize = 1

    @property
    def description(self):
        return tuple(
            [(name, None, None, None, None, None, None) for name in self.df.columns]
        )

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
        return next(self.df_iter, None)

    def fetchmany(self, size=None):
        size = size or self.arraysize
        return list(itertools.islice(self.df_iter, size))

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

        assert type(result) is pd.DataFrame
        assert list(result.columns) == ["field1", "field2"]

        assert all(db_client("DUMMY_QUERY") == result)

    def test_multiple_queries(self, mocker, db_client):
        mocked = mocker.spy(db_client, "_execute")
        db_client.query("DUMMY QUERY; DUMMY_QUERY")
        assert mocked.call_count == 2

    def test_statement_hash(self, db_client):
        statement = "DUMMY QUERY"
        assert (
            db_client.statement_hash(statement)
            == hashlib.sha256(statement.encode()).hexdigest()
        )

    def test_stream(self, db_client):
        stream = db_client.stream("DUMMY QUERY")
        row = next(stream)
        assert tuple(row) == (0, "a")
        stream.close()

    @pytest.mark.parametrize("batch", [None, 1, 3, 20])
    def test_stream_csv(self, db_client, batch):
        rows = [f"{i},{letter}\r\n" for i, letter in enumerate("abcdefghij")]
        size = batch or 1

        result = list(db_client.stream("DUMMY QUERY", format="csv", batch=batch))

        assert result == [
            "".join(rows[i : i + size]) for i in range(0, len(rows), size)
        ]

    @pytest.mark.parametrize("batch", [None, 1, 3, 20])
    @pytest.mark.parametrize("empty", [False, True])
    @pytest.mark.parametrize("use_path", [False, True])
    def test_stream_to_file_csv(
        self, db_client, mocker, tmp_path, batch, empty, use_path
    ):
        cursor = DummyCursor()
        if empty:
            cursor.df = cursor.df.iloc[:0]
        execute = mocker.patch.object(db_client, "execute", return_value=cursor)
        close = mocker.spy(cursor, "close")
        path = tmp_path / "data.csv"
        file = str(path) if use_path else io.StringIO()

        db_client.stream_to_file(
            "DUMMY QUERY", file, batch=batch, fs=LocalFsClient(home=str(tmp_path))
        )

        if use_path:
            with path.open(newline="") as output:
                result = output.read()
        else:
            assert not file.closed
            result = file.getvalue()
        expected = "field1,field2\r\n"
        if not empty:
            expected += "".join(
                f"{i},{letter}\r\n" for i, letter in enumerate("abcdefghij")
            )
        assert result == expected
        execute.assert_called_once_with("DUMMY QUERY", wait=True)
        close.assert_called_once_with()

    @pytest.mark.parametrize("batch", [None, 3])
    @pytest.mark.parametrize("include_header", [False, True])
    def test_stream_to_file_csv_format_opts(self, db_client, batch, include_header):
        file = io.StringIO()
        format_opts = {
            "include_header": include_header,
            "column_name_formatter": "uppercase",
        }

        db_client.stream_to_file(
            "DUMMY QUERY", file, batch=batch, format_opts=format_opts
        )

        expected = "FIELD1,FIELD2\r\n" if include_header else ""
        expected += "".join(
            f"{i},{letter}\r\n" for i, letter in enumerate("abcdefghij")
        )
        assert file.getvalue() == expected
        assert format_opts == {
            "include_header": include_header,
            "column_name_formatter": "uppercase",
        }

    @pytest.mark.parametrize("batch", [None, 3])
    def test_stream_to_file_csv_quoting(self, db_client, mocker, batch):
        cursor = DummyCursor()
        cursor.df = pd.DataFrame({"field,1": ["a,b", "line\nbreak", 'a"b']})
        mocker.patch.object(db_client, "execute", return_value=cursor)
        file = io.StringIO()

        db_client.stream_to_file(
            "DUMMY QUERY", file, format=CsvCursorFormatter, batch=batch
        )

        file.seek(0)
        assert list(csv.reader(file, escapechar="\\", doublequote=False)) == [
            ["field,1"],
            ["a,b"],
            ["line\nbreak"],
            ['a"b'],
        ]

    @pytest.mark.parametrize("batch", [None, 3])
    def test_stream_tuple(self, db_client, batch):
        rows = list(enumerate("abcdefghij"))

        result = list(db_client.stream("DUMMY QUERY", format="tuple", batch=batch))

        if batch is None:
            assert result == rows
        else:
            assert result == [rows[i : i + batch] for i in range(0, len(rows), batch)]

    def test_stream_to_file_csv_default_format_opts(self):
        db_client = DummyDatabaseClient(default_format_opts={"include_header": False})
        file = io.StringIO()

        db_client.stream_to_file("DUMMY QUERY", file, batch=3)

        assert file.getvalue() == "".join(
            f"{i},{letter}\r\n" for i, letter in enumerate("abcdefghij")
        )

    def test_format_csv_without_header(self, db_client):
        result = db_client.query(
            "DUMMY QUERY", format="csv", format_opts={"include_header": False}
        )

        assert result == "".join(
            f"{i},{letter}\r\n" for i, letter in enumerate("abcdefghij")
        )

    def test_format(self, db_client):
        result = db_client.query("DUMMY QUERY", format="csv")
        assert (
            result
            == "field1,field2\r\n0,a\r\n1,b\r\n2,c\r\n3,d\r\n4,e\r\n5,f\r\n6,g\r\n7,h\r\n8,i\r\n9,j\r\n"
        )
