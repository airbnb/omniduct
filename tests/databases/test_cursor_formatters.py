import sqlite3

import pytest

from omniduct.databases._cursor_formatters import HiveCursorFormatter


@pytest.mark.parametrize("mode", ["dump", "stream", "batch"])
def test_hive_formatter_preserves_nulls_and_escapes_tabs(mode):
    with sqlite3.connect(":memory:") as connection:
        cursor = connection.execute("SELECT ?, ?, ?", (None, "a\tb", 42))
        formatter = HiveCursorFormatter(cursor)
        if mode == "dump":
            result = formatter.dump()
        elif mode == "stream":
            result = "".join(formatter.stream())
        else:
            result = "".join(formatter.stream(batch=1))
        assert result == "\\N\ta\\tb\t42\n"
        with pytest.raises(sqlite3.ProgrammingError, match="closed cursor"):
            cursor.fetchone()
