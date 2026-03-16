from __future__ import annotations

import _csv
import csv
import io
from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Any

from omniduct.utils.debug import logger

if TYPE_CHECKING:
    import pandas as pd

COLUMN_NAME_FORMATTERS: dict[str | None, Callable[[str], str]] = {
    None: lambda x: x,
    "lowercase": lambda x: x.lower(),
    "uppercase": lambda x: x.upper(),
}


class CursorFormatter:
    """
    An abstract base class for all cursor formatters.

    Cursor formatters are expected to transform a DB-API 2.0
    (https://www.python.org/dev/peps/pep-0249/) cursor into the format requested
    by the user.

    Attributes:
        cursor (DB-API 2.0 cursor): The cursor to be formatted.
        column_name_formatter: The column name formatter.
    """

    cursor: Any
    column_name_formatter: Callable[[str], str]

    def __init__(
        self,
        cursor: Any,
        column_name_formatter: Callable[[str], str] | str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        cursor: The cursor to be formatted.
        column_name_formatter: A function to transform column names, or one of
            `None`, `'lowercase'` or `'uppercase'`.
        **kwargs: Any additional formatting arguments required by subclasses,
            which will be passed onto `self._init`.
        """
        self.cursor = cursor
        self.column_name_formatter = (
            column_name_formatter
            if callable(column_name_formatter)
            else COLUMN_NAME_FORMATTERS[column_name_formatter]
        )
        self._init(**kwargs)

    def _init(self) -> None:
        pass

    @property
    def column_names(self) -> list[str]:
        """list[str]: The formatted names of the columns in the cursor."""
        return [self.column_name_formatter(c[0]) for c in self.cursor.description]

    @property
    def column_formats(self) -> list[Any]:
        """list[Any]: The formats of the columns in the cursor."""
        return [c[1] for c in self.cursor.description]

    def dump(self) -> Any:
        """
        Format and output the cursor in one batch dump.

        Returns:
            The data in the cursor transformed to the request format.
        """
        try:
            data = [self._prepare_row(row) for row in self.cursor.fetchall()]
            out = self._format_dump(data)
        finally:
            self.cursor.close()
        return out

    def stream(self, batch: int | None = None) -> Generator[Any, None, None]:
        """
        Format and output data in the cursor incrementally.

        Args:
            batch: The number of rows to transform in one go. If `None`, each
                row in the cursor is output separately. If an integer, including
                `1`, output is a list of formatted rows of length `batch`.

        Returns:
            The formatted rows of the cursor.
        """
        try:
            if batch is not None:
                while True:
                    b = [self._prepare_row(row) for row in self.cursor.fetchmany(batch)]
                    if len(b) == 0:
                        return
                    yield self._format_dump(b)
            else:
                row = self.cursor.fetchone()
                while row is not None:
                    yield self._format_row(row)
                    row = self.cursor.fetchone()
        finally:
            self.cursor.close()

    def _prepare_row(self, row: Any) -> Any:
        return row

    def _format_dump(self, data: list[Any]) -> Any:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support formatting dumped data."
        )

    def _format_row(self, row: Any) -> Any:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support formatting streaming data."
        )


class PandasCursorFormatter(CursorFormatter):
    """
    Formats a cursor into pandas datatypes.

    Dumped data is transformed into a pandas DataFrame.
    Streamed data is transformed into lists of pandas Series objects.
    """

    index_fields: list[str] | None
    date_fields: list[str] | None

    def _init(
        self,
        index_fields: list[str] | None = None,
        date_fields: list[str] | None = None,
    ) -> None:
        self.index_fields = index_fields
        self.date_fields = date_fields

    def _format_dump(self, data: list[Any]) -> pd.DataFrame:
        import pandas as pd

        df = pd.DataFrame(data=data, columns=self.column_names)

        if self.date_fields is not None:
            try:
                parse_date_columns = getattr(pd.io.sql, "_parse_date_columns", None)
                if parse_date_columns is not None:
                    df = parse_date_columns(df, self.date_fields)
            except Exception as e:
                logger.warning(
                    f"Unable to parse date columns. Perhaps your version of pandas is outdated.Original error message was: {e.__class__.__name__}: {str(e)}"
                )

        if self.index_fields is not None:
            df.set_index(self.index_fields, inplace=True)

        return df

    def _format_row(self, row: Any) -> pd.Series:
        import pandas as pd

        # TODO: Handle parsing of date fields

        return pd.Series(row, index=self.column_names)


class DictCursorFormatter(CursorFormatter):
    """
    Formats a cursor into a list of dictionaries.
    """

    def _format_dump(self, data: list[Any]) -> list[dict[str, Any]]:
        return [self._format_row(row) for row in data]

    def _format_row(self, row: Any) -> dict[str, Any]:
        return dict(zip(self.column_names, row))


class TupleCursorFormatter(CursorFormatter):
    """
    Formats a cursor into a list of tuples.
    """

    def _format_dump(self, data: list[Any]) -> list[tuple[Any, ...]]:
        return [self._format_row(row) for row in data]

    def _format_row(self, row: Any) -> tuple[Any, ...]:
        return tuple(row)


class RawCursorFormatter(CursorFormatter):
    """
    Applies the trivial transformation to each row in the cursor.
    """

    def _format_dump(self, data: list[Any]) -> list[Any]:
        return data

    def _format_row(self, row: Any) -> Any:
        return row


class CsvCursorFormatter(CursorFormatter):
    """
    Formats each row of the cursor as a comma-separated value string.
    """

    FORMAT_PARAMS: dict[str, Any] = {
        "delimiter": ",",
        "doublequote": False,
        "escapechar": "\\",
        "lineterminator": "\r\n",
        "quotechar": '"',
        "quoting": csv.QUOTE_MINIMAL,
    }

    output: io.StringIO
    include_header: bool
    writer: _csv.Writer

    def _init(self, include_header: bool = True) -> None:
        self.output = io.StringIO()
        self.include_header = include_header
        self.writer = csv.writer(self.output, **self.FORMAT_PARAMS)

    def _format_dump(self, data: list[Any]) -> str:
        if self.include_header:
            self.writer.writerow(self.column_names)
        try:
            self.writer.writerows(data)
            return self.output.getvalue()
        finally:
            self.output.truncate(0)
            self.output.seek(0)

    def _format_row(self, row: Any) -> str:
        try:
            self.writer.writerow(row)
            return self.output.getvalue()
        finally:
            self.output.truncate(0)
            self.output.seek(0)


class HiveCursorFormatter(CsvCursorFormatter):
    """
    Formats each row of the cursor as a tab-separated value string.

    Note: `None` values are transformed into the hive-specific representation of
    `'\\N'`.
    """

    FORMAT_PARAMS: dict[str, Any] = {
        "delimiter": "\t",
        "doublequote": False,
        "escapechar": "",
        "lineterminator": "\n",
        "quotechar": "",
        "quoting": csv.QUOTE_NONE,
    }

    def _init(self) -> None:  # type: ignore[override]
        CsvCursorFormatter._init(self, include_header=False)

    # Convert null values to '\N'.
    def _prepare_row(self, row: Any) -> list[str]:
        return [r"\N" if v is None else str(v).replace("\t", r"\t") for v in row]
