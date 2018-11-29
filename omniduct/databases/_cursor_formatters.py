import csv
import io
import six

from omniduct.utils.debug import logger

COLUMN_NAME_FORMATTERS = {
    None: lambda x: x,
    'lowercase': lambda x: x.lower(),
    'uppercase': lambda x: x.upper()
}


class CursorFormatter(object):
    """
    An abstract base class for all cursor formatters.

    Cursor formatters are expected to transform a DB-API 2.0
    (https://www.python.org/dev/peps/pep-0249/) cursor into the format requested
    by the user.

    Attributes:
        cursor (DB-API 2.0 cursor): The cursor to be formatted.
        column_name_formatter (function): The column name formatter.
    """

    def __init__(self, cursor, column_name_formatter=None, **kwargs):
        """
        cursor (DB-API 2.0 cursor): The cursor to be formatted.
        column_name_formatter (function -> str, str, None): A function to
            transform column names, or one of `None`, `'lowercase'` or
            `'uppercase'`.
        **kwargs (dict): Any additional formatting arguments required by
            subclasses, which will be passed onto `self._init`.
        """
        self.cursor = cursor
        self.column_name_formatter = (
            column_name_formatter if callable(column_name_formatter)
            else COLUMN_NAME_FORMATTERS[column_name_formatter]
        )
        self._init(**kwargs)

    def _init(self):
        pass

    @property
    def column_names(self):
        """list<str>: The formatted names of the columns in the cursor."""
        return [self.column_name_formatter(c[0]) for c in self.cursor.description]

    @property
    def column_formats(self):
        """list<str>: The formats of the columns in the cursor."""
        return [c[1] for c in self.cursor.description]

    def dump(self):
        """
        Format and output the cursor in one batch dump.

        Returns:
            object: The data in the cursor transformed to the request format.
        """
        try:
            data = [self._prepare_row(row) for row in self.cursor.fetchall()]
            out = self._format_dump(data)
        finally:
            self.cursor.close()
        return out

    def stream(self, batch=None):
        """
        Format and output data in the cursor incrementally.

        Args:
            batch (None, int): The number of rows to transform in one go. If
                `None`, each row in the cursor is output separately. If an
                integer, including `1`, output is a list of formatted rows of
                length `batch`.

        Returns:
            object, list<object>: The formatted rows of the cursor.
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

    def _prepare_row(self, row):
        return row

    def _format_dump(self, data):
        raise NotImplementedError("{} does not support formatting dumped data.".format(self.__class__.__name__))

    def _format_row(self, row):
        raise NotImplementedError("{} does not support formatting streaming data.".format(self.__class__.__name__))


class PandasCursorFormatter(CursorFormatter):
    """
    Formats a cursor into pandas datatypes.

    Dumped data is transformed into a pandas DataFrame.
    Streamed data is transformed into lists of pandas Series objects.
    """

    def _init(self, index_fields=None, date_fields=None):
        self.index_fields = index_fields
        self.date_fields = date_fields

    def _format_dump(self, data):
        import pandas as pd

        df = pd.DataFrame(data=data, columns=self.column_names)

        if self.date_fields is not None:
            try:
                df = pd.io.sql._parse_date_columns(df, self.date_fields)
            except Exception as e:
                logger.warning('Unable to parse date columns. Perhaps your version of pandas is outdated.'
                               'Original error message was: {}: {}'.format(e.__class__.__name__, str(e)))

        if self.index_fields is not None:
            df.set_index(self.index_fields, inplace=True)

        return df

    def _format_row(self, row):
        import pandas as pd

        # TODO: Handle parsing of date fields

        return pd.Series(row, index=self.column_names)


class DictCursorFormatter(CursorFormatter):
    """
    Formats a cursor into a list of dictionaries.
    """

    def _format_dump(self, data):
        return [self._format_row(row) for row in data]

    def _format_row(self, row):
        return dict(zip(self.column_names, row))


class TupleCursorFormatter(CursorFormatter):
    """
    Formats a cursor into a list of tuples.
    """

    def _format_dump(self, data):
        return [self._format_row(row) for row in data]

    def _format_row(self, row):
        return tuple(row)


class RawCursorFormatter(CursorFormatter):
    """
    Applies the trivial transformation to each row in the cursor.
    """

    def _format_dump(self, data):
        return data

    def _format_row(self, row):
        return row


class CsvCursorFormatter(CursorFormatter):
    """
    Formats each row of the cursor as a comma-separated value string.
    """

    FORMAT_PARAMS = {
        'delimiter': ',',
        'doublequote': False,
        'escapechar': '\\',
        'lineterminator': '\r\n',
        'quotechar': '"',
        'quoting': csv.QUOTE_MINIMAL
    }

    def _init(self, include_header=True):
        self.output = io.StringIO() if six.PY3 else io.BytesIO()
        self.include_header = include_header
        self.writer = csv.writer(self.output, **self.FORMAT_PARAMS)

    def _format_dump(self, data):
        if self.include_header:
            self.writer.writerow(self.column_names)
        try:
            self.writer.writerows(data)
            return self.output.getvalue()
        finally:
            self.output.truncate(0)
            self.output.seek(0)

    def _format_row(self, row):
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

    FORMAT_PARAMS = {
        'delimiter': '\t',
        'doublequote': False,
        'escapechar': '',
        'lineterminator': '\n',
        'quotechar': '',
        'quoting': csv.QUOTE_NONE
    }

    def _init(self):
        CsvCursorFormatter._init(self, include_header=False)

    # Convert null values to '\N'.
    def _prepare_row(self, row):
        return [r'\N' if v is None else str(v).replace('\t', r'\t') for v in row]
