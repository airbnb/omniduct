# pylint: disable=abstract-method

from __future__ import absolute_import

import pandas as pd
import sqlalchemy
from sqlalchemy import Table
from sqlalchemy import types as sql_types

from omniduct.utils.debug import logger
from omniduct.utils.decorators import require_connection

try:
    from pyhive.sqlalchemy_presto import PrestoDialect

    def get_columns(self, connection, table_name, schema=None, **kw):
        # Extend types supported by PrestoDialect as defined in PyHive
        type_map = {
            "bigint": sql_types.BigInteger,
            "integer": sql_types.Integer,
            "boolean": sql_types.Boolean,
            "double": sql_types.Float,
            "varchar": sql_types.String,
            "timestamp": sql_types.TIMESTAMP,
            "date": sql_types.DATE,
            "array<bigint>": sql_types.ARRAY(sql_types.Integer),
            "array<varchar>": sql_types.ARRAY(sql_types.String),
        }

        rows = self._get_table_columns(connection, table_name, schema)
        result = []
        for row in rows:
            try:
                coltype = type_map[row.Type]
            except KeyError:
                logger.warn(
                    f"Did not recognize type '{row.Type}' of column '{row.Column}'"
                )
                coltype = sql_types.NullType
            result.append(
                {
                    "name": row.Column,
                    "type": coltype,
                    # newer Presto no longer includes this column
                    "nullable": getattr(row, "Null", True),
                    "default": None,
                }
            )
        return result

    PrestoDialect.get_columns = get_columns
except ImportError:
    logger.debug(
        "Not monkey patching pyhive's PrestoDialect.get_columns due to missing dependencies."
    )


class SchemasMixin:
    """
    Attaches a tab-completable `.schemas` attribute to a `DatabaseClient` instance.

    It is currently implemented as a mixin rather than directly provided on the
    base class because it requires that the host `DatabaseClient` instance have a
    `sqlalchemy` metadata object handle, and not all backends support this.

    If we are willing to forgo the ability to actually make queries using the
    SQLAlchemy ORM, we could instead use an SQL agnostic version.
    """

    @property
    @require_connection
    def schemas(self):
        """
        object: An object with attributes corresponding to the names of the schemas
            in this database.
        """
        from lazy_object_proxy import Proxy

        def get_schemas():
            if not getattr(self, "_schemas", None):
                assert (
                    getattr(self, "_sqlalchemy_metadata", None) is not None
                ), f"`{self.__class__.__name__}` instances do not provide the required sqlalchemy metadata for schema exploration."
                self._schemas = Schemas(self._sqlalchemy_metadata)
            return self._schemas

        return Proxy(get_schemas)


# Extend Table to support returning pandas description of table
class TableDesc(Table):
    """
    Extends the SQL Alchemy `Table` class with some short-hand introspection methods.
    """

    def desc(self):
        """pandas.DataFrame: The description of this SQL table."""
        return pd.DataFrame(
            [
                [col.name, col.type.compile(self.bind.dialect)]
                for col in self.columns.values()
            ],
            columns=["name", "type"],
        )

    def head(self, n=10):
        """
        Retrieve the first `n` rows from this table.

        Args:
            n (int): The number of rows to retrieve from this table.

        Returns:
            pandas.DataFrame: A dataframe representation of the first `n` rows
                of this table.
        """
        statement = self.select()
        if n is not None:
            statement = statement.limit(n)
        return pd.read_sql(statement, self.bind)

    def dump(self):
        """
        Retrieve the entire database table as a pandas DataFrame.

        Returns:
            pandas.DataFrame: A dataframe representation of the entire table.
        """
        return self.head(n=None)

    def __repr__(self):
        return self.desc().__repr__()


# Define helpers to allow for table completion/etc
class Schemas:
    """
    An object which has as its attributes all of the schemas in a nominated database.

    Args:
        metadata (sqlalchemy.MetaData): A SQL Alchemy `MetaData` instance
            configured for the nominated database.
    """

    def __init__(self, metadata):
        self._metadata = metadata
        self._schema_names = None
        self._schema_cache = {}

    @property
    def all(self):
        "list<str>: The list of schema names."
        if self._schema_names is None:
            self._schema_names = sqlalchemy.inspect(
                self._metadata.bind
            ).get_schema_names()
        return self._schema_names

    def __dir__(self):
        return self.all

    def __getattr__(self, value):
        if value in self.all:
            if value not in self._schema_cache:
                self._schema_cache[value] = Schema(
                    metadata=self._metadata, schema=value
                )
            return self._schema_cache[value]
        raise AttributeError(f"No such schema {value}")

    def __repr__(self):
        return f"<Schemas: {len(self.all)} schemas>"

    def __iter__(self):
        for schema in self.all:
            yield schema

    def __len__(self):
        return len(self.all)


class Schema:
    """
    An object which has as its attributes all of the tables in a nominated database schema.

    Args:
        metadata (sqlalchemy.MetaData): A SQL Alchemy `MetaData` instance
            configured for the nominated database.
        schema (str): The schema within which to expose tables.
    """

    def __init__(self, metadata, schema):
        self._metadata = metadata
        self._schema = schema
        self._table_cache = {}
        self._table_names = None

    @property
    def all(self):
        """list<str>: The table names in this database schema."""
        if self._table_names is None:
            self._table_names = sqlalchemy.inspect(self._metadata.bind).get_table_names(
                self._schema
            )
        return self._table_names

    def __dir__(self):
        return self.all

    def __getattr__(self, table):
        if table in self.all:
            if table not in self._table_cache:
                self._table_cache[table] = TableDesc(
                    f"{table}",
                    self._metadata,
                    autoload=True,
                    schema=self._schema,
                )
            return self._table_cache[table]
        raise AttributeError(f"No such table {table}")

    def __repr__(self):
        return f"<Schema `{self._schema}`: {len(self.all)} tables>"

    def __iter__(self):
        for schema in self.all:
            yield schema

    def __len__(self):
        return len(self.all)
