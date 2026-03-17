from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, cast

import sqlalchemy
from sqlalchemy import Table
from sqlalchemy import types as sql_types

from omniduct.utils.debug import logger
from omniduct.utils.decorators import require_connection

if TYPE_CHECKING:
    import pandas as pd
    import sqlalchemy as sa

try:
    from pyhive.sqlalchemy_presto import PrestoDialect

    def get_columns(
        self: Any,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kw: Any,
    ) -> list[dict[str, Any]]:
        # Extend types supported by PrestoDialect as defined in PyHive
        type_map: dict[str, Any] = {
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
    `sqlalchemy` engine object handle, and not all backends support this.

    If we are willing to forgo the ability to actually make queries using the
    SQLAlchemy ORM, we could instead use an SQL agnostic version.
    """

    _schemas: Schemas | None
    _sqlalchemy_engine: sa.Engine | None

    @property
    @require_connection
    def schemas(self) -> Schemas:
        """
        An object with attributes corresponding to the names of the schemas
        in this database.
        """
        from lazy_object_proxy import Proxy

        def get_schemas() -> Schemas:
            if not getattr(self, "_schemas", None):
                if getattr(self, "_sqlalchemy_engine", None) is None:
                    raise RuntimeError(
                        f"`{self.__class__.__name__}` instances do not provide the required sqlalchemy engine for schema exploration."
                    )
                self._schemas = Schemas(self._sqlalchemy_engine)
            if self._schemas is None:
                raise RuntimeError("Schemas could not be initialized.")
            return self._schemas

        return cast(Schemas, Proxy(get_schemas))


# Extend Table to support returning pandas description of table
class TableDesc(Table):
    """
    Extends the SQL Alchemy `Table` class with some short-hand introspection methods.
    """

    _bound_engine: sa.Engine

    @classmethod
    def reflect(
        cls, name: str, metadata: sqlalchemy.MetaData, engine: sa.Engine, schema: str
    ) -> TableDesc:
        """Reflect a table from the database, binding an engine for introspection."""
        t = cls(name, metadata, autoload_with=engine, schema=schema)
        t._bound_engine = engine
        return t

    def desc(self) -> pd.DataFrame:
        """pandas.DataFrame: The description of this SQL table."""
        import pandas as pd

        engine = self._bound_engine
        return pd.DataFrame(
            [
                [col.name, col.type.compile(engine.dialect)]
                for col in self.columns.values()
            ],
            columns=["name", "type"],
        )

    def head(self, n: int | None = 10) -> pd.DataFrame:
        """
        Retrieve the first `n` rows from this table.

        Args:
            n: The number of rows to retrieve from this table.

        Returns:
            A dataframe representation of the first `n` rows of this table.
        """
        import pandas as pd

        statement = self.select()
        if n is not None:
            statement = statement.limit(n)
        return pd.read_sql(statement, self._bound_engine)

    def dump(self) -> pd.DataFrame:
        """
        Retrieve the entire database table as a pandas DataFrame.

        Returns:
            A dataframe representation of the entire table.
        """
        return self.head(n=None)

    def __repr__(self) -> str:
        return self.desc().__repr__()


# Define helpers to allow for table completion/etc
class Schemas:
    """
    An object which has as its attributes all of the schemas in a nominated database.

    Args:
        engine: A SQL Alchemy `Engine` instance configured for the nominated
            database.
    """

    _engine: sa.Engine
    _schema_names: list[str] | None
    _schema_cache: dict[str, Schema]

    def __init__(self, engine: sa.Engine) -> None:
        self._engine = engine
        self._schema_names = None
        self._schema_cache = {}

    @property
    def all(self) -> list[str]:
        "list[str]: The list of schema names."
        if self._schema_names is None:
            self._schema_names = sqlalchemy.inspect(self._engine).get_schema_names()
        return self._schema_names

    def __dir__(self) -> list[str]:
        return self.all

    def __getattr__(self, value: str) -> Schema:
        if value in self.all:
            if value not in self._schema_cache:
                self._schema_cache[value] = Schema(engine=self._engine, schema=value)
            return self._schema_cache[value]
        raise AttributeError(f"No such schema {value}")

    def __repr__(self) -> str:
        return f"<Schemas: {len(self.all)} schemas>"

    def __iter__(self) -> Iterator[str]:
        yield from self.all

    def __len__(self) -> int:
        return len(self.all)


class Schema:
    """
    An object which has as its attributes all of the tables in a nominated database schema.

    Args:
        engine: A SQL Alchemy `Engine` instance configured for the nominated
            database.
        schema: The schema within which to expose tables.
    """

    _engine: sa.Engine
    _schema: str
    _table_cache: dict[str, TableDesc]
    _table_names: list[str] | None
    _metadata: sqlalchemy.MetaData

    def __init__(self, engine: sa.Engine, schema: str) -> None:
        self._engine = engine
        self._schema = schema
        self._table_cache = {}
        self._table_names = None
        self._metadata = sqlalchemy.MetaData()

    @property
    def all(self) -> list[str]:
        """list[str]: The table names in this database schema."""
        if self._table_names is None:
            self._table_names = sqlalchemy.inspect(self._engine).get_table_names(
                self._schema
            )
        return self._table_names

    def __dir__(self) -> list[str]:
        return self.all

    def __getattr__(self, table: str) -> TableDesc:
        if table in self.all:
            if table not in self._table_cache:
                self._table_cache[table] = TableDesc.reflect(
                    table, self._metadata, self._engine, self._schema
                )
            return self._table_cache[table]
        raise AttributeError(f"No such table {table}")

    def __repr__(self) -> str:
        return f"<Schema `{self._schema}`: {len(self.all)} tables>"

    def __iter__(self) -> Iterator[str]:
        yield from self.all

    def __len__(self) -> int:
        return len(self.all)
