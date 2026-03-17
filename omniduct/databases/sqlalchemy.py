from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING, Any, Literal

from interface_meta import override

from omniduct.utils.debug import logger

from . import _pandas
from ._namespaces import ParsedNamespaces
from ._schemas import SchemasMixin
from .base import DatabaseClient

if TYPE_CHECKING:
    import pandas as pd
    import sqlalchemy as sa


class SQLAlchemyClient(DatabaseClient, SchemasMixin):
    """
    This Duct connects to several different databases using one of several
    SQLAlchemy drivers. In general, these are provided for their potential
    utility, but will be less functional than the specially crafted database
    clients.
    """

    PROTOCOLS = [
        "sqlalchemy",
        "firebird",
        "mssql",
        "mysql",
        "oracle",
        "postgresql",
        "sybase",
        "snowflake",
    ]
    NAMESPACE_NAMES: list[str] = ["database", "table"]
    NAMESPACE_QUOTECHAR: str = '"'  # TODO: Apply overrides depending on protocol?
    NAMESPACE_SEPARATOR: str = "."

    dialect: str | None
    driver: str | None
    database: str
    engine_opts: dict[str, Any]
    engine: sa.Engine | None
    connection: sa.Connection | None

    @property
    @override
    def NAMESPACE_DEFAULT(self) -> dict[str, str]:  # type: ignore[override]
        return {"database": self.database}

    @override
    def _init(
        self,
        dialect: str | None = None,
        driver: str | None = None,
        database: str = "",
        engine_opts: dict[str, Any] | None = None,
    ) -> None:
        if self._port is None:
            raise ValueError(
                "Omniduct requires SQLAlchemy databases to manually specify a port, as "
                "it will often be the case that ports are being forwarded."
            )

        if self.protocol != "sqlalchemy":
            self.dialect = self.protocol
        else:
            self.dialect = dialect
        if self.dialect is None:
            raise ValueError("Dialect not specified.")

        self.driver = driver
        self.database = database
        self.connection_fields += ("schema",)
        self.engine_opts = engine_opts or {}

        self.engine = None
        self.connection = None

    @property
    def db_uri(self) -> str:
        if self.dialect is None:
            raise RuntimeError("dialect must be set before accessing db_uri")
        username = self.username
        password = self.password
        host = self.host
        return "{dialect}://{login}@{host_port}/{database}".format(
            dialect=self.dialect + (f"+{self.driver}" if self.driver else ""),
            login=(username or "")
            + (f":{urllib.parse.quote_plus(password)}" if password else ""),
            host_port=(host or "") + (f":{self.port}" if self.port else ""),
            database=self.database,
        )

    @property
    @override
    def _sqlalchemy_engine(self) -> sa.Engine | None:
        """
        The SQLAlchemy engine object for the SchemasMixin.
        """
        return self.engine

    @_sqlalchemy_engine.setter
    def _sqlalchemy_engine(self, engine: sa.Engine | None) -> None:
        self.engine = engine

    @override
    def _connect(self) -> None:
        import sqlalchemy

        if self.protocol not in ["mysql"]:
            logger.warning(
                "While querying and executing should work as "
                "expected, some operations on this database client "
                "(such as listing tables, querying to tables, etc) "
                "may not function as expected due to the backend "
                "not supporting ANSI SQL."
            )

        self.engine = sqlalchemy.create_engine(self.db_uri, **self.engine_opts)
        self.connection = self.engine.connect()

    @override
    def _is_connected(self) -> bool:
        return self.connection is not None

    @override
    def _disconnect(self) -> None:
        if self.connection is not None:
            self.connection.close()
        self.connection = None
        self.engine = None
        self._schemas = None

    @override
    def _execute(
        self,
        statement: str,
        cursor: Any,
        wait: bool,
        session_properties: dict[str, Any],
        query: bool = True,
        **kwargs: Any,
    ) -> Any:
        import sqlalchemy

        if not wait:
            raise RuntimeError(
                "`SQLAlchemyClient` does not support asynchronous operations."
            )

        if self.connection is None:
            raise RuntimeError("Not connected.")
        if cursor:
            cursor.execute(statement)
        else:
            cursor = self.connection.execute(sqlalchemy.text(statement)).cursor
        return cursor

    @override
    def _query_to_table(
        self,
        statement: str,
        table: ParsedNamespaces,
        if_exists: str,
        **kwargs: Any,
    ) -> Any:
        statements = []

        if if_exists == "fail" and self.table_exists(table):
            raise RuntimeError(f"Table {table} already exists!")
        if if_exists == "replace":
            statements.append(f"DROP TABLE IF EXISTS {table};")
        elif if_exists == "append":
            raise NotImplementedError(
                f"Append operations have not been implemented for {self.__class__.__name__}."
            )

        statement = f"CREATE TABLE {table} AS ({statement})"
        return self.execute(statement, **kwargs)

    @override
    def _dataframe_to_table(
        self,
        df: pd.DataFrame,
        table: ParsedNamespaces,
        if_exists: Literal["fail", "replace", "append", "delete_rows"] = "fail",
        **kwargs: Any,
    ) -> None:
        return _pandas.to_sql(
            df=df,
            name=table.table,  # type: ignore
            schema=table.database,
            con=self.engine,
            index=False,
            if_exists=if_exists,
            **kwargs,
        )

    @override
    def _table_list(self, namespace: ParsedNamespaces, **kwargs: Any) -> Any:
        return self.query(f"SHOW TABLES IN {namespace}", **kwargs)

    @override
    def _table_exists(self, table: ParsedNamespaces, **kwargs: Any) -> bool:
        logger.disabled = True
        try:
            self.table_desc(table, **kwargs)
            return True
        except:
            return False
        finally:
            logger.disabled = False

    @override
    def _table_drop(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        return self.execute(f"DROP TABLE {table}")

    @override
    def _table_desc(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        return self.query(f"DESCRIBE {table}", **kwargs)

    @override
    def _table_head(self, table: ParsedNamespaces, n: int = 10, **kwargs: Any) -> Any:
        # Use parameterized query to avoid SQL injection
        query = f"SELECT * FROM {table} LIMIT %s"
        return self.query(query, n, **kwargs)

    @override
    def _table_props(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        raise NotImplementedError
