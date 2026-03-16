from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from interface_meta import override

from omniduct.databases.base import DatabaseClient
from omniduct.databases.hiveserver2 import HiveServer2Client

from ._namespaces import ParsedNamespaces

if TYPE_CHECKING:
    import pandas as pd


class PySparkClient(DatabaseClient):
    """
    This Duct connects to a local PySpark session using the `pyspark` library.
    """

    PROTOCOLS = ["pyspark"]
    DEFAULT_PORT: int | None = None
    SUPPORTS_SESSION_PROPERTIES = True
    NAMESPACE_NAMES: list[str] = ["schema", "table"]
    NAMESPACE_QUOTECHAR: str = "`"
    NAMESPACE_SEPARATOR: str = "."

    app_name: str
    config: dict[str, Any]
    master: str | None
    enable_hive_support: bool
    _spark_session: Any

    @override
    def _init(
        self,
        app_name: str = "omniduct",
        config: dict[str, Any] | None = None,
        master: str | None = None,
        enable_hive_support: bool = False,
    ) -> None:
        """
        Args:
            app_name: The application name of the SparkSession.
            config: Any additional configuration to pass through to the
                SparkSession builder.
            master: The Spark master URL to connect to (only necessary if
                environment specified configuration is missing).
            enable_hive_support: Whether to enable Hive support for the Spark
                session.

        Note: Pyspark must be installed in order to use this backend.
        """
        self.app_name = app_name
        self.config = config or {}
        self.master = master
        self.enable_hive_support = enable_hive_support
        self._spark_session = None

    # Connection management

    @override
    def _connect(self) -> None:
        from pyspark.sql import SparkSession

        builder = SparkSession.builder.appName(self.app_name)
        if self.master:
            builder.master(self.master)
        if self.enable_hive_support:
            builder.enableHiveSupport()
        if self.config:
            for key, value in self.config.items():
                builder.config(key, value)

        self._spark_session = builder.getOrCreate()

    @override
    def _is_connected(self) -> bool:
        return self._spark_session is not None

    @override
    def _disconnect(self) -> None:
        self._spark_session.sparkContext.stop()

    # Database operations
    @override
    def _statement_prepare(
        self,
        statement: str,
        session_properties: dict[str, Any],
        **kwargs: Any,
    ) -> str:
        return (
            "\n".join(
                f"SET {key} = {value};" for key, value in session_properties.items()
            )
            + statement
        )

    @override
    def _execute(
        self,
        statement: str,
        cursor: Any,
        wait: bool,
        session_properties: dict[str, Any],
    ) -> SparkCursor:
        if not wait:
            raise NotImplementedError(
                "This Spark backend does not support asynchronous operations."
            )
        return SparkCursor(self._spark_session.sql(statement))

    @override
    def _query_to_table(
        self,
        statement: str,
        table: ParsedNamespaces,
        if_exists: str,
        **kwargs: Any,
    ) -> Any:
        return HiveServer2Client._query_to_table(
            self, statement, table, if_exists, **kwargs
        )

    @override
    def _table_list(self, namespace: ParsedNamespaces, **kwargs: Any) -> Any:
        return HiveServer2Client._table_list(self, namespace, **kwargs)

    @override
    def _table_exists(self, table: ParsedNamespaces, **kwargs: Any) -> bool:
        return HiveServer2Client._table_exists(self, table, **kwargs)  # type: ignore[no-any-return]

    @override
    def _table_drop(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        return HiveServer2Client._table_drop(self, table, **kwargs)

    @override
    def _table_desc(self, table: ParsedNamespaces, **kwargs: Any) -> pd.DataFrame:
        return HiveServer2Client._table_desc(self, table, **kwargs)  # type: ignore[no-any-return]

    @override
    def _table_head(self, table: ParsedNamespaces, n: int = 10, **kwargs: Any) -> Any:
        return HiveServer2Client._table_head(self, table, n=n, **kwargs)

    @override
    def _table_props(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        return HiveServer2Client._table_props(self, table, **kwargs)


class SparkCursor:
    """
    This DBAPI2 compatible cursor wraps around a Spark DataFrame
    """

    df: Any
    _df_iter: Iterator[Any] | None

    arraysize: int = 1

    def __init__(self, df: Any) -> None:
        self.df = df
        self._df_iter = None

    @property
    def df_iter(self) -> Iterator[Any]:
        if not getattr(self, "_df_iter"):
            self._df_iter = self.df.toLocalIterator()
        if self._df_iter is None:
            raise RuntimeError("df_iter is not initialized")
        return self._df_iter

    @property
    def description(self) -> tuple[tuple[str, str, None, None, None, None, None], ...]:
        return tuple(
            (name, type_, None, None, None, None, None)
            for name, type_ in self.df.dtypes
        )

    @property
    def row_count(self) -> int:
        return -1

    def close(self) -> None:
        pass

    def execute(self, operation: Any, parameters: Any = None) -> None:
        raise NotImplementedError

    def executemany(self, operation: Any, seq_of_parameters: Any = None) -> None:
        raise NotImplementedError

    def fetchone(self) -> list[Any]:
        return [value or None for value in next(self.df_iter)]

    def fetchmany(self, size: int | None = None) -> list[list[Any]]:
        size = size or self.arraysize
        return [self.fetchone() for _ in range(size)]

    def fetchall(self) -> list[Any]:
        return self.df.collect()  # type: ignore[no-any-return]

    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        pass
