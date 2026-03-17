from __future__ import annotations

from typing import Any

from interface_meta import override

from omniduct.utils.debug import logger

from ._namespaces import ParsedNamespaces
from .base import DatabaseClient


class DruidClient(DatabaseClient):
    """
    This Duct connects to a Druid server using the `pydruid` python library.
    """

    PROTOCOLS = ["druid"]
    DEFAULT_PORT = 80
    NAMESPACE_NAMES: list[str] = ["table"]
    NAMESPACE_QUOTECHAR: str = '"'
    NAMESPACE_SEPARATOR: str = "."

    @override
    def _init(self) -> None:
        self.__druid: Any = None

    # Connection
    @override
    def _connect(self) -> None:
        from pydruid.db import connect

        logger.info("Connecting to Druid database ...")
        self.__druid = connect(
            self.host, self.port, path="/druid/v2/sql/", scheme="http"
        )
        if self.username or self.password:
            logger.warning(
                "Duct username and password not passed to pydruid connection. "
                "pydruid connection currently does not allow these fields to be passed."
            )

    @override
    def _is_connected(self) -> bool:
        return self.__druid is not None

    @override
    def _disconnect(self) -> None:
        logger.info("Disconnecting from Druid database ...")
        try:
            self.__druid.close()
        except:
            pass
        self.__druid = None

    # Querying
    @override
    def _execute(
        self,
        statement: str,
        cursor: Any,
        wait: bool,
        session_properties: dict[str, Any],
    ) -> Any:
        cursor = cursor or self.__druid.cursor()
        cursor.execute(statement)
        return cursor

    @override
    def _table_list(
        self, namespace: ParsedNamespaces, like: str | None = None, **kwargs: Any
    ) -> Any:
        cmd = "SELECT * FROM INFORMATION_SCHEMA.TABLES"
        return self.query(cmd, **kwargs)

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
        raise NotImplementedError

    @override
    def _table_desc(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        query = f"\n            SELECT\n                TABLE_SCHEMA\n                , TABLE_NAME\n                , COLUMN_NAME\n                , ORDINAL_POSITION\n                , COLUMN_DEFAULT\n                , IS_NULLABLE\n                , DATA_TYPE\n            FROM INFORMATION_SCHEMA.COLUMNS\n            WHERE TABLE_NAME = '{table}'"
        return self.query(query, **kwargs)

    @override
    def _table_head(self, table: ParsedNamespaces, n: int = 10, **kwargs: Any) -> Any:
        return self.query(f"SELECT * FROM {table} LIMIT {n}", **kwargs)

    @override
    def _table_props(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        raise NotImplementedError
