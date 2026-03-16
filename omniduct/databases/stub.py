from __future__ import annotations

from typing import Any

from omniduct.databases._namespaces import ParsedNamespaces
from omniduct.databases.base import DatabaseClient


class StubDatabaseClient(DatabaseClient):
    PROTOCOLS: list[str] = []
    DEFAULT_PORT: int | None = None

    def _init(self) -> None:
        pass

    # Connection management

    def _connect(self) -> None:
        raise NotImplementedError

    def _is_connected(self) -> bool:
        raise NotImplementedError

    def _disconnect(self) -> None:
        raise NotImplementedError

    # Database operations

    def _execute(
        self,
        statement: str,
        cursor: Any,
        wait: bool,
        session_properties: dict[str, Any],
        **kwargs: Any,
    ) -> Any:
        raise NotImplementedError

    def _table_list(self, namespace: ParsedNamespaces, **kwargs: Any) -> Any:
        raise NotImplementedError

    def _table_exists(self, table: ParsedNamespaces, **kwargs: Any) -> bool:
        raise NotImplementedError

    def _table_desc(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        raise NotImplementedError

    def _table_head(self, table: ParsedNamespaces, n: int = 10, **kwargs: Any) -> Any:
        raise NotImplementedError

    def _table_props(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        raise NotImplementedError

    def _table_drop(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        raise NotImplementedError
