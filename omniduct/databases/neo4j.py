from __future__ import annotations

from typing import Any

from interface_meta import override

from omniduct.utils.debug import logger

from ._namespaces import ParsedNamespaces
from .base import DatabaseClient


class Neo4jClient(DatabaseClient):
    """
    This Duct connects to a Neo4j graph database server using the `neo4j` python
    library.
    """

    PROTOCOLS = ["neo4j"]
    DEFAULT_PORT = 7687
    DEFAULT_CURSOR_FORMATTER = "raw"

    @override
    @classmethod
    def statement_cleanup(cls, statement: str) -> str:
        return statement  # base statement cleanup assumes SQL

    @override
    def _init(self) -> None:
        self.__driver: Any = None

    # Connection
    @override
    def _connect(self) -> None:
        from neo4j.v1 import GraphDatabase

        logger.info("Connecting to Neo4J graph database ...")
        auth = (self.username, self.password) if self.username else None
        self.__driver = GraphDatabase.driver(
            f"bolt://{self.host}:{self.port}", auth=auth
        )  # TODO: Add kerberos support

    @override
    def _is_connected(self) -> bool:
        return hasattr(self, "__driver") and self.__driver is not None

    @override
    def _disconnect(self) -> None:
        logger.info("Disconnecting from Neo4J graph database ...")
        try:
            self.__driver.close()
        except:
            pass
        self.__driver = None

    # Querying
    @override
    def _execute(
        self,
        statement: str,
        cursor: Any,
        wait: bool,
        session_properties: dict[str, Any],
    ) -> Any:
        with self.__driver.session() as session:
            result = session.run(statement)

        # hacky: make the result look like a cursor
        result.close = result.detach
        result.fetchall = result.records
        return result

    @override
    def _table_exists(self, table: ParsedNamespaces, **kwargs: Any) -> bool:
        raise RuntimeError("tables do not apply to the Neo4J graph database")

    @override
    def _table_drop(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        raise RuntimeError("tables do not apply to the Neo4J graph database")

    @override
    def _table_desc(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        raise RuntimeError("tables do not apply to the Neo4J graph database")

    @override
    def _table_head(self, table: ParsedNamespaces, n: int = 10, **kwargs: Any) -> Any:
        raise RuntimeError("tables do not apply to the Neo4J graph database")

    @override
    def _table_list(self, namespace: ParsedNamespaces, **kwargs: Any) -> Any:
        raise RuntimeError("tables do not apply to the Neo4J graph database")

    @override
    def _table_props(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        raise RuntimeError("tables do not apply to the Neo4J graph database")
