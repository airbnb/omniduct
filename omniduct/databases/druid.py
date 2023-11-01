from __future__ import absolute_import

from interface_meta import override

from omniduct.utils.debug import logger

from .base import DatabaseClient


class DruidClient(DatabaseClient):
    """
    This Duct connects to a Druid server using the `pydruid` python library.
    """

    PROTOCOLS = ["druid"]
    DEFAULT_PORT = 80
    NAMESPACE_NAMES = ["table"]
    NAMESPACE_QUOTECHAR = '"'
    NAMESPACE_SEPARATOR = "."

    @override
    def _init(self):
        self.__druid = None

    # Connection
    @override
    def _connect(self):
        from pydruid.db import connect  # pylint: disable=import-error

        logger.info("Connecting to Druid database ...")
        self.__druid = connect(  # pylint: disable=attribute-defined-outside-init
            self.host, self.port, path="/druid/v2/sql/", scheme="http"
        )
        if self.username or self.password:
            logger.warning(
                "Duct username and password not passed to pydruid connection. "
                "pydruid connection currently does not allow these fields to be passed."
            )

    @override
    def _is_connected(self):
        return self.__druid is not None

    @override
    def _disconnect(self):
        logger.info("Disconnecting from Druid database ...")
        try:
            self.__druid.close()
        except:  # pylint: disable=bare-except
            pass
        self.__druid = None  # pylint: disable=attribute-defined-outside-init

    # Querying
    @override
    def _execute(self, statement, cursor, wait, session_properties):
        cursor = cursor or self.__druid.cursor()
        cursor.execute(statement)
        return cursor

    @override
    def _table_list(self, namespace, like=None, **kwargs):
        cmd = "SELECT * FROM INFORMATION_SCHEMA.TABLES"
        return self.query(cmd, **kwargs)

    @override
    def _table_exists(self, table, **kwargs):
        logger.disabled = True
        try:
            self.table_desc(table, **kwargs)
            return True
        except:  # pylint: disable=bare-except
            return False
        finally:
            logger.disabled = False

    @override
    def _table_drop(self, table, **kwargs):
        raise NotImplementedError

    @override
    def _table_desc(self, table, **kwargs):
        query = f"\n            SELECT\n                TABLE_SCHEMA\n                , TABLE_NAME\n                , COLUMN_NAME\n                , ORDINAL_POSITION\n                , COLUMN_DEFAULT\n                , IS_NULLABLE\n                , DATA_TYPE\n            FROM INFORMATION_SCHEMA.COLUMNS\n            WHERE TABLE_NAME = '{table}'"
        return self.query(query, **kwargs)

    @override
    def _table_head(self, table, n=10, **kwargs):
        return self.query(f"SELECT * FROM {table} LIMIT {n}", **kwargs)

    @override
    def _table_props(self, table, **kwargs):
        raise NotImplementedError
