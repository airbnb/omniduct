from __future__ import absolute_import

from omniduct.utils.debug import logger

from .base import DatabaseClient


class DruidClient(DatabaseClient):

    PROTOCOLS = ['druid']
    DEFAULT_PORT = 80

    def _init(self):
        self.__druid = None

    # Connection
    def _connect(self):
        from pydruid.db import connect
        logger.info('Connecting to Druid database ...')
        self.__druid = connect(self.host, self.port, path='/druid/v2/sql/', scheme='http')

    def _is_connected(self):
        return self.__druid is not None

    def _disconnect(self):
        logger.info('Disconnecting from Druid database ...')
        try:
            self.__druid.close()
        except Exception:
            pass
        self.__druid = None

    # Querying
    def _execute(self, statement, cursor=None, async=False):
        cursor = cursor or self.__druid.cursor()
        cursor.execute(statement)
        return cursor

    def _table_list(self, schema=None, like=None, **kwargs):
        cmd = "SELECT * FROM INFORMATION_SCHEMA.TABLES"
        return self.query(cmd, **kwargs)

    def _table_exists(self, table, schema=None):
        return (self.table_list(renew=True, schema=schema)['TABLE_NAME'] == table).any()

    def _table_desc(self, table, **kwargs):
        query = ("""
            SELECT
                TABLE_SCHEMA
                , TABLE_NAME
                , COLUMN_NAME
                , ORDINAL_POSITION
                , COLUMN_DEFAULT
                , IS_NULLABLE
                , DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{}'""").format(table)
        return self.query(query, **kwargs)

    def _table_head(self, table, n=10, **kwargs):
        return self.query("SELECT * FROM {} LIMIT {}".format(table, n), **kwargs)

    def _table_props(self, table, **kwargs):
        raise NotImplementedError
