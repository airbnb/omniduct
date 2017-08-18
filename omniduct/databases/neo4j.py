from __future__ import absolute_import

from omniduct.utils.debug import logger

from .base import DatabaseClient
from . import cursor_formatters


class Neo4jClient(DatabaseClient):

    PROTOCOLS = ['neo4j']
    DEFAULT_PORT = 7687
    DEFAULT_CURSOR_FORMATTER = 'raw'

    @classmethod
    def statement_cleanup(cls, statement):
        return statement  # base statement cleanup assumes SQL

    def _init(self):
        self.__driver = None

    # Connection
    def _connect(self):
        from neo4j.v1 import GraphDatabase
        logger.info('Connecting to Neo4J graph database ...')
        self.__driver = GraphDatabase.driver("bolt://{}:{}".format(self.host, self.port))

    def _is_connected(self):
        return hasattr(self, '__driver') and self.__driver is not None

    def _disconnect(self):
        logger.info('Disconnecting from Neo4J graph database ...')
        try:
            self.__driver.close()
        except Exception:
            pass
        self.__driver = None

    # Querying
    def _execute(self, statement, cursor=None, async=False):
        with self.__driver.session() as session:
            result = session.run(statement)

        # hacky: make the result look like a cursor
        result.close = result.detach
        result.fetchall = result.records
        return result

    def _table_exists(self, table, schema=None):
        raise Exception('tables do not apply to the Neo4J graph database')

    def _table_desc(self, table, **kwargs):
        raise Exception('tables do not apply to the Neo4J graph database')

    def _table_head(self, table, n=10, **kwargs):
        raise Exception('tables do not apply to the Neo4J graph database')

    def _table_list(self, table, schema=None):
        raise Exception('tables do not apply to the Neo4J graph database')

    def _table_props(self, table, **kwargs):
        raise Exception('tables do not apply to the Neo4J graph database')
