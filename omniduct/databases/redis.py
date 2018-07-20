from __future__ import absolute_import

import redis
from omniduct.utils.debug import logger

from .base import DatabaseClient


class RedisCursor(object):

    def __init__(self, result):
        self.result = result

    def close(self):
        pass

    def fetchall(self):
        yield self.result


class RedisClient(DatabaseClient):
    """
    This Duct connects to a redis database server using the `redis` python
    library.
    """

    PROTOCOLS = ['redis']
    DEFAULT_PORT = 6379
    DEFAULT_CURSOR_FORMATTER = 'singleton'

    @classmethod
    def statement_cleanup(cls, statement):
        return statement  # base statement cleanup assumes SQL

    def _init(self):
        self.__redis_connection = None

    # Connection
    def _connect(self):
        self.__redis_connection = redis.Redis(
            self.host,
            self.port,
        )

    def _is_connected(self):
        return hasattr(self, '__redis_connection') and self.__redis_connection is not None

    def _disconnect(self):
        logger.info('Disconnecting from Redis database ...')
        self.__redis_connection = None

    # Querying
    def _execute(self, statement, cursor=None, asynchronous=False):
        return RedisCursor(self.__redis_connection.execute_command(statement))

    def _table_exists(self, table, schema=None):
        raise Exception('tables do not apply to the Redis database')

    def _table_desc(self, table, **kwargs):
        raise Exception('tables do not apply to the Redis database')

    def _table_head(self, table, n=10, **kwargs):
        raise Exception('tables do not apply to the Redis database')

    def _table_list(self, table, schema=None):
        raise Exception('tables do not apply to the Redis database')

    def _table_props(self, table, **kwargs):
        raise Exception('tables do not apply to the Redis database')
