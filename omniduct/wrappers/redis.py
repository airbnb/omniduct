from __future__ import absolute_import

import redis
from omniduct.utils.debug import logger
from .base import WrapperClient

from omniduct.utils.magics import (MagicsProvider, process_line_arguments,
                                   process_line_cell_arguments)


class RedisClient(WrapperClient, MagicsProvider):
    """
    This Duct connects to a redis database server using the `redis` python library.
    """
    PROTOCOLS = ['redis']
    DEFAULT_PORT = 6379

    def _init(self):
        self._redis_connection = None

    def _connect(self):
        self._redis_connection = redis.Redis(self.host, self.port)

    def _is_connected(self):
        return hasattr(self, '_redis_connection') and self._redis_connection is not None

    def _disconnect(self):
        logger.info('Disconnecting from Redis database ...')
        self._redis_connection = None

    @property
    def wrapped_field(self):
        return '_redis_connection'

    def _register_magics(self, base_name):
        """
        The following magic functions will be registered (assuming that
        the base name is chosen to be 'redis'):
        - Cell Magics:
            - `%%redis`: Run the provided command

        Documentation for these magics is provided online.
        """
        from IPython.core.magic import register_cell_magic

        @register_cell_magic(base_name)
        @process_line_cell_arguments
        def execute_command_magic(*args, **kwargs):
            return self.execute_command(*args, **kwargs)
