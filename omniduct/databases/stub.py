from omniduct.databases.base import DatabaseClient


class StubDatabaseClient(DatabaseClient):

    PROTOCOLS = []
    DEFAULT_PORT = None

    def _init(self):
        pass

    # Connection management

    def _connect(self):
        raise NotImplementedError

    def _is_connected(self):
        raise NotImplementedError

    def _disconnect(self):
        raise NotImplementedError

    # Database operations

    def _execute(self, statement, cursor, wait, session_properties, **kwargs):
        raise NotImplementedError

    def _table_list(self, **kwargs):
        raise NotImplementedError

    def _table_exists(self, table, **kwargs):
        raise NotImplementedError

    def _table_desc(self, table, **kwargs):
        raise NotImplementedError

    def _table_head(self, table, n=10, **kwargs):
        raise NotImplementedError

    def _table_props(self, table, **kwargs):
        raise NotImplementedError
