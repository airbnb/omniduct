from interface_meta import override
from omniduct.databases.base import DatabaseClient


class StubDatabaseClient(DatabaseClient):

    PROTOCOLS = []
    DEFAULT_PORT = None

    @override
    def _init(self):
        pass

    # Connection management

    @override
    def _connect(self):
        raise NotImplementedError

    @override
    def _is_connected(self):
        raise NotImplementedError

    @override
    def _disconnect(self):
        raise NotImplementedError

    # Database operations

    @override
    def _execute(self, statement, cursor, wait, session_properties, **kwargs):
        raise NotImplementedError

    @override
    def _table_list(self, **kwargs):
        raise NotImplementedError

    @override
    def _table_exists(self, table, **kwargs):
        raise NotImplementedError

    @override
    def _table_desc(self, table, **kwargs):
        raise NotImplementedError

    @override
    def _table_head(self, table, n=10, **kwargs):
        raise NotImplementedError

    @override
    def _table_props(self, table, **kwargs):
        raise NotImplementedError

    @override
    def _table_drop(self, table, **kwargs):
        raise NotImplementedError
