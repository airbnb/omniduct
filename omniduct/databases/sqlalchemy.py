import pandas as pd

from .base import DatabaseClient


class SQLAlchemyClient(DatabaseClient):

    PROTOCOLS = ['sqlalchemy', 'firebird', 'mssql', 'mysql', 'oracle', 'postgresql', 'sybase']

    def _init(self, dialect=None, driver=None, database=''):

        assert self._port is not None, "Omniduct requires SQLAlchemy databases to manually specify a port, as " \
                                       "it will often be the case that ports are being forwarded."

        if self.protocol is not 'sqlalchemy':
            self.dialect = self.protocol
        else:
            self.dialect = dialect
        assert self.dialect is not None, "Dialect not specified."

        self.driver = driver
        self.database = database
        self.__hive = None
        self.connection_fields += ('schema',)

        self.engine = None
        self.connection = None

    @property
    def db_uri(self):
        return '{dialect}://{login}@{host_port}/{database}'.format(
            dialect=self.dialect + ("+{}".format(self.driver) if self.driver else ''),
            login=self.username + (":{}".format(self.password) if self.password else ''),
            host_port=self.host + (":{}".format(self.port) if self.port else ''),
            database=self.database
        )

    def _connect(self):
        import sqlalchemy
        self.engine = sqlalchemy.create_engine(self.db_uri)
        self.connection = self.engine.connect()

    def _push(self):
        raise NotImplementedError

    def _is_connected(self):
        return self.connection and not self.connection.closed

    def _disconnect(self):
        if self.connection:
            self.connection.close()
        self.connection = None
        self.engine = None

    def _execute(self, statement, query=True, cursor=None, **kwargs):
        if cursor:
            cursor.execute(statement)
        else:
            cursor = self.connection.execute(statement).cursor
        return cursor

    def _cursor_empty(self, cursor):
        return False

    def _cursor_to_dataframe(self, cursor):
        records = list(cursor.cursor.fetchall())
        description = cursor.cursor.description
        return pd.DataFrame(data=records, columns=[c[0] for c in description])

    def _table_list(self, **kwargs):
        return self.query("SHOW TABLES", **kwargs)

    def _table_exists(self, table, schema=None):
        return (self.table_list(renew=True, schema=schema)['Table'] == table).any()

    def _table_desc(self, table, **kwargs):
        return self.query("DESCRIBE {0}".format(table), **kwargs)

    def _table_head(self, table, n=10, **kwargs):
        return self.query("SELECT * FROM {} LIMIT {}".format(table, n), **kwargs)

    def _table_props(self, table, **kwargs):
        raise NotImplementedError
