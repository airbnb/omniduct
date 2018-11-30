from __future__ import absolute_import

from omniduct.utils.debug import logger

from .base import DatabaseClient
from ._schemas import SchemasMixin
from . import _pandas


class SQLAlchemyClient(DatabaseClient, SchemasMixin):
    """
    This Duct connects to several different databases using one of several
    SQLAlchemy drivers. In general, these are provided for their potential
    utility, but will be less functional than the specially crafted database
    clients.
    """

    PROTOCOLS = ['sqlalchemy', 'firebird', 'mssql', 'mysql', 'oracle', 'postgresql', 'sybase', 'snowflake']
    NAMESPACE_NAMES = ['database', 'table']
    NAMESPACE_QUOTECHAR = '"'  # TODO: Apply overrides depending on protocol?
    NAMESPACE_SEPARATOR = '.'

    def _init(self, dialect=None, driver=None, database='', engine_opts=None):

        assert self._port is not None, "Omniduct requires SQLAlchemy databases to manually specify a port, as " \
                                       "it will often be the case that ports are being forwarded."

        if self.protocol is not 'sqlalchemy':
            self.dialect = self.protocol
        else:
            self.dialect = dialect
        assert self.dialect is not None, "Dialect not specified."

        self.driver = driver
        self.database = database
        self.connection_fields += ('schema',)
        self.engine_opts = engine_opts or {}

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
        if self.protocol not in ['mysql']:
            logger.warning("While querying and executing should work as "
                           "expected, some operations on this database client "
                           "(such as listing tables, querying to tables, etc) "
                           "may not function as expected due to the backend "
                           "not supporting ANSI SQL.")

        self.engine = sqlalchemy.create_engine(self.db_uri, **self.engine_opts)
        self._sqlalchemy_metadata = sqlalchemy.MetaData(self.engine)

    def _is_connected(self):
        return self.engine is not None

    def _disconnect(self):
        self.engine = None
        self._sqlalchemy_metadata = None
        self._schemas = None

    def _execute(self, statement, cursor, wait, session_properties, query=True, **kwargs):
        assert wait, "`SQLAlchemyClient` does not support asynchronous operations."
        if cursor:
            cursor.execute(statement)
        else:
            cursor = self.engine.execute(statement).cursor
        return cursor

    def _query_to_table(self, statement, table, if_exists, **kwargs):
        statements = []

        if if_exists == 'fail' and self.table_exists(table):
            raise RuntimeError("Table {} already exists!".format(table))
        elif if_exists == 'replace':
            statements.append('DROP TABLE IF EXISTS {};'.format(table))
        elif if_exists == 'append':
            raise NotImplementedError("Append operations have not been implemented for {}.".format(self.__class__.__name__))

        statement = "CREATE TABLE {table} AS ({statement})".format(
            table=table,
            statement=statement
        )
        return self.execute(statement, **kwargs)

    def _dataframe_to_table(self, df, table, if_exists='fail', **kwargs):
        return _pandas.to_sql(
            df=df, name=table.table, schema=table.database, con=self.engine,
            index=False, if_exists=if_exists, **kwargs
        )

    def _table_list(self, **kwargs):
        return self.query("SHOW TABLES", **kwargs)

    def _table_exists(self, table, **kwargs):
        logger.disabled = True
        try:
            self.table_desc(table, **kwargs)
            return True
        except:
            return False
        finally:
            logger.disabled = False

    def _table_drop(self, table, **kwargs):
        return self.execute("DROP TABLE {table}".format(table=table))

    def _table_desc(self, table, **kwargs):
        return self.query("DESCRIBE {0}".format(table), **kwargs)

    def _table_head(self, table, n=10, **kwargs):
        return self.query("SELECT * FROM {} LIMIT {}".format(table, n), **kwargs)

    def _table_props(self, table, **kwargs):
        raise NotImplementedError
