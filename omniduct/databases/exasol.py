from __future__ import absolute_import

from interface_meta import override

from omniduct.utils.debug import logger

from .base import DatabaseClient
from . import _pandas


class ExasolClient(DatabaseClient):
    """
    This client connects to an Exasol service using the `pyexasol` python library.

    An example configuration for exasol.

    databases:
        exasol_db:
            protocol: exasol
            host:
              - 'localhost:8563'
              - 'localhost:8564'
            username: exasol_user
            password: ****
            schema: users
    """

    PROTOCOLS = ["exasol"]
    DEFAULT_PORT = 8563
    NAMESPACE_NAMES = ["schema", "table"]
    NAMESPACE_QUOTECHAR = '"'
    NAMESPACE_SEPARATOR = "."

    @property
    @override
    def NAMESPACE_DEFAULT(self):
        return {"database": self.schema}

    @override
    def _init(self, schema=None, engine_opts=None):
        self.__exasol = None

        self.schema = schema
        self.connection_fields += ("schema",)
        self.engine_opts = engine_opts or {}

    @override
    def _connect(self):
        import pyexasol

        logger.info("Connecting to Exasol ...")
        self.__exasol = pyexasol.connect(
            dsn=f"{self.host}:{self.port}",
            user=self.username,
            password=self.password,
            **self.engine_opts,
        )

    @override
    def _is_connected(self):
        return self.__exasol is not None

    @override
    def _disconnect(self):
        try:
            self.__exasol.close()
        except Exception:
            pass
        self.__exasol = None
        self._schemas = None

    @override
    def _execute(self, statement, cursor, wait, session_properties, query=True):
        #: pyexasol.ExaStatement has a similar interface to that of
        # a DBAPI2 cursor.
        cursor = self.__exasol.execute(statement)

        # hacky: make the result look like a cursor
        cursor.description = cursor.columns()
        return cursor

    @override
    def _query_to_table(self, statement, table, if_exists, **kwargs):
        statements = []

        if if_exists == "fail" and self.table_exists(table):
            raise RuntimeError("Table {} already exists!".format(table))
        elif if_exists == "replace":
            statements.append("DROP TABLE IF EXISTS {};".format(table))
        elif if_exists == "append":
            raise NotImplementedError(
                "Append operations have not been implemented for {}.".format(
                    self.__class__.__name__
                )
            )

        statement = "CREATE TABLE {table} AS ({statement})".format(
            table=table, statement=statement
        )
        return self.execute(statement, **kwargs)

    @override
    def _dataframe_to_table(self, df, table, if_exists="fail", **kwargs):
        table = self._parse_namespaces(table, defaults={"schema": self.username})
        return _pandas.to_sql(
            df=df,
            name=table.table,
            schema=table.database,
            con=self.engine,
            index=False,
            if_exists=if_exists,
            **kwargs,
        )

    @override
    def _table_list(self, namespace, **kwargs):
        return self.query("SHOW TABLES IN {}".format(namespace), **kwargs)

    @override
    def _table_exists(self, table, **kwargs):
        logger.disabled = True
        try:
            self.table_desc(table, **kwargs)
            return True
        except:
            return False
        finally:
            logger.disabled = False

    @override
    def _table_drop(self, table, **kwargs):
        return self.execute("DROP TABLE {table}".format(table=table))

    @override
    def _table_desc(self, table, **kwargs):
        return self.query("DESCRIBE {0}".format(table), **kwargs)

    @override
    def _table_head(self, table, n=10, **kwargs):
        return self.query("SELECT * FROM {} LIMIT {}".format(table, n), **kwargs)

    @override
    def _table_props(self, table, **kwargs):
        raise NotImplementedError
