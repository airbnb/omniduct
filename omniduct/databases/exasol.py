from __future__ import absolute_import

from interface_meta import override

from omniduct.utils.debug import logger

from .base import DatabaseClient


class ExasolClient(DatabaseClient):
    """
    This client connects to an Exasol service using the `pyexasol` python library.

    Example Config
    --------------

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

    @override
    @property
    def NAMESPACE_DEFAULT(self):
        return {"schema": self.schema}

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
        # pylint: disable-next=attribute-defined-outside-init
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
        except:  # pylint: disable=bare-except
            pass
        # pylint: disable-next=attribute-defined-outside-init
        self.__exasol = None

    @override
    def _execute(self, statement, cursor, wait, session_properties, query=True):
        # pyexasol.ExaStatement has a similar interface to that of
        # a DBAPI2 cursor.
        cursor = cursor or self.__exasol.execute(statement)

        # hacky: make the result look like a cursor.
        # cursor.columns returns a dict with the required attributes
        cursor.description = []
        for key, values in cursor.columns().items():
            cursor.description.append(
                (
                    key,
                    values.get("type", None),
                    values.get("size", None),
                    values.get("size", None),
                    values.get("precision", None),
                    values.get("scale", None),
                    True,
                )
            )

        return cursor

    @override
    def _query_to_table(self, statement, table, if_exists, **kwargs):
        statements = []

        if if_exists == "fail" and self.table_exists(table):
            raise RuntimeError(f"Table {table} already exists!")
        if if_exists == "replace":
            statements.append(f"DROP TABLE IF EXISTS {table};")
        elif if_exists == "append":
            raise NotImplementedError(
                f"Append operations have not been implemented for {self.__class__.__name__}."
            )

        statement = f"CREATE TABLE {table} AS ({statement})"
        return self.execute(statement, **kwargs)

    @override
    def _table_list(self, namespace, **kwargs):
        # Since this namespace is a conditional, exasol requires single quotations
        # instead of double quotations. " -> '
        exasol_namespace = namespace.render(quote_char="'")
        query = f"SELECT TABLE_NAME FROM EXA_ALL_TABLES WHERE table_schema={exasol_namespace}"
        return self.query(query, **kwargs)

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
        # Schema and tables are always under uppercase namespaces.
        return self.execute(f"DROP TABLE {str(table).upper()}", **kwargs)

    @override
    def _table_desc(self, table, **kwargs):
        # Schema and tables are always under uppercase namespaces.
        return self.query(f"DESCRIBE {str(table).upper()}", **kwargs)

    @override
    def _table_head(self, table, n=10, **kwargs):
        # Schema and tables are always under uppercase namespaces.
        return self.query(f"SELECT * FROM {str(table).upper()} LIMIT {n}", **kwargs)

    @override
    def _table_props(self, table, **kwargs):
        raise NotImplementedError
