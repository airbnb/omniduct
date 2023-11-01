from __future__ import absolute_import

from interface_meta import override

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

    PROTOCOLS = [
        "sqlalchemy",
        "firebird",
        "mssql",
        "mysql",
        "oracle",
        "postgresql",
        "sybase",
        "snowflake",
    ]
    NAMESPACE_NAMES = ["database", "table"]
    NAMESPACE_QUOTECHAR = '"'  # TODO: Apply overrides depending on protocol?
    NAMESPACE_SEPARATOR = "."

    @property
    @override
    def NAMESPACE_DEFAULT(self):
        return {"database": self.database}

    @override
    def _init(self, dialect=None, driver=None, database="", engine_opts=None):
        assert self._port is not None, (
            "Omniduct requires SQLAlchemy databases to manually specify a port, as "
            "it will often be the case that ports are being forwarded."
        )

        if self.protocol != "sqlalchemy":
            self.dialect = self.protocol
        else:
            self.dialect = dialect
        assert self.dialect is not None, "Dialect not specified."

        self.driver = driver
        self.database = database
        self.connection_fields += ("schema",)
        self.engine_opts = engine_opts or {}

        self.engine = None
        self.connection = None

    @property
    def db_uri(self):
        # pylint: disable-next=consider-using-f-string
        return "{dialect}://{login}@{host_port}/{database}".format(
            dialect=self.dialect + (f"+{self.driver}" if self.driver else ""),
            login=self.username + (f":{self.password}" if self.password else ""),
            host_port=self.host + (f":{self.port}" if self.port else ""),
            database=self.database,
        )

    @override
    def _connect(self):
        import sqlalchemy

        if self.protocol not in ["mysql"]:
            logger.warning(
                "While querying and executing should work as "
                "expected, some operations on this database client "
                "(such as listing tables, querying to tables, etc) "
                "may not function as expected due to the backend "
                "not supporting ANSI SQL."
            )

        # pylint: disable-next=attribute-defined-outside-init
        self.engine = sqlalchemy.create_engine(self.db_uri, **self.engine_opts)
        self._sqlalchemy_metadata = sqlalchemy.MetaData(self.engine)

    @override
    def _is_connected(self):
        return self.engine is not None

    @override
    def _disconnect(self):
        # pylint: disable-next=attribute-defined-outside-init
        self.engine = None
        self._sqlalchemy_metadata = None
        # pylint: disable-next=attribute-defined-outside-init
        self._schemas = None

    @override
    def _execute(
        self, statement, cursor, wait, session_properties, query=True, **kwargs
    ):
        assert wait, "`SQLAlchemyClient` does not support asynchronous operations."
        if cursor:
            cursor.execute(statement)
        else:
            cursor = self.engine.execute(statement).cursor
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
    def _dataframe_to_table(self, df, table, if_exists="fail", **kwargs):
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
        return self.query(f"SHOW TABLES IN {namespace}", **kwargs)

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
        return self.execute(f"DROP TABLE {table}")

    @override
    def _table_desc(self, table, **kwargs):
        return self.query(f"DESCRIBE {table}", **kwargs)

    @override
    def _table_head(self, table, n=10, **kwargs):
        return self.query(f"SELECT * FROM {table} LIMIT {n}", **kwargs)

    @override
    def _table_props(self, table, **kwargs):
        raise NotImplementedError
