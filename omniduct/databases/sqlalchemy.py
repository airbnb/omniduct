import urllib.parse

from interface_meta import override

from omniduct.utils.debug import logger

from . import _pandas
from ._schemas import SchemasMixin
from .base import DatabaseClient


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
        if self._port is None:
            raise ValueError(
                "Omniduct requires SQLAlchemy databases to manually specify a port, as "
                "it will often be the case that ports are being forwarded."
            )

        if self.protocol != "sqlalchemy":
            self.dialect = self.protocol
        else:
            self.dialect = dialect
        if self.dialect is None:
            raise ValueError("Dialect not specified.")

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
            login=self.username
            + (f":{urllib.parse.quote_plus(self.password)}" if self.password else ""),
            host_port=self.host + (f":{self.port}" if self.port else ""),
            database=self.database,
        )

    @property
    def _sqlalchemy_engine(self):
        """
        The SQLAlchemy engine object for the SchemasMixin.
        """
        return self.engine

    @_sqlalchemy_engine.setter
    def _sqlalchemy_engine(self, engine):
        self.engine = engine

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
        # pylint: disable-next=attribute-defined-outside-init
        self.connection = self.engine.connect()

    @override
    def _is_connected(self):
        return self.connection is not None

    @override
    def _disconnect(self):
        if self.connection is not None:
            self.connection.close()
        # pylint: disable-next=attribute-defined-outside-init
        self.connection = None
        # pylint: disable-next=attribute-defined-outside-init
        self.engine = None
        # pylint: disable-next=attribute-defined-outside-init
        self._schemas = None

    @override
    def _execute(
        self, statement, cursor, wait, session_properties, query=True, **kwargs
    ):
        import sqlalchemy

        if not wait:
            raise RuntimeError(
                "`SQLAlchemyClient` does not support asynchronous operations."
            )

        if self.connection is None:
            raise RuntimeError("Not connected.")
        if cursor:
            cursor.execute(statement)
        else:
            cursor = self.connection.execute(sqlalchemy.text(statement)).cursor  # type: ignore[attr-defined]
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
        # Use parameterized query to avoid SQL injection
        query = f"SELECT * FROM {table} LIMIT %s"
        return self.query(query, n, **kwargs)

    @override
    def _table_props(self, table, **kwargs):
        raise NotImplementedError
