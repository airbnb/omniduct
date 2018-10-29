from __future__ import absolute_import

import pandas as pd
import sqlalchemy
from sqlalchemy import Table
from sqlalchemy import types as sql_types

from omniduct.utils.debug import logger

try:
    from pyhive.sqlalchemy_presto import PrestoDialect

    def get_columns(self, connection, table_name, schema=None, **kw):
        # Extend types supported by PrestoDialect as defined in PyHive
        type_map = {
            'bigint': sql_types.BigInteger,
            'integer': sql_types.Integer,
            'boolean': sql_types.Boolean,
            'double': sql_types.Float,
            'varchar': sql_types.String,
            'timestamp': sql_types.TIMESTAMP,
            'date': sql_types.DATE,
            'array<bigint>': sql_types.ARRAY(sql_types.Integer),
            'array<varchar>': sql_types.ARRAY(sql_types.String)
        }

        rows = self._get_table_columns(connection, table_name, schema)
        result = []
        for row in rows:
            try:
                coltype = type_map[row.Type]
            except KeyError:
                logger.warn("Did not recognize type '%s' of column '%s'" % (row.Type, row.Column))
                coltype = sql_types.NullType
            result.append({
                'name': row.Column,
                'type': coltype,
                # newer Presto no longer includes this column
                'nullable': getattr(row, 'Null', True),
                'default': None,
            })
        return result

    PrestoDialect.get_columns = get_columns
except ImportError:
    logger.debug("Not monkey patching pyhive's PrestoDialect.get_columns due to missing dependencies.")


class SchemasMixin(object):

    @property
    def schemas(self):
        """
        This object has as attributes the schemas on the current catalog. These
        schema objects in turn have the tables as SQLAlchemy `Table` objects.
        This allows tab completion and exploration of Databases.
        """
        from werkzeug import LocalProxy

        def get_schemas():
            if not getattr(self, '_schemas', None):
                self.connect()
                assert getattr(self, '_sqlalchemy_metadata', None) is not None, (
                    "`{class_name}` instances do not provide the required sqlalchemy metadata "
                    "for schema exploration.".format(self.__class__.__name__)
                )
                self._schemas = Schemas(self._sqlalchemy_metadata)
            return self._schemas
        return LocalProxy(get_schemas)


# Extend Table to support returning pandas description of table
class TableDesc(Table):

    def desc(self):
        return pd.DataFrame(
            [[col.name, col.type] for col in self.columns.values()],
            columns=['name', 'type']
        )

    def head(self, n=10):
        return pd.read_sql(
            'SELECT * FROM "{}"."{}"'.format(self.schema, self.name)
            + 'LIMIT {}'.format(n) if n is not None else '',
            self.bind
        )

    def dump(self):
        return self.head(n=None)

    def __repr__(self):
        return self.desc().__repr__()


# Define helpers to allow for table completion/etc
class Schemas(object):

    def __init__(self, metadata):
        self._metadata = metadata
        self._schema_names = sqlalchemy.inspect(self._metadata.bind).get_schema_names()
        self._schema_cache = {}

    def __dir__(self):
        return self._schema_names

    def all(self):
        return self._schema_names

    def __getattr__(self, value):
        if value in self._schema_names:
            if value not in self._schema_cache:
                self._schema_cache[value] = Schema(metadata=self._metadata, schema=value)
            return self._schema_cache[value]
        raise AttributeError("No such schema {}".format(value))

    def __repr__(self):
        return "<Schemas: {} schemas>".format(len(self._schema_names))

    def __iter__(self):
        for schema in self._schema_names:
            yield schema

    def __len__(self):
        return len(self._schema_names)


class Schema(object):

    def __init__(self, metadata, schema):
        self._metadata = metadata
        self._schema = schema
        self._table_cache = {}
        self._table_names = None

    @property
    def table_names(self):
        if self._table_names is None:
            self._table_names = sqlalchemy.inspect(self._metadata.bind).get_table_names(self._schema)
        return self._table_names

    def __dir__(self):
        return self.table_names

    def all(self):
        return self.table_names

    def __getattr__(self, table):
        if table in self.table_names:
            if table not in self._table_cache:
                self._table_cache[table] = TableDesc(
                    '{}'.format(table), self._metadata, autoload=True, schema=self._schema
                )
            return self._table_cache[table]
        raise AttributeError("No such table {}".format(table))

    def __repr__(self):
        return "<Schema `{}`: {} tables>".format(self._schema, len(self.table_names))

    def __iter__(self):
        for schema in self.table_names:
            yield schema

    def __len__(self):
        return len(self.table_names)
