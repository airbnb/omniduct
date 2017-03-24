import logging

import pandas as pd
from sqlalchemy import (ARRAY, Boolean, Column, Float, Integer, MetaData,
                        String, Table, inspect, types)

from pyhive.sqlalchemy_presto import PrestoDialect

logger = logging.getLogger(__name__)


# Extend types supported by PrestoDialect as defined in PyHive
_type_map = {
    'bigint': types.BigInteger,
    'integer': types.Integer,
    'boolean': types.Boolean,
    'double': types.Float,
    'varchar': types.String,
    'timestamp': types.TIMESTAMP,
    'date': types.DATE,
    'array<bigint>': ARRAY(Integer),
    'array<varchar>': ARRAY(String)
}


def get_columns(self, connection, table_name, schema=None, **kw):
    rows = self._get_table_columns(connection, table_name, schema)
    result = []
    for row in rows:
        try:
            coltype = _type_map[row.Type]
        except KeyError:
            logger.warn("Did not recognize type '%s' of column '%s'" % (row.Type, row.Column))
            coltype = types.NullType
        result.append({
            'name': row.Column,
            'type': coltype,
            # newer Presto no longer includes this column
            'nullable': getattr(row, 'Null', True),
            'default': None,
        })
    return result


PrestoDialect.get_columns = get_columns


# Extend Table to support returning pandas description of table
class TableDesc(Table):

    def desc(self):
        return pd.read_sql('describe "{}"."{}"'.format(self.schema, self.name), self.bind)

    def head(self, n=10):
        return pd.read_sql('SELECT * FROM "{}"."{}" LIMIT {}'.format(self.schema, self.name, n), self.bind)


# Define helpers to allow for table completion/etc
class Schemas(object):

    def __init__(self, metadata):
        self._metadata = metadata
        self._schema_names = inspect(self._metadata.bind).get_schema_names()
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
        self._table_names = inspect(self._metadata.bind).get_table_names(schema)
        self._table_cache = {}

    def __dir__(self):
        return self._table_names

    def all(self):
        return self._table_names

    def __getattr__(self, table):
        if table in self._table_names:
            if table not in self._table_cache:
                self._table_cache[table] = TableDesc('{}'.format(table), self._metadata,
                                                     autoload=True,
                                                     schema=self._schema
                                                     )
            return self._table_cache[table]
        raise AttributeError("No such table {}".format(table))

    def __repr__(self):
        return "<Schema `{}`: {} tables>".format(self._schema, len(self._table_names))

    def __iter__(self):
        for schema in self._table_names:
            yield schema

    def __len__(self):
        return len(self._table_names)
