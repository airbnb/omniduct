from __future__ import absolute_import

import ast
import logging
import re
import sys

import pandas.io.sql
import six
from future.utils import raise_with_traceback

from omniduct._version import __version__
from omniduct.utils.debug import logger

from .base import DatabaseClient
from ._schemas import SchemasMixin
from . import _pandas


class PrestoClient(DatabaseClient, SchemasMixin):
    """
    This Duct connects to a Facebook Presto server instance using the `pyhive`
    library.

    In addition to the standard `DatabaseClient` API, `PrestoClient` adds a
    `.schemas` descriptor attribute, which enables a tab completion driven
    exploration of a Presto database's schemas and tables.

    Attributes:
        catalog (str): The default catalog to use in database queries.
        schema (str): The default schema/database to use in database queries.
        connection_options (dict): Additional options to pass on to
            `pyhive.presto.connect(...)`.
    """

    PROTOCOLS = ['presto']
    DEFAULT_PORT = 3506
    SUPPORTS_SESSION_PROPERTIES = True
    NAMESPACE_NAMES = ['catalog', 'schema', 'table']
    NAMESPACE_QUOTECHAR = '"'
    NAMESPACE_SEPARATOR = '.'

    def _init(self, catalog='default', schema='default', server_protocol='http', source=None):
        """
        catalog (str): The default catalog to use in database queries.
        schema (str): The default schema/database to use in database queries.
        server_protocol (str): The protocol over which to connect to the Presto REST
            service ('http' or 'https'). (default='http')
        source (str): The source of this query (by default "omniduct <version>").
            If manually specified, result will be: "<source> / omniduct <version>".
        """
        self.catalog = catalog
        self.schema = schema
        self.server_protocol = server_protocol
        self.source = source
        self.__presto = None
        self.connection_fields += ('catalog', 'schema')

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source):
        omniduct_source = 'omniduct {}'.format(__version__)
        if source is None:
            self._source = omniduct_source
        else:
            self._source = "{} / {}".format(source, omniduct_source)

    # Connection

    def _connect(self):
        from sqlalchemy import create_engine, MetaData
        logging.getLogger('pyhive').setLevel(1000)  # Silence pyhive logging.
        logger.info('Connecting to Presto coordinator...')
        self._sqlalchemy_engine = create_engine('presto://{}:{}/{}/{}'.format(self.host, self.port, self.catalog, self.schema))
        self._sqlalchemy_metadata = MetaData(self._sqlalchemy_engine)

    def _is_connected(self):
        try:
            return self.__presto is not None
        except:
            return False

    def _disconnect(self):
        logger.info('Disconnecting from Presto coordinator...')
        try:
            self.__presto.close()
        except:
            pass
        self._sqlalchemy_engine = None
        self._sqlalchemy_metadata = None
        self._schemas = None

    # Querying
    def _execute(self, statement, cursor, wait, session_properties):
        """
        If something goes wrong, `PrestoClient` will attempt to parse the error
        log and present the user with useful debugging information. If that fails,
        the full traceback will be raised instead.
        """
        from pyhive import presto  # Imported here due to slow import performance in Python 3
        from pyhive.exc import DatabaseError  # Imported here due to slow import performance in Python 3
        try:
            cursor = cursor or presto.Cursor(
                host=self.host, port=self.port, username=self.username, password=self.password,
                catalog=self.catalog, schema=self.schema, session_props=session_properties,
                poll_interval=1, source=self.source, protocol=self.server_protocol
            )
            cursor.execute(statement)
            status = cursor.poll()
            if wait:
                logger.progress(0)
                # status None means command executed successfully
                # See https://github.com/dropbox/PyHive/blob/master/pyhive/presto.py#L234
                while status is not None and status['stats']['state'] != "FINISHED":
                    if status['stats'].get('totalSplits', 0) > 0:
                        pct_complete = round(status['stats']['completedSplits'] / float(status['stats']['totalSplits']), 4)
                        logger.progress(pct_complete * 100)
                    status = cursor.poll()
                logger.progress(100, complete=True)
            return cursor
        except (DatabaseError, pandas.io.sql.DatabaseError) as e:
            # Attempt to parse database error, before ultimately reraising the same
            # exception, maintaining the full stacktrace.
            exception, exception_args, traceback = sys.exc_info()

            try:
                message = e.args[0]
                if isinstance(message, six.string_types):
                    message = ast.literal_eval(re.match("[^{]*({.*})[^}]*$", message).group(1))

                linenumber = message['errorLocation']['lineNumber'] - 1
                splt = statement.splitlines()
                splt[linenumber] += '   <--  {errorType} ({errorName}) occurred. {message} '.format(**message)
                context = '\n\n[Error Context]\n{}\n'.format('\n'.join([splt[l] for l in range(max(linenumber - 1, 0),
                                                                                               min(linenumber + 2, len(splt)))]))

                class ErrContext(object):

                    def __repr__(self):
                        return context

                # logged twice so that both notebook and console users see the error context
                exception_args.args = [exception_args, ErrContext()]
                logger.error(context)
            except:
                logger.warn(("Omniduct was unable to parse the database error messages. Refer to the "
                             "traceback below for full error details."))

            if isinstance(exception, type):
                exception = exception(exception_args)

            raise_with_traceback(exception, traceback)

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
        """
        If if the schema namespace is not specified, `table.schema` will be
        defaulted to your username. Catalog overrides will be ignored, and will
        default to `self.catalog`.
        """
        table = self._parse_namespaces(table, defaults={'schema': self.username})
        return _pandas.to_sql(
            df=df, name=table.table, schema=table.schema, con=self._sqlalchemy_engine,
            index=False, if_exists=if_exists, **kwargs
        )

    def _cursor_empty(self, cursor):
        return False

    def _table_list(self, namespace, like=None, **kwargs):
        cmd = "SHOW TABLES "
        if namespace:
            cmd = cmd + " FROM " + namespace.name
        if like is not None:
            cmd = cmd + " LIKE " + like + "'"
        return self.query(cmd, **kwargs)

    def _table_exists(self, table, **kwargs):
        from pyhive.exc import DatabaseError

        logger.disabled = True
        try:
            self.table_desc(table, **kwargs)
            return True
        except DatabaseError:
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
