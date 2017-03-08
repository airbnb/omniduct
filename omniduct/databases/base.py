from __future__ import absolute_import, print_function

import hashlib
import logging
import os
import sys
from abc import abstractmethod

import pandas as pd
import pandas.io.sql
import sqlparse
from decorator import decorator
from jinja2 import Template

from omniduct.caches.base import cached_method
from omniduct.duct import Duct
from omniduct.utils.config import config
from omniduct.utils.debug import logger, logging_scope
from omniduct.utils.magics import MagicsProvider, process_line_arguments

logging.getLogger('requests').setLevel(logging.WARNING)


config.register('date_fields',
                description='Default date fields to attempt to parse when databases',
                type=list)


@decorator
def sanitize_sqlalchemy_statement(f, self, statement, *args, **kwargs):
    try:
        from sqlalchemy.sql.base import Executable
        if isinstance(statement, Executable):
            statement = str(statement.compile(compile_kwargs={"literal_binds": True}))
    except ImportError:
        pass
    return f(self, statement, *args, **kwargs)


class DatabaseClient(Duct, MagicsProvider):
    '''
    QueryClient is an abstract class that can be subclassed to allow the databases
    of various data sources, such as databases or website apis.
    '''

    DUCT_TYPE = Duct.Type.DATABASE
    DEFAULT_PORT = None

    def __init__(self, *args, **kwargs):
        '''
        This is a shim __init__ function that passes all arguments onto
        `self._init`, which is implemented by subclasses. This allows subclasses
        to instantiate themselves with arbitrary parameters.
        '''
        Duct.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)
        self._init(*args, **kwargs)

    @abstractmethod
    def _init(self):
        pass

    def __call__(self, query, **kwargs):
        """Calls run() largely for backwards compatibility"""
        return self.query(query, **kwargs)

    # Querying
    @classmethod
    def statements_split(cls, statements):
        for statement in sqlparse.split(statements):
            statement = statement.strip()
            if statement.endswith(';'):
                statement = statement[:-1].strip()
            if statement:  # remove empty statements
                yield statement

    @classmethod
    def statement_cleanup(cls, statement):
        statement = sqlparse.format(statement, strip_comments=True, reindent=True)
        statement = os.linesep.join([line for line in statement.splitlines() if line])
        return statement

    @classmethod
    def statement_hash(cls, statement):
        statement = cls.statement_cleanup(statement)
        if sys.version_info.major == 3 or sys.version_info.major == 2 and isinstance(statement, unicode):
            statement = statement.encode('utf8')
        return hashlib.sha256(statement).hexdigest()

    @logging_scope("Query", timed=True)
    @sanitize_sqlalchemy_statement
    @cached_method(
        id_str=lambda self, kwargs: self.statement_hash(kwargs['statement']),
        use_cache=lambda self, kwargs: kwargs.pop('use_cache', True) and kwargs.get('query', False)
    )
    def execute(self, statement, query=False, parse=True, index_field=None, date_fields=None,
                cleanup_statement=True, render_only=False, **kwargs):
        '''
        Execute a statement against the data source.

        Parameters
        ----------
        statement : The statement to be executed by the query client.
        query : Whether this statement should return data, in which case `query` should be `True`;
            and `False` otherwise.
        parse : Whether the results of this query should be converted to a pandas DataFrame.
        index_field : The field to use as an index in the dataframe, or None.
        date_fields: List of fields to be converted to datetime objects, or None.
        kwargs : Extra keyword arguments to be passed on to _execute, as implemented by subclasses.

        Returns
        -------
        A pandas.DataFrame object if `query` and `parse` are both `True`.
        A DBAPI2 cursor object if `query` is `True`, and `parse` is `False`.
        `None` otherwise.
        '''
        self.connect()
        statements = self.statements_split(statement)
        statements = [self.statement_cleanup(stmt) if cleanup_statement else stmt for stmt in statements]
        assert len(statements) > 0, "No non-empty statements were provided."
        if render_only:
            return ';\n'.join(statements)
        cursor = None
        for statement in statements[:-1]:
            cursor = self.connect()._execute(statement, query=False, cursor=cursor, **kwargs)
        cursor = self.connect()._execute(statements[-1], query, cursor=cursor, **kwargs)

        if not query or self._cursor_empty(cursor):
            return None
        if parse:
            df = self._cursor_to_dataframe(cursor)
            cursor.close()

            if date_fields is None:  # if user supplied, use as is
                date_fields = config.date_fields or []
                date_fields = [field for field in date_fields if field in df]

            if date_fields:
                try:
                    df = pandas.io.sql._parse_date_columns(df, date_fields)
                except:
                    logger.error('Unable to parse date columns. Perhaps your version of pandas is outdated.')
            if index_field is not None:
                df.set_index(index_field, inplace=True)
            return df
        else:
            return cursor

    def query(self, query, **kwargs):
        '''
        This method  is shorthand for:
        >>> client.execute(query, query=True, **kwargs)
        '''
        return self.execute(query, query=True, **kwargs)

    def query_dump(self, query, file, format='hive', **kwargs):
        '''
        This method executes a query, and streams the results into a file. This is
        especially useful for large queries, which may not fit entirely in memory.
        Currently all fields are treated as strings.

        Parameters
        ----------
        query : The query to be executed on the data source.
        file : A file-like object with a `write` method that accepts strings; or
            a string representing a path to be written to.
        format : A string indicating the format of the outputted data.
            Valid options are 'hive' and 'csv'.
        kwargs : Additional keyword arguments passed on to `self.execute`.

        Returns
        -------
        An integer representing the number of output records.
        '''
        cursor = self.execute(query, parse=False, **kwargs)
        record = cursor.fetchone()
        if format == 'hive':
            sep = "\t"
            pre = ''
            post = '\n'
        else:
            sep = '","'
            pre = '"'
            post = '"\n'
        if isinstance(file, str):
            file = open(file, 'w')
        while record is not None:
            file.write(pre + sep.join([str(v).replace('"', '\"') for v in record]) + post)
            record = cursor.fetchone()
        file.close()
        cursor.close()

    def execute_from_file(self, file, **kwargs):
        """
        Read file contents and execute them against the connected data source.

        Parameters
        ----------
        file : str
            File containing a query
        kwargs : dict
            Extra parameters to pass on to `execute`.
        """
        with open(file, 'r') as f:
            query = f.read()
        return self.query(query, **kwargs)

    def query_from_file(self, file, **kwargs):
        '''
        This method is shorthand for:
        QueryClient.execute_from_file(file, parse=True, **kwargs)
        '''
        return self.execute_file(file, parse=True, **kwargs)

    # Uploading data to data store

    def push(self, df, table, overwrite=False, **kwargs):
        '''
        This method pushes a local dataframe `df` to the connected data store
        as a table `table`.

        Parameters
        ----------
        df : pandas.DataFrame
            The pandas dataframe to push into the data store.
        table : str
            The name of the table into which the dataframe should be pushed.
        overwrite : bool
            Whether the table should be replaced, if it already exists in the data store.
        kwargs : dict
            Additional arguments which are passed on to `QueryClient._push`.
        '''
        self._push(df, table, overwrite=overwrite, **kwargs)

    @abstractmethod
    def _push(self, df, table, overwrite=True, **kwargs):
        pass

    # Table properties

    @abstractmethod
    def _execute(self, statement, query=True, cursor=None, **kwargs):
        pass

    @abstractmethod
    def _cursor_empty(self, cursor):
        pass

    def _cursor_to_dataframe(self, cursor):
        records = cursor.fetchall()
        description = cursor.description
        return pd.DataFrame(data=records, columns=[c[0] for c in description])

    def table_list(self, **kwargs):
        '''
        Return a list of table names in the data source as a DataFrame. Additional kwargs are
        passed to `QueryClient._table_list`.
        '''
        return self._table_list(**kwargs)

    @abstractmethod
    def _table_list(self, **kwargs):
        pass

    def table_exists(self, table, **kwargs):
        '''
        Return boolean if table exists in schema
        '''
        return self._table_exists(table=table, **kwargs)

    @abstractmethod
    def _table_exists(self, table, **kwargs):
        pass

    def table_desc(self, table, **kwargs):
        '''
        Describe a table in the data source. Additional kwargs are
        passed to `QueryClient._table_desc`.

        Returns a pandas dataframe of table fields and descriptors.
        '''
        return self._table_desc(table, **kwargs)

    @abstractmethod
    def _table_desc(self, table, **kwargs):
        pass

    def table_head(self, table, n=10, **kwargs):
        '''
        Show a sample of the data in `table` of the data source. `n` is the number of records
        to show in this sample. The additional `kwargs` are passed on to `QueryClient._table_head`.

        Returns a pandas DataFrame.
        '''
        return self._table_head(table, n=n, **kwargs)

    @abstractmethod
    def _table_head(self, table, n=10, **kwargs):
        pass

    def table_props(self, table, **kwargs):
        '''
        Return a dataframe of table properties for `table`.
        '''
        return self._table_props(table, **kwargs)

    @abstractmethod
    def _table_props(self, table, **kwargs):
        pass

    def _register_magics(self, base_name):
        from IPython.core.magic import register_line_magic, register_cell_magic

        @register_cell_magic(base_name)
        @process_line_arguments
        def query_magic(statement, variable=None, show='head', template=True, name=None, auto_transpose=True, **kwargs):

            ip = get_ipython()

            # If name is specified, save this query as a template that can be used in subsequent queries
            if name is not None:
                if not hasattr(self, '_templates'):
                    self._templates = {}
                self._templates[name] = statement

            # Create the query by inserting stored templates where indicated by `::<name>::`
            # And then render the template using variables in the current user namespace.
            if template:
                statement = Template(statement,
                                     block_start_string='#{%#',
                                     block_end_string='#%}#',
                                     variable_start_string='::',
                                     variable_end_string='::',
                                     comment_start_string='#{%#',
                                     comment_end_string='#%}#').render(getattr(self, '_templates', {}))
                statement = Template(statement).render(ip.user_ns)

            result = self(statement, **kwargs)

            if variable is not None:
                ip.user_ns[variable] = result

            if kwargs.get('render_only', False):
                print(result, file=sys.stderr)
                return
            elif variable is None:
                return result

            if isinstance(show, int):
                r = result.head(show)
                if show <= 10:
                    r = r.T
                return r
            elif show == 'head':
                r = result.head()
            elif show == 'all':
                r = result
            elif show == 'none':
                return None
            else:
                raise ValueError("Omniduct does not recognise the argument show='{0}' in cell magic.".format(show))

            if auto_transpose and len(r) <= 10:
                return r.T
            return r

        @register_line_magic("{}.{}".format(base_name, 'desc'))
        @process_line_arguments
        def table_desc(table_name, **kwargs):
            return self.table_desc(table_name, **kwargs)

        @register_line_magic("{}.{}".format(base_name, 'head'))
        @process_line_arguments
        def table_desc(table_name, **kwargs):
            return self.table_head(table_name, **kwargs)

        @register_line_magic("{}.{}".format(base_name, 'props'))
        @process_line_arguments
        def table_desc(table_name, **kwargs):
            return self.table_props(table_name, **kwargs)
