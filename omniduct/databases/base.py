from __future__ import absolute_import, print_function

import hashlib
import inspect
import logging
import os
import sys
from abc import abstractmethod

import sqlparse
from decorator import decorator
from jinja2 import StrictUndefined, Template

from . import cursor_formatters
from omniduct.caches.base import cached_method
from omniduct.duct import Duct
from omniduct.utils.debug import logger, logging_scope
from omniduct.utils.magics import MagicsProvider, process_line_arguments, process_line_cell_arguments

logging.getLogger('requests').setLevel(logging.WARNING)


def cache_serializer(format):
    return DatabaseClient.CURSOR_FORMATTERS[format].serialize


def cache_deserializer(format):
    return DatabaseClient.CURSOR_FORMATTERS[format].deserialize


@decorator
def render_statement(method, self, statement, *args, **kwargs):
    if kwargs.pop('template', True):
        statement = self.render_template(
            statement,
            context=kwargs.pop('context', {}),
            by_name=False,
        )
    return method(self, statement, *args, **kwargs)


class DatabaseClient(Duct, MagicsProvider):
    """
    `DatabaseClient` is an abstract subclass of `Duct` that provides a common
    API for all database clients, which in turn will be subclasses of this
    class.

    Allow use of `DatabaseClient(...)` as a short-hand for
    `DatabaseClient.query()`.

    Class Attributes:
        DUCT_TYPE (`Duct.Type`): The type of `Duct` protocol implemented by this class.
        DEFAULT_PORT (int): The default port for the database service (defined
            by subclasses).
        CURSOR_FORMATTERS (dict<str, CursorFormatter): asdsd
        DEFAULT_CURSOR_FORMATTER (str): ...
    """

    DUCT_TYPE = Duct.Type.DATABASE
    DEFAULT_PORT = None

    CURSOR_FORMATTERS = {
        'pandas': cursor_formatters.PandasCursorFormatter,
        'hive': cursor_formatters.HiveCursorFormatter,
        'csv': cursor_formatters.CsvCursorFormatter,
        'tuple': cursor_formatters.TupleCursorFormatter,
        'dict': cursor_formatters.DictCursorFormatter,
        'raw': cursor_formatters.RawCursorFormatter,
    }
    DEFAULT_CURSOR_FORMATTER = 'pandas'

    def __init__(self, *args, **kwargs):
        """
        This is a shim __init__ function that passes all arguments onto
        `self._init`, which is implemented by subclasses. This allows subclasses
        to instantiate themselves with arbitrary parameters.
        """
        Duct.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)

        self._templates = kwargs.pop('templates', {})
        self._template_context = kwargs.pop('template_context', {})
        self._sqlalchemy_engine = None
        self._sqlalchemy_metadata = None

        self._init(*args, **kwargs)

    @abstractmethod
    def _init(self):
        pass

    def __call__(self, query, **kwargs):
        """
        Allow use of `DatabaseClient(...)` as a short-hand for
        `DatabaseClient.query()`.
        """
        return self.query(query, **kwargs)

    # Querying
    @classmethod
    def statements_split(cls, statements):
        """
        This classmethod converts a single string containing one or more SQL
        statements into an iterator of strings, each corresponding to one SQL
        statement. If the statement's language is not to be SQL, this method
        should be overloaded appropriately.

        Parameters:
            statements (str): A string containing one or more SQL statements.

        Returns:
            iterator<str>: An iterator of SQL statements.
        """
        for statement in sqlparse.split(statements):
            statement = statement.strip()
            if statement.endswith(';'):
                statement = statement[:-1].strip()
            if statement:  # remove empty statements
                yield statement

    @classmethod
    def statement_cleanup(cls, statement):
        """
        This classmethod takes an SQL statement and reformats it by consistently
        removing comments and replacing all whitespace. It is used by the
        `query` method to avoid functionally identical queries hitting different
        cache kets. If the statement's language is not to be SQL, this method
        should be overloaded appropriately.

        Parameters:
            statement (str): The statement to be reformatted/cleaned-up.

        Returns:
            str: The new statement, consistently reformatted.
        """
        statement = sqlparse.format(statement, strip_comments=True, reindent=True)
        statement = os.linesep.join([line for line in statement.splitlines() if line])
        return statement

    @classmethod
    def statement_hash(cls, statement, cleanup=True):
        """
        This classmethod is used to determine the hash used to identify query
        statements to the cache (if configured).

        Parameters:
            statement (str): A string representation of the statement to be
                hashed.
            cleanup (bool): Whether the statement should first be consistently
                reformatted using `statement_cleanup`.

        Returns:
            str: The hash used to identify a statement to the cache.
        """
        if cleanup:
            statement = cls.statement_cleanup(statement)
        if sys.version_info.major == 3 or sys.version_info.major == 2 and isinstance(statement, unicode):
            statement = statement.encode('utf8')
        return hashlib.sha256(statement).hexdigest()

    @render_statement
    def execute(self, statement, cleanup=True, async=False, cursor=None, **kwargs):
        """
        This method executes a given statement against the relevant database,
        returning the results as a standard DBAPI2 compatible cursor. Where
        supported by database implementations, this cursor can the be used
        in future executions, by passing it as the `cursor` keyword argument.

        Parameters:
            statement (str): The statement to be executed by the query client
                (possibly templated).
            cleanup (bool): Whether statement should be cleaned up before
                computing the hash used to cache results.
            async (bool): Whether the cursor should be returned before the
                server-side query computation is complete and the relevant
                results downloaded.
            cursor (DBAPI2 cursor):  Rather than creating a new cursor, execute
                the statement against the provided cursor.
            **kwargs (dict): Extra keyword arguments to be passed on to
                `_execute`, as implemented by subclasses.
            template (bool): Whether the statement should be treated as a Jinja2
                template. [Used by `render_template` decorator.]
            context (dict): The context in which the template should be
                evaluated (a dictionary of parameters to values). [Used by
                `render_template` decorator.]

        Returns:
            DBAPI2 cursor: A DBAPI2 compatible cursor instance.
        """

        self.connect()

        statements = self.statements_split(statement)
        statements = [self.statement_cleanup(stmt) if cleanup else stmt for stmt in statements]
        assert len(statements) > 0, "No non-empty statements were provided."

        for statement in statements[:-1]:
            cursor = self.connect()._execute(statement, cursor=cursor, async=False, **kwargs)
        cursor = self.connect()._execute(statements[-1], cursor=cursor, async=async, **kwargs)

        return cursor

    @logging_scope("Query", timed=True)
    @render_statement
    @cached_method(
        id_str=lambda self, kwargs: "{}:\n{}".format(kwargs['format'], self.statement_hash(kwargs['statement'], cleanup=kwargs.get('cleanup', True))),
        format=lambda self, kwargs: kwargs['format'] if kwargs['format'] is not None else self.DEFAULT_CURSOR_FORMATTER,
        serializer=cache_serializer,
        deserializer=cache_deserializer
    )
    def query(self, statement, format=None, format_opts={}, **kwargs):
        """
        This method executes a statement against the database using
        `DatabaseClient.execute()`, and then collects the results before
        returning them formatted as nominated; optionally (and by default)
        caching the result if a cache is configured.

        Parameters:
            statement (str): The statement to be executed by the query client
                (possibly templated).
            format (str): A subclass of CursorFormatter, or one of: 'pandas',
                'hive', 'csv', 'tuple' or 'dict'. Defaults to
                `self.DEFAULT_CURSOR_FORMATTER`.
            format_opts (dict): A dictionary of format-specific options.
            **kwargs (dict): Additional arguments to pass on to
                `DatabaseClient.execute()`.
            use_cache (bool): True (default) or False. Whether to use the cache
                (if present). [Used by `cached_method` decorator.]
            renew (bool): True or False (default). If cache is being used, renew
                it before returning stored value. [Used by `cached_method`
                decorator.]

        Returns:
            The results of the query formatted as nominated.
        """
        cursor = self.execute(statement, async=False, template=False, **kwargs)

        # Some DBAPI2 cursor implementations error if attempting to extract
        # data from an empty cursor, and if so, we simply return None.
        if self._cursor_empty(cursor):
            return None

        formatter = self._get_formatter(format, cursor, **format_opts)
        return formatter.dump()

    def stream(self, statement, format=None, format_opts={}, batch=None, **kwargs):
        """
        This method executes a statement against the database, and streams
        results from the resulting cursor object as an iterator over objects
        of the nominated format. If `batch` is not `None`, then the iterator
        will be over lists of size `batch`.

        Parameters:
            statement (str): The statement to be executed against the database.
            format (str): A subclass of CursorFormatter, or one of: 'pandas',
                'hive', 'csv', 'tuple' or 'dict'. Defaults to
                `self.DEFAULT_CURSOR_FORMATTER`.
            format_opts (dict): A dictionary of format-specific options.
            batch (int): If not `None`, the number of rows from the resulting
                cursor to be returned at once.
            **kwargs (dict): Additional keyword arguments to pass onto
                `DatabaseClient.execute`.

        Returns:
            iterator: An iterator over objects of the nominated format or, if
                batched, a list of such objects.
        """
        cursor = self.execute(statement, async=False, **kwargs)
        formatter = self._get_formatter(format, cursor, **format_opts)

        for row in formatter.stream(batch=batch):
            yield row

    def _get_formatter(self, formatter, cursor, **kwargs):
        formatter = formatter or self.DEFAULT_CURSOR_FORMATTER
        if not (inspect.isclass(formatter) and issubclass(formatter, cursor_formatters.CursorFormatter)):
            assert formatter in self.CURSOR_FORMATTERS, "Invalid format '{}'. Choose from: {}".format(formatter, ','.join(self.CURSOR_FORMATTERS.keys()))
            formatter = self.CURSOR_FORMATTERS[formatter]
        return formatter(cursor, **kwargs)

    def stream_to_file(self, statement, file, format='csv', **kwargs):
        """
        This method is a wrapper around `DatabaseClient.stream` that enables the
        iterative writing of cursor results to a file. This is especially useful
        when there are a very large number of results, and loading them all into
        memory would require considerable resources. Note that 'csv' is always
        the default format for this method.

        Parameters:
            statement (str): The statement to be executed against the database.
            file (str, file-like-object): The filename where the data should be
                written, or an open file-like resource.
            format (str): The format to be used ('csv' by default). Format
                options can be passed via `**kwargs`.
            **kwargs: Additional keyword arguments to pass onto
                `DatabaseClient.stream`.
        """
        close_later = False
        if isinstance(file, str):
            file = open(file, 'w')
            close_later = True

        try:
            file.writelines(self.stream(statement, format=format, **kwargs))
        finally:
            if close_later:
                file.close()

    def execute_from_file(self, file, **kwargs):
        """
        This method reads the contents of a file and executes them as a string
        statement against against the database.

        Parameters:
            file (str): The path of the file containing the query statement to
                be executed against the database.
            **kwargs (dict): Extra keyword arguments to pass on to
                `DatabaseClient.execute`.

        Returns:
            DBAPI2 cursor: A DBAPI2 compatible cursor instance.
        """
        with open(file, 'r') as f:
            return self.execute(f.read(), **kwargs)

    def query_from_file(self, file, **kwargs):
        """
        This method reads the contents of a file and executes them as a string
        statement against against the database, returning the results of the
        query formatted as nominated (see `DatabaseClient.query` for more
        details).

        Parameters:
            file (str): The path of the file containing the query statement to
                be executed against the database.
            **kwargs (dict): Extra keyword arguments to pass on to
                `DatabaseClient.query`.

        Returns:
            The results of the query formatted as nominated.
        """
        with open(file, 'r') as f:
            return self.query(f.read(), **kwargs)

    def add_template(self, name, body):
        "TODO: Templating"
        self._templates[name] = body
        return self

    def render_template(self, name_or_statement, context=None, by_name=False):
        "TODO: Templating"
        if by_name:
            if name_or_statement not in self._templates:
                raise ValueError("No such template of name: '{}'.".format(name_or_statement))
            statement = self._templates[name_or_statement]
        else:
            statement = name_or_statement

        try:
            from sqlalchemy.sql.base import Executable
            if isinstance(statement, Executable):
                statement = str(statement.compile(compile_kwargs={"literal_binds": True}))
        except ImportError:
            pass

        if context is None or context is False:
            context = {}

        template_context = {}
        template_context.update(self._template_context)  # default context
        template_context.update(context)  # context passed in
        intersection = set(self._template_context.keys()) & set(context.keys())
        if intersection:
            logger.warning(
                "The following default template context keys have been overridden "
                "by the local context: {}."
                .format(intersection)
            )

        # Substitute in any other named statements recursively
        while '{{{' in statement or '{{%' in statement:
            statement = Template(statement,
                                 block_start_string='{{%',
                                 block_end_string='%}}',
                                 variable_start_string='{{{',
                                 variable_end_string='}}}',
                                 comment_start_string='{{#',
                                 comment_end_string='#}}',
                                 undefined=StrictUndefined).render(getattr(self, '_templates', {}))

        return Template(statement, undefined=StrictUndefined).render(template_context)

    def execute_from_template(self, name, context=None, **kwargs):
        "TODO: Templating"
        statement = self.render_template(name, context, by_name=True)
        return self.execute(statement, **kwargs)

    def query_from_template(self, name, context=None, **kwargs):
        "TODO: Templating"
        statement = self.render_template(name, context, by_name=True)
        return self.query(statement, **kwargs)

    # Uploading data to data store
    @logging_scope('Push', timed=True)
    def push(self, df, table, if_exists='fail', **kwargs):
        """
        Todo:
            Review the naming of this method.

        This method uploads a local dataframe to the database, creating,
        overwriting or appending to the nominated table.

        Parameters:
            df (pandas.DataFrame): The dataframe to upload into the database.
            table (str): The name of the table into which the dataframe should
                be uploaded.
            if_exists (str): if nominated table already exists: 'fail' to do
                nothing, 'replace' to drop, recreate and insert data into new
                table, and 'append' to add data from this table into the
                existing table.
            **kwargs (dict): Additional keyword arguments to pass onto
                `QueryClient._push`.
        """
        assert if_exists in {'fail', 'replace', 'append'}
        self.connect()._push(df, table, if_exists=if_exists, **kwargs)

    # Table properties

    @abstractmethod
    def _execute(self, statement, cursor=None, async=False, **kwargs):
        pass

    def _push(self, df, table, if_exists='fail', **kwargs):
        if self._sqlalchemy_engine is None:
            raise NotImplementedError("Support for pushing data tables using `{}` is not currently implemented.".format(self.__class__.__name__))
        return df.to_sql(name=table, con=self._sqlalchemy_engine, index=False, if_exists=if_exists, **kwargs)

    def _cursor_empty(self, cursor):
        return False

    def table_list(self, **kwargs):
        """
        Return a list of table names in the data source as a DataFrame. Additional kwargs are
        passed to `QueryClient._table_list`.
        """
        return self._table_list(**kwargs)

    @abstractmethod
    def _table_list(self, **kwargs):
        pass

    def table_exists(self, table, **kwargs):
        """
        Return boolean if table exists in schema
        """
        return self._table_exists(table=table, **kwargs)

    @abstractmethod
    def _table_exists(self, table, **kwargs):
        pass

    def table_desc(self, table, **kwargs):
        """
        Describe a table in the data source. Additional kwargs are
        passed to `QueryClient._table_desc`.

        Returns a pandas dataframe of table fields and descriptors.
        """
        return self._table_desc(table, **kwargs)

    @abstractmethod
    def _table_desc(self, table, **kwargs):
        pass

    def table_head(self, table, n=10, **kwargs):
        """
        Show a sample of the data in `table` of the data source. `n` is the number of records
        to show in this sample. The additional `kwargs` are passed on to `QueryClient._table_head`.

        Returns a pandas DataFrame.
        """
        return self._table_head(table, n=n, **kwargs)

    @abstractmethod
    def _table_head(self, table, n=10, **kwargs):
        pass

    def table_props(self, table, **kwargs):
        """
        Return a dataframe of table properties for `table`.
        """
        return self._table_props(table, **kwargs)

    @abstractmethod
    def _table_props(self, table, **kwargs):
        pass

    def _register_magics(self, base_name):
        from IPython.core.magic import register_line_magic, register_cell_magic, register_line_cell_magic

        def statement_executor_magic(executor, statement, variable=None, show='head', transpose=False, template=True, context=None, **kwargs):

            ip = get_ipython()

            if context is None:
                context = ip.user_ns

            # Line magic
            if statement is None:
                return self.query_from_template(variable, context=context, **kwargs)

            # Cell magic
            result = getattr(self, executor)(statement, template=template, context=context, **kwargs)

            if variable is not None:
                ip.user_ns[variable] = result

            if executor != 'query':
                if variable is None:
                    return result
                return
            elif variable is None:
                return result

            format = kwargs.get('format', self.DEFAULT_CURSOR_FORMATTER)
            if show == 'head':
                show = 10
            if isinstance(show, int):
                r = result.head(show) if format == 'pandas' else result[:show]
            elif show == 'all':
                r = result
            elif show == 'none':
                return None
            else:
                raise ValueError("Omniduct does not recognise the argument show='{0}' in cell magic.".format(show))

            if format == 'pandas' and transpose:
                return r.T
            return r

        @register_line_cell_magic(base_name)
        @process_line_cell_arguments
        def query_magic(*args, **kwargs):
            return statement_executor_magic('query', *args, **kwargs)

        @register_line_cell_magic("{}.{}".format(base_name, 'execute'))
        @process_line_cell_arguments
        def query_magic(*args, **kwargs):
            return statement_executor_magic('execute', *args, **kwargs)

        @register_line_cell_magic("{}.{}".format(base_name, 'stream'))
        @process_line_cell_arguments
        def query_magic(*args, **kwargs):
            return statement_executor_magic('stream', *args, **kwargs)

        @register_cell_magic("{}.{}".format(base_name, 'template'))
        @process_line_arguments
        def add_template(body, name):
            self.add_template(name, body)

        @register_line_cell_magic("{}.{}".format(base_name, 'render'))
        @process_line_cell_arguments
        def render_template_magic(body=None, name=None, context=None, show=True):

            ip = get_ipython()

            if body is None:
                assert name is not None, "Name must be specified in line-mode."
                rendered = self.render_template(name, context=context or ip.user_ns, by_name=True)
            else:
                rendered = self.render_template(body, context=context or ip.user_ns, by_name=False)
                if name is not None:
                    ip.user_ns[name] = rendered

            if show:
                print(rendered)
            else:
                return rendered

        @register_line_magic("{}.{}".format(base_name, 'desc'))
        @process_line_arguments
        def table_desc(table_name, **kwargs):
            return self.table_desc(table_name, **kwargs)

        @register_line_magic("{}.{}".format(base_name, 'head'))
        @process_line_arguments
        def table_head(table_name, **kwargs):
            return self.table_head(table_name, **kwargs)

        @register_line_magic("{}.{}".format(base_name, 'props'))
        @process_line_arguments
        def table_props(table_name, **kwargs):
            return self.table_props(table_name, **kwargs)
