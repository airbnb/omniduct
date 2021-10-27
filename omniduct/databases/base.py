from __future__ import absolute_import, print_function

import hashlib
import inspect
import itertools
import logging
import os
import sys
from abc import abstractmethod

import jinja2
import jinja2.meta
import sqlparse
from decorator import decorator
from interface_meta import quirk_docs, override

from omniduct.caches.base import cached_method
from omniduct.duct import Duct
from omniduct.filesystems.local import LocalFsClient
from omniduct.utils.debug import logger, logging_scope
from omniduct.utils.decorators import require_connection
from omniduct.utils.magics import (MagicsProvider, process_line_arguments,
                                   process_line_cell_arguments)

from . import _cursor_formatters
from ._cursor_serializer import CursorSerializer
from ._namespaces import ParsedNamespaces

logging.getLogger('requests').setLevel(logging.WARNING)


@decorator
def render_statement(method, self, statement, *args, **kwargs):
    """
    Pre-render a statement template prior to wrapped function execution.

    This decorator expects to act as wrapper on functions which
    takes statements as the second argument.
    """
    if kwargs.pop('template', True):
        statement = self.template_render(
            statement,
            context=kwargs.pop('context', {}),
            by_name=False,
        )
    return method(self, statement, *args, **kwargs)


class DatabaseClient(Duct, MagicsProvider):
    """
    An abstract class providing the common API for all database clients.

    Note: `DatabaseClient` subclasses are callable, so that one can use
    `DatabaseClient(...)` as a short-hand for `DatabaseClient.query(...)`.

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
        'pandas': _cursor_formatters.PandasCursorFormatter,
        'hive': _cursor_formatters.HiveCursorFormatter,
        'csv': _cursor_formatters.CsvCursorFormatter,
        'tuple': _cursor_formatters.TupleCursorFormatter,
        'dict': _cursor_formatters.DictCursorFormatter,
        'raw': _cursor_formatters.RawCursorFormatter,
    }
    DEFAULT_CURSOR_FORMATTER = 'pandas'
    SUPPORTS_SESSION_PROPERTIES = False
    NAMESPACE_NAMES = ['database', 'table']
    NAMESPACE_QUOTECHAR = '"'
    NAMESPACE_SEPARATOR = '.'

    NAMESPACE_DEFAULT = None  # DEPRECATED (use NAMESPACE_DEFAULTS_READ instead): Will be removed in Omniduct 2.0.0

    @property
    def NAMESPACE_DEFAULTS_READ(self):
        """
        Backwards compatible shim for `NAMESPACE_DEFAULTS`.
        """
        return self.NAMESPACE_DEFAULT

    @property
    def NAMESPACE_DEFAULTS_WRITE(self):
        """
        Unless overridden, this is the same as `NAMESPACE_DEFAULTS_READ`.
        """
        return self.NAMESPACE_DEFAULTS_READ

    @quirk_docs('_init', mro=True)
    def __init__(
        self, session_properties=None, templates=None, template_context=None, default_format_opts=None,
        **kwargs
    ):
        """
        session_properties (dict): A mapping of default session properties
            to values. Interpretation is left up to implementations.
        templates (dict): A dictionary of name to template mappings. Additional
            templates can be added using `.template_add`.
        template_context (dict): The default template context to use when
            rendering templates.
        default_format_opts (dict): The default formatting options passed to
            cursor formatter.
        """
        Duct.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)

        self.session_properties = session_properties or {}
        self._templates = templates or {}
        self._template_context = template_context or {}
        self._sqlalchemy_engine = None
        self._sqlalchemy_metadata = None
        self._default_format_opts = default_format_opts or {}

        self._init(**kwargs)

    @abstractmethod
    def _init(self):
        pass

    # Session property management and configuration
    @property
    def session_properties(self):
        """dict: The default session properties used in statement executions."""
        return self._session_properties

    @session_properties.setter
    def session_properties(self, properties):
        self._session_properties = self._get_session_properties(default=properties)

    def _get_session_properties(self, overrides=None, default=None):
        """
        Retrieve the default session properties with optional overrides.

        Properties with a value of None will be skipped, in order to allow
        overrides to remove default properties.

        Args:
            overrides (dict, None): A dictionary of session property overrides.
            default (dict, None): A dictionary of default session properties, if
                it is necessary to override `self.session_properties`.

        Returns:
            dict: A dictionary of session properties.
        """
        if (default or overrides) and not self.SUPPORTS_SESSION_PROPERTIES:
            raise RuntimeError("Session properties are not supported by this backend.")

        props = (default if default is not None else self.session_properties).copy()
        props.update(overrides or {})

        # Remove any properties with value set to None.
        for key, value in props.items():
            if value is None:
                del props[key]

        return props

    def __call__(self, query, **kwargs):
        return self.query(query, **kwargs)

    # Querying
    def _statement_prepare(self, statement, session_properties, **kwargs):
        """
        Prepare a statement for execution.

        This is a hook that can be used by subclasses to transform and/or
        validate statements before execution in `_execute`; for example
        inserting session properties in query headers.

        Args:
            statement (str): The statement to be executed.
            session_properties (dict): A mutable dictionary of session properties
                and their values (this method can mutate it depending on statement
                contents).
            **kwargs (dict): Any additional keyword arguments passed through to
                `self.execute` (will match the extra keyword arguments added to
                `self._execute`).

        Returns:
            statement (str): The statement to be executed (potentially transformed).
        """
        return statement

    def _statement_split(self, statements):
        """
        Split a statement into separate SQL statements.

        This method converts a single string containing one or more SQL
        statements into an iterator of strings, each corresponding to one SQL
        statement. If the statement's language is not to be SQL, this method
        should be overloaded appropriately.

        Args:
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
    def statement_hash(cls, statement, cleanup=True):
        """
        Retrieve the hash to use to identify query statements to the cache.

        Args:
            statement (str): A string representation of the statement to be
                hashed.
            cleanup (bool): Whether the statement should first be consistently
                reformatted using `statement_cleanup`.

        Returns:
            str: The hash used to identify a statement to the cache.
        """
        if cleanup:
            statement = cls.statement_cleanup(statement)
        if (
            sys.version_info.major == 3
            or sys.version_info.major == 2 and isinstance(statement, unicode)  # noqa: F821
        ):
            statement = statement.encode('utf8')
        return hashlib.sha256(statement).hexdigest()

    @classmethod
    def statement_cleanup(cls, statement):
        """
        Clean up statements prior to hash computation.

        This classmethod takes an SQL statement and reformats it by consistently
        removing comments and replacing all whitespace. It is used by the
        `statement_hash` method to avoid functionally identical queries hitting
        different cache keys. If the statement's language is not to be SQL, this
        method should be overloaded appropriately.

        Args:
            statement (str): The statement to be reformatted/cleaned-up.

        Returns:
            str: The new statement, consistently reformatted.
        """
        statement = sqlparse.format(statement, strip_comments=True, reindent=True)
        statement = os.linesep.join([line for line in statement.splitlines() if line])
        return statement

    @render_statement
    @cached_method(
        key=lambda self, kwargs: self.statement_hash(
            statement=kwargs['statement'],
            cleanup=kwargs.pop('cleanup', True)
        ),
        serializer=lambda self, kwargs: CursorSerializer(),
        use_cache=lambda self, kwargs: kwargs.pop('use_cache', False),
        metadata=lambda self, kwargs: {
            'statement': kwargs['statement'],
            'session_properties': kwargs['session_properties']
        }
    )
    @quirk_docs('_execute')
    @require_connection
    def execute(self, statement, wait=True, cursor=None, session_properties=None, **kwargs):
        """
        Execute a statement against this database and return a cursor object.

        Where supported by database implementations, this cursor can the be used
        in future executions, by passing it as the `cursor` keyword argument.

        Args:
            statement (str): The statement to be executed by the query client
                (possibly templated).
            wait (bool): Whether the cursor should be returned before the
                server-side query computation is complete and the relevant
                results downloaded.
            cursor (DBAPI2 cursor):  Rather than creating a new cursor, execute
                the statement against the provided cursor.
            session_properties (dict): Additional session properties and/or
                overrides to use for this query. Setting a session property
                value to `None` will cause it to be omitted.
            **kwargs (dict): Extra keyword arguments to be passed on to
                `_execute`, as implemented by subclasses.
            template (bool): Whether the statement should be treated as a Jinja2
                template. [Used by `render_statement` decorator.]
            context (dict): The context in which the template should be
                evaluated (a dictionary of parameters to values). [Used by
                `render_statement` decorator.]
            use_cache (bool): True or False (default). Whether to use the cache
                (if present). [Used by `cached_method` decorator.]
            renew (bool): True or False (default). If cache is being used, renew
                it before returning stored value. [Used by `cached_method`
                decorator.]
            cleanup (bool): Whether statement should be cleaned up before
                computing the hash used to cache results. [Used by `cached_method`
                decorator.]

        Returns:
            DBAPI2 cursor: A DBAPI2 compatible cursor instance.
        """

        session_properties = self._get_session_properties(overrides=session_properties)

        statements = list(self._statement_split(
            self._statement_prepare(statement, session_properties=session_properties, **kwargs)
        ))
        assert len(statements) > 0, "No non-empty statements were provided."

        for statement in statements[:-1]:
            cursor = self._execute(statement, cursor=cursor, wait=True, session_properties=session_properties, **kwargs)
        cursor = self._execute(statements[-1], cursor=cursor, wait=wait, session_properties=session_properties, **kwargs)

        return cursor

    @logging_scope("Query", timed=True)
    @render_statement
    def query(self, statement, format=None, format_opts={}, use_cache=True, **kwargs):
        """
        Execute a statement against this database and collect formatted data.

        Args:
            statement (str): The statement to be executed by the query client
                (possibly templated).
            format (str): A subclass of CursorFormatter, or one of: 'pandas',
                'hive', 'csv', 'tuple' or 'dict'. Defaults to
                `self.DEFAULT_CURSOR_FORMATTER`.
            format_opts (dict): A dictionary of format-specific options.
            use_cache (bool): Whether to cache the cursor returned by
                `DatabaseClient.execute()` (overrides the default of False
                for `.execute()`). (default=True)
            **kwargs (dict): Additional arguments to pass on to
                `DatabaseClient.execute()`.

        Returns:
            The results of the query formatted as nominated.
        """
        cursor = self.execute(statement, wait=True, template=False, use_cache=use_cache, **kwargs)

        # Some DBAPI2 cursor implementations error if attempting to extract
        # data from an empty cursor, and if so, we simply return None.
        if self._cursor_empty(cursor):
            return None

        formatter = self._get_formatter(format, cursor, **format_opts)
        return formatter.dump()

    def stream(self, statement, format=None, format_opts={}, batch=None, **kwargs):
        """
        Execute a statement against this database and stream formatted results.

        This method returns a generator over objects representing rows in the
        result set. If `batch` is not `None`, then the iterator
        will be over lists of length `batch` containing formatted rows.

        Args:
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
        cursor = self.execute(statement, wait=True, **kwargs)
        formatter = self._get_formatter(format, cursor, **format_opts)

        for row in formatter.stream(batch=batch):
            yield row

    def _get_formatter(self, formatter, cursor, **kwargs):
        formatter = formatter or self.DEFAULT_CURSOR_FORMATTER
        if not (inspect.isclass(formatter) and issubclass(formatter, _cursor_formatters.CursorFormatter)):
            assert formatter in self.CURSOR_FORMATTERS, "Invalid format '{}'. Choose from: {}".format(formatter, ','.join(self.CURSOR_FORMATTERS.keys()))
            formatter = self.CURSOR_FORMATTERS[formatter]
        format_opts = dict(itertools.chain(self._default_format_opts.items(), kwargs.items()))
        return formatter(cursor, **format_opts)

    def stream_to_file(self, statement, file, format='csv', fs=None, **kwargs):
        """
        Execute a statement against this database and stream results to a file.

        This method is a wrapper around `DatabaseClient.stream` that enables the
        iterative writing of cursor results to a file. This is especially useful
        when there are a very large number of results, and loading them all into
        memory would require considerable resources. Note that 'csv' is the
        default format for this method (rather than `pandas`).

        Args:
            statement (str): The statement to be executed against the database.
            file (str, file-like-object): The filename where the data should be
                written, or an open file-like resource.
            format (str): The format to be used ('csv' by default). Format
                options can be passed via `**kwargs`.
            fs (None, FileSystemClient): The filesystem wihin which the
                nominated file should be found. If `None`, the local filesystem
                will be used.
            **kwargs: Additional keyword arguments to pass onto
                `DatabaseClient.stream`.
        """
        close_later = False
        if isinstance(file, str):
            file = (fs or LocalFsClient()).open(file, 'w')
            close_later = True

        try:
            file.writelines(self.stream(statement, format=format, **kwargs))
        finally:
            if close_later:
                file.close()

    def execute_from_file(self, file, fs=None, **kwargs):
        """
        Execute a statement stored in a file.

        Args:
            file (str, file-like-object): The path of the file containing the
                query statement to be executed against the database, or an open
                file-like resource.
            fs (None, FileSystemClient): The filesystem wihin which the
                nominated file should be found. If `None`, the local filesystem
                will be used.
            **kwargs (dict): Extra keyword arguments to pass on to
                `DatabaseClient.execute`.

        Returns:
            DBAPI2 cursor: A DBAPI2 compatible cursor instance.
        """
        close_later = False
        if isinstance(file, str):
            file = (fs or LocalFsClient()).open(file, 'r')
            close_later = True

        try:
            return self.execute(file.read(), **kwargs)
        finally:
            if close_later:
                file.close()

    def query_from_file(self, file, fs=None, **kwargs):
        """
        Query using a statement stored in a file.

        Args:
            file (str, file-like-object): The path of the file containing the
                query statement to be executed against the database, or an open
                file-like resource.
            fs (None, FileSystemClient): The filesystem wihin which the
                nominated file should be found. If `None`, the local filesystem
                will be used.
            **kwargs (dict): Extra keyword arguments to pass on to
                `DatabaseClient.query`.

        Returns:
            object: The results of the query formatted as nominated.
        """
        close_later = False
        if isinstance(file, str):
            file = (fs or LocalFsClient()).open(file, 'r')
            close_later = True

        try:
            return self.query(file.read(), **kwargs)
        finally:
            if close_later:
                file.close()

    @property
    def template_names(self):
        """
        list: A list of names associated with the templates associated with this
        client.
        """
        return list(self._templates)

    def template_add(self, name, body):
        """
        Add a named template to the internal dictionary of templates.

        Note: Templates are interpreted as `jinja2` templates. See
        `.template_render` for more information.

        Args:
            name (str): The name of the template.
            body (str): The (typically) multiline body of the template.

        Returns:
            PrestoClient: A reference to this object.
        """
        self._templates[name] = body
        return self

    def template_get(self, name):
        """
        Retrieve a named template.

        Args:
            name (str): The name of the template to retrieve.

        Raises:
            ValueError: If `name` is not associated with a template.

        Returns:
            str: The requested template.
        """
        if name not in self._templates:
            raise ValueError("No such template named: '{}'.".format(name))
        return self._templates[name]

    def template_variables(self, name_or_statement, by_name=False):
        """
        Return the set of undeclared variables required for this template.

        Args:
            name_or_statement (str): The name of a template (if `by_name` is True)
                or else a string representation of a `jinja2` template.
            by_name (bool): `True` if `name_or_statement` should be interpreted as a
                template name, or `False` (default) if `name_or_statement` should be
                interpreted as a template body.

        Returns:
            set<str>: A set of names which the template requires to be rendered.
        """
        ast = jinja2.Environment().parse(
            self.template_render(name_or_statement, by_name=by_name, meta_only=True)
        )
        return jinja2.meta.find_undeclared_variables(ast)

    def template_render(self, name_or_statement, context=None, by_name=False,
                        cleanup=False, meta_only=False):
        """
        Render a template by name or value.

        In addition to the `jinja2` templating syntax, described in more detail
        in the official `jinja2` documentation, a meta-templating extension is
        also provided. This meta-templating allows you to reference other
        reference other templates. For example, if you had two SQL templates
        named 'template_a' and 'template_b', then you could render them into one
        SQL query using (for example):

        .. code-block:: python

            .template_render('''
            WITH
                a AS (
                    {{{template_a}}}
                ),
                b AS (
                    {{{template_b}}}
                )
            SELECT *
            FROM a
            JOIN b ON a.x = b.x
            ''')

        Note that template substitution in this way is iterative, so you can
        chain template embedding, provided that such embedding is not recursive.

        Args:
            name_or_statement (str): The name of a template (if `by_name` is True)
                or else a string representation of a `jinja2` template.
            context (dict, None): A dictionary to use as the template context.
                If not specified, an empty dictionary is used.
            by_name (bool): `True` if `name_or_statement` should be interpreted as a
                template name, or `False` (default) if `name_or_statement` should be
                interpreted as a template body.
            cleanup (bool): `True` if the rendered statement should be formatted,
                `False` (default) otherwise
            meta_only (bool): `True` if rendering should only progress as far as
                rendering nested templates (i.e. don't actually substitute in
                variables from the context); `False` (default) otherwise.

        Returns:
            str: The rendered template.
        """
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
            statement = (
                jinja2.Template(
                    statement,
                    block_start_string='{{%',
                    block_end_string='%}}',
                    variable_start_string='{{{',
                    variable_end_string='}}}',
                    comment_start_string='{{#',
                    comment_end_string='#}}',
                    undefined=jinja2.StrictUndefined
                )
                .render(getattr(self, '_templates', {}))
            )

        if not meta_only:
            statement = (
                jinja2.Template(statement, undefined=jinja2.StrictUndefined)
                .render(template_context)
            )

        if cleanup:
            statement = self.statement_cleanup(statement)

        return statement

    def execute_from_template(self, name, context=None, **kwargs):
        """
        Render and then execute a named template.

        Args:
            name (str): The name of the template to be rendered and executed.
            context (dict): The context in which the template should be rendered.
            **kwargs (dict): Additional parameters to pass to `.execute()`.

        Returns:
            DBAPI2 cursor: A DBAPI2 compatible cursor instance.
        """
        statement = self.template_render(name, context, by_name=True)
        return self.execute(statement, **kwargs)

    def query_from_template(self, name, context=None, **kwargs):
        """
        Render and then query using a named tempalte.

        Args:
            name (str): The name of the template to be rendered and used to query
                the database.
            context (dict): The context in which the template should be rendered.
            **kwargs (dict): Additional parameters to pass to `.query()`.

        Returns:
           object: The results of the query formatted as nominated.
        """
        statement = self.template_render(name, context, by_name=True)
        return self.query(statement, **kwargs)

    # Uploading/querying data into data store
    @logging_scope('Query [CTAS]', timed=True)
    @quirk_docs('_query_to_table')
    def query_to_table(self, statement, table, if_exists='fail', **kwargs):
        """
        Run a query and store the results in a table in this database.

        Args:
            statement: The statement to be executed.
            table (str): The name of the table into which the dataframe should
                be uploaded.
            if_exists (str): if nominated table already exists: 'fail' to do
                nothing, 'replace' to drop, recreate and insert data into new
                table, and 'append' to add data from this table into the
                existing table.
            **kwargs (dict): Additional keyword arguments to pass onto
                `DatabaseClient._query_to_table`.

        Returns:
            DB-API cursor: The cursor object associated with the execution.
        """
        assert if_exists in {'fail', 'replace', 'append'}
        table = self._parse_namespaces(table, write=True)
        return self._query_to_table(statement, table, if_exists=if_exists, **kwargs)

    @logging_scope('Dataframe Upload', timed=True)
    @quirk_docs('_dataframe_to_table')
    @require_connection
    def dataframe_to_table(self, df, table, if_exists='fail', **kwargs):
        """
        Upload a local pandas dataframe into a table in this database.

        Args:
            df (pandas.DataFrame): The dataframe to upload into the database.
            table (str, ParsedNamespaces): The name of the table into which the
                dataframe should be uploaded.
            if_exists (str): if nominated table already exists: 'fail' to do
                nothing, 'replace' to drop, recreate and insert data into new
                table, and 'append' to add data from this table into the
                existing table.
            **kwargs (dict): Additional keyword arguments to pass onto
                `DatabaseClient._dataframe_to_table`.
        """
        assert if_exists in {'fail', 'replace', 'append'}
        self._dataframe_to_table(df, self._parse_namespaces(table, write=True), if_exists=if_exists, **kwargs)

    # Table properties

    @abstractmethod
    def _execute(self, statement, cursor, wait, session_properties):
        pass

    def _query_to_table(self, statement, table, if_exists, **kwargs):
        raise NotImplementedError

    def _dataframe_to_table(self, df, table, if_exists='fail', **kwargs):
        raise NotImplementedError

    def _cursor_empty(self, cursor):
        return cursor is None

    def _parse_namespaces(self, name, level=0, defaults=None, write=False):
        return ParsedNamespaces.from_name(
            name,
            self.NAMESPACE_NAMES[:-level] if level > 0 else self.NAMESPACE_NAMES,
            quote_char=self.NAMESPACE_QUOTECHAR,
            separator=self.NAMESPACE_SEPARATOR,
            defaults=defaults if defaults else (self.NAMESPACE_DEFAULTS_WRITE if write else self.NAMESPACE_DEFAULTS_READ),
        )

    @quirk_docs('_table_list')
    def table_list(self, namespace=None, renew=True, **kwargs):
        """
        Return a list of table names in the data source as a DataFrame.

        Args:
            namespace (str): The namespace in which to look for tables.
            renew (bool): Whether to renew the table list or use cached results
                (default: True).
            **kwargs (dict): Additional arguments passed through to implementation.

        Returns:
            list<str>: The names of schemas in this database.
        """
        return self._table_list(self._parse_namespaces(namespace, level=1), renew=renew, **kwargs)

    @abstractmethod
    def _table_list(self, namespace, **kwargs):
        pass

    @quirk_docs('_table_exists')
    def table_exists(self, table, renew=True, **kwargs):
        """
        Check whether a table exists.

        Args:
            table (str): The table for which to check.
            renew (bool): Whether to renew the table list or use cached results
                (default: True).
            **kwargs (dict): Additional arguments passed through to implementation.

        Returns:
            bool: `True` if table exists, and `False` otherwise.
        """
        return self._table_exists(table=self._parse_namespaces(table), renew=renew, **kwargs)

    @abstractmethod
    def _table_exists(self, table, **kwargs):
        pass

    @quirk_docs('_table_drop')
    def table_drop(self, table, **kwargs):
        """
        Remove a table from the database.

        Args:
            table (str): The table to drop.
            **kwargs (dict): Additional arguments passed through to implementation.

        Returns:
            DB-API cursor: The cursor associated with this execution.
        """
        return self._table_drop(table=self._parse_namespaces(table, write=True), **kwargs)

    @abstractmethod
    def _table_drop(self, table, **kwargs):
        pass

    @quirk_docs('_table_desc')
    def table_desc(self, table, renew=True, **kwargs):
        """
        Describe a table in the database.

        Args:
            table (str): The table to describe.
            renew (bool): Whether to renew the results (default: True).
            **kwargs (dict): Additional arguments passed through to implementation.

        Returns:
            pandas.DataFrame: A dataframe description of the table.
        """
        return self._table_desc(table=self._parse_namespaces(table), renew=renew, **kwargs)

    @abstractmethod
    def _table_desc(self, table, **kwargs):
        pass

    @quirk_docs('_table_partition_cols')
    def table_partition_cols(self, table, renew=True, **kwargs):
        """
        Extract the columns by which a table is partitioned (if database supports partitions).

        Args:
            table (str): The table from which to extract data.
            renew (bool): Whether to renew the results (default: True).
            **kwargs (dict): Additional arguments passed through to implementation.

        Returns:
            list<str>: A list of columns by which table is partitioned.
        """
        return self._table_partition_cols(table=self._parse_namespaces(table), renew=renew, **kwargs)

    def _table_partition_cols(self, table, **kwargs):
        raise NotImplementedError(
            "Database backend `{}` does not support, or has not implemented, "
            "support for extracting partition columns."
            .format(self.__class__.__name__)
        )

    @quirk_docs('_table_head')
    def table_head(self, table, n=10, renew=True, **kwargs):
        """
        Retrieve the first `n` rows from a table.

        Args:
            table (str): The table from which to extract data.
            n (int): The number of rows to extract.
            renew (bool): Whether to renew the table list or use cached results
                (default: True).
            **kwargs (dict): Additional arguments passed through to implementation.

        Returns:
            pandas.DataFrame: A dataframe representation of the first `n` rows
                of the nominated table.
        """
        return self._table_head(table=self._parse_namespaces(table), n=n, renew=renew, **kwargs)

    @abstractmethod
    def _table_head(self, table, n=10, **kwargs):
        pass

    @quirk_docs('_table_props')
    def table_props(self, table, renew=True, **kwargs):
        """
        Retrieve the properties associated with a table.

        Args:
            table (str): The table from which to extract data.
            renew (bool): Whether to renew the table list or use cached results
                (default: True).
            **kwargs (dict): Additional arguments passed through to implementation.

        Returns:
            pandas.DataFrame: A dataframe representation of the table
                properties.
        """
        return self._table_props(table=self._parse_namespaces(table), renew=renew, **kwargs)

    @abstractmethod
    def _table_props(self, table, **kwargs):
        pass

    @override
    def _register_magics(self, base_name):
        """
        The following magic functions will be registered (assuming that
        the base name is chosen to be 'hive'):
        - Cell Magics:
            - `%%hive`: For querying the database.
            - `%%hive.execute`: For executing a statement against the database.
            - `%%hive.stream`: For executing a statement against the database,
                and streaming the results.
            - `%%hive.template`: The defining a new template.
            - `%%hive.render`: Render a provided query statement.
        - Line Magics:
            - `%hive`: For querying the database using a named template.
            - `%hive.execute`: For executing a named template statement against
                the database.
            - `%hive.stream`: For executing a named template against the database,
                and streaming the results.
            - `%hive.render`: Render a provided a named template.
            - `%hive.desc`: Describe the table nominated.
            - `%hive.head`: Return the first rows in a specified table.
            - `%hive.props`: Show the properties specified for a nominated table.

        Documentation for these magics is provided online.
        """
        from IPython import get_ipython
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
            elif show in (None, 'none'):
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
        def execute_magic(*args, **kwargs):
            return statement_executor_magic('execute', *args, **kwargs)

        @register_line_cell_magic("{}.{}".format(base_name, 'stream'))
        @process_line_cell_arguments
        def stream_magic(*args, **kwargs):
            return statement_executor_magic('stream', *args, **kwargs)

        @register_cell_magic("{}.{}".format(base_name, 'template'))
        @process_line_arguments
        def template_add(body, name):
            self.template_add(name, body)

        @register_line_cell_magic("{}.{}".format(base_name, 'render'))
        @process_line_cell_arguments
        def template_render_magic(body=None, name=None, context=None, show=True,
                                  cleanup=False, meta_only=False):

            ip = get_ipython()

            if body is None:
                assert name is not None, "Name must be specified in line-mode."
                rendered = self.template_render(
                    name, context=context or ip.user_ns, by_name=True,
                    cleanup=cleanup, meta_only=meta_only
                )
            else:
                rendered = self.template_render(
                    body, context=context or ip.user_ns, by_name=False,
                    cleanup=cleanup, meta_only=meta_only
                )
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
