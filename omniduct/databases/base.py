from __future__ import annotations

import hashlib
import inspect
import itertools
import logging
import os
from abc import abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Literal

import jinja2
import jinja2.meta
import sqlparse
from decorator import decorator
from interface_meta import inherit_docs, override
from typing_extensions import Self

from omniduct.caches.base import cached_method
from omniduct.duct import Duct
from omniduct.filesystems.local import LocalFsClient
from omniduct.utils.debug import logger, logging_scope
from omniduct.utils.decorators import require_connection
from omniduct.utils.magics import (
    MagicsProvider,
    process_line_arguments,
    process_line_cell_arguments,
)

from . import _cursor_formatters
from ._cursor_serializer import CursorSerializer
from ._namespaces import ParsedNamespaces

if TYPE_CHECKING:
    import pandas as pd

    from omniduct.filesystems.base import FileSystemClient

logging.getLogger("requests").setLevel(logging.WARNING)


@decorator
def render_statement(
    method: Any, self: Any, statement: Any, *args: Any, **kwargs: Any
) -> Any:
    """
    Pre-render a statement template prior to wrapped function execution.

    This decorator expects to act as wrapper on functions which
    takes statements as the second argument.
    """
    if kwargs.pop("template", True):
        statement = self.template_render(
            statement,
            context=kwargs.pop("context", {}),
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
    DEFAULT_PORT: int | None = None

    CURSOR_FORMATTERS: dict[str, type[_cursor_formatters.CursorFormatter]] = {
        "pandas": _cursor_formatters.PandasCursorFormatter,
        "hive": _cursor_formatters.HiveCursorFormatter,
        "csv": _cursor_formatters.CsvCursorFormatter,
        "tuple": _cursor_formatters.TupleCursorFormatter,
        "dict": _cursor_formatters.DictCursorFormatter,
        "raw": _cursor_formatters.RawCursorFormatter,
    }
    DEFAULT_CURSOR_FORMATTER: str = "pandas"
    SUPPORTS_SESSION_PROPERTIES: bool = False
    NAMESPACE_NAMES: list[str] = ["database", "table"]
    NAMESPACE_QUOTECHAR: str = '"'
    NAMESPACE_SEPARATOR: str = "."

    NAMESPACE_DEFAULT: dict[str, str] | None = (
        None  # DEPRECATED (use NAMESPACE_DEFAULTS_READ instead): Will be removed in Omniduct 2.0.0
    )

    @property
    def NAMESPACE_DEFAULTS_READ(self) -> dict[str, str] | None:
        """
        Backwards compatible shim for `NAMESPACE_DEFAULTS`.
        """
        return self.NAMESPACE_DEFAULT

    @property
    def NAMESPACE_DEFAULTS_WRITE(self) -> dict[str, str] | None:
        """
        Unless overridden, this is the same as `NAMESPACE_DEFAULTS_READ`.
        """
        return self.NAMESPACE_DEFAULTS_READ

    @inherit_docs("_init", mro=True)
    def __init__(
        self,
        session_properties: dict[str, Any] | None = None,
        templates: dict[str, str] | None = None,
        template_context: dict[str, Any] | None = None,
        default_format_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        session_properties: A mapping of default session properties to values.
            Interpretation is left up to implementations.
        templates: A dictionary of name to template mappings. Additional
            templates can be added using `.template_add`.
        template_context: The default template context to use when rendering
            templates.
        default_format_opts: The default formatting options passed to cursor
            formatter.
        """
        Duct.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)

        self.session_properties = session_properties or {}
        self._templates: dict[str, str] = templates or {}
        self._template_context: dict[str, Any] = template_context or {}
        self._sqlalchemy_engine: Any = None
        self._default_format_opts: dict[str, Any] = default_format_opts or {}

        self._init(**kwargs)

    @abstractmethod
    def _init(self) -> None:
        pass

    # Session property management and configuration
    @property
    def session_properties(self) -> dict[str, Any]:
        """dict: The default session properties used in statement executions."""
        return self._session_properties

    @session_properties.setter
    def session_properties(self, properties: dict[str, Any]) -> None:
        self._session_properties = self._get_session_properties(default=properties)

    def _get_session_properties(
        self,
        overrides: dict[str, Any] | None = None,
        default: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Retrieve the default session properties with optional overrides.

        Properties with a value of None will be skipped, in order to allow
        overrides to remove default properties.

        Args:
            overrides: A dictionary of session property overrides.
            default: A dictionary of default session properties, if it is
                necessary to override `self.session_properties`.

        Returns:
            A dictionary of session properties.
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

    def __call__(self, query: str, **kwargs: Any) -> Any:
        return self.query(query, **kwargs)

    # Querying
    def _statement_prepare(
        self,
        statement: str,
        session_properties: dict[str, Any],
        **kwargs: Any,
    ) -> str:
        """
        Prepare a statement for execution.

        This is a hook that can be used by subclasses to transform and/or
        validate statements before execution in `_execute`; for example
        inserting session properties in query headers.

        Args:
            statement: The statement to be executed.
            session_properties: A mutable dictionary of session properties and
                their values (this method can mutate it depending on statement
                contents).
            **kwargs: Any additional keyword arguments passed through to
                `self.execute` (will match the extra keyword arguments added to
                `self._execute`).

        Returns:
            The statement to be executed (potentially transformed).
        """
        return statement

    def _statement_split(self, statements: str) -> Iterator[str]:
        """
        Split a statement into separate SQL statements.

        This method converts a single string containing one or more SQL
        statements into an iterator of strings, each corresponding to one SQL
        statement. If the statement's language is not to be SQL, this method
        should be overloaded appropriately.

        Args:
            statements: A string containing one or more SQL statements.

        Returns:
            An iterator of SQL statements.
        """
        for statement in sqlparse.split(statements):
            statement = statement.strip()
            if statement.endswith(";"):
                statement = statement[:-1].strip()
            if statement:  # remove empty statements
                yield statement

    @classmethod
    def statement_hash(cls, statement: str, cleanup: bool = True) -> str:
        """
        Retrieve the hash to use to identify query statements to the cache.

        Args:
            statement: A string representation of the statement to be hashed.
            cleanup: Whether the statement should first be consistently
                reformatted using `statement_cleanup`.

        Returns:
            The hash used to identify a statement to the cache.
        """
        if cleanup:
            statement = cls.statement_cleanup(statement)
        return hashlib.sha256(statement.encode("utf8")).hexdigest()

    @classmethod
    def statement_cleanup(cls, statement: str) -> str:
        """
        Clean up statements prior to hash computation.

        This classmethod takes an SQL statement and reformats it by consistently
        removing comments and replacing all whitespace. It is used by the
        `statement_hash` method to avoid functionally identical queries hitting
        different cache keys. If the statement's language is not to be SQL, this
        method should be overloaded appropriately.

        Args:
            statement: The statement to be reformatted/cleaned-up.

        Returns:
            The new statement, consistently reformatted.
        """
        statement = sqlparse.format(statement, strip_comments=True, reindent=True)
        statement = os.linesep.join([line for line in statement.splitlines() if line])
        return statement

    @render_statement
    @cached_method(
        key=lambda self, kwargs: self.statement_hash(
            statement=kwargs["statement"], cleanup=kwargs.pop("cleanup", True)
        ),
        serializer=lambda self, kwargs: CursorSerializer(),
        use_cache=lambda self, kwargs: kwargs.pop("use_cache", False),
        metadata=lambda self, kwargs: {
            "statement": kwargs["statement"],
            "session_properties": kwargs["session_properties"],
        },
    )
    @inherit_docs("_execute")
    @require_connection
    def execute(
        self,
        statement: str,
        wait: bool = True,
        cursor: Any = None,
        session_properties: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a statement against this database and return a cursor object.

        Where supported by database implementations, this cursor can the be used
        in future executions, by passing it as the `cursor` keyword argument.

        Args:
            statement: The statement to be executed by the query client
                (possibly templated).
            wait: Whether the cursor should be returned before the server-side
                query computation is complete and the relevant results downloaded.
            cursor: Rather than creating a new cursor, execute the statement
                against the provided cursor.
            session_properties: Additional session properties and/or overrides
                to use for this query. Setting a session property value to
                `None` will cause it to be omitted.
            **kwargs: Extra keyword arguments to be passed on to `_execute`, as
                implemented by subclasses.
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
            A DBAPI2 compatible cursor instance.
        """

        session_properties = self._get_session_properties(overrides=session_properties)

        statements = list(
            self._statement_split(
                self._statement_prepare(
                    statement, session_properties=session_properties, **kwargs
                )
            )
        )
        if len(statements) == 0:
            raise ValueError("No non-empty statements were provided.")

        for stmt in statements[:-1]:
            cursor = self._execute(
                stmt,
                cursor=cursor,
                wait=True,
                session_properties=session_properties,
                **kwargs,
            )
        cursor = self._execute(
            statements[-1],
            cursor=cursor,
            wait=wait,
            session_properties=session_properties,
            **kwargs,
        )

        return cursor

    @logging_scope("Query", timed=True)
    @render_statement
    def query(
        self,
        statement: str,
        format: str | type[_cursor_formatters.CursorFormatter] | None = None,
        format_opts: dict[str, Any] | None = None,
        use_cache: bool = True,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a statement against this database and collect formatted data.

        Args:
            statement: The statement to be executed by the query client
                (possibly templated).
            format: A subclass of CursorFormatter, or one of: 'pandas', 'hive',
                'csv', 'tuple' or 'dict'. Defaults to
                `self.DEFAULT_CURSOR_FORMATTER`.
            format_opts: A dictionary of format-specific options.
            use_cache: Whether to cache the cursor returned by
                `DatabaseClient.execute()` (overrides the default of False for
                `.execute()`). (default=True)
            **kwargs: Additional arguments to pass on to
                `DatabaseClient.execute()`.

        Returns:
            The results of the query formatted as nominated.
        """
        format_opts = format_opts or {}
        cursor = self.execute(
            statement, wait=True, template=False, use_cache=use_cache, **kwargs
        )

        # Some DBAPI2 cursor implementations error if attempting to extract
        # data from an empty cursor, and if so, we simply return None.
        if self._cursor_empty(cursor):
            return None

        formatter = self._get_formatter(format, cursor, **format_opts)
        return formatter.dump()

    def stream(
        self,
        statement: str,
        format: str | type[_cursor_formatters.CursorFormatter] | None = None,
        format_opts: dict[str, Any] | None = None,
        batch: int | None = None,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """
        Execute a statement against this database and stream formatted results.

        This method returns a generator over objects representing rows in the
        result set. If `batch` is not `None`, then the iterator
        will be over lists of length `batch` containing formatted rows.

        Args:
            statement: The statement to be executed against the database.
            format: A subclass of CursorFormatter, or one of: 'pandas', 'hive',
                'csv', 'tuple' or 'dict'. Defaults to
                `self.DEFAULT_CURSOR_FORMATTER`.
            format_opts: A dictionary of format-specific options.
            batch: If not `None`, the number of rows from the resulting cursor
                to be returned at once.
            **kwargs: Additional keyword arguments to pass onto
                `DatabaseClient.execute`.

        Returns:
            An iterator over objects of the nominated format or, if batched, a
            list of such objects.
        """
        format_opts = format_opts or {}
        cursor = self.execute(statement, wait=True, **kwargs)
        formatter = self._get_formatter(format, cursor, **format_opts)

        yield from formatter.stream(batch=batch)

    def _get_formatter(
        self,
        formatter: str | type[_cursor_formatters.CursorFormatter] | None,
        cursor: Any,
        **kwargs: Any,
    ) -> _cursor_formatters.CursorFormatter:
        formatter = formatter or self.DEFAULT_CURSOR_FORMATTER
        if not (
            inspect.isclass(formatter)
            and issubclass(formatter, _cursor_formatters.CursorFormatter)
        ):
            if (
                not isinstance(formatter, str)
                or formatter not in self.CURSOR_FORMATTERS
            ):
                raise ValueError(
                    f"Invalid format '{formatter}'. Choose from: {','.join(self.CURSOR_FORMATTERS.keys())}"
                )
            formatter = self.CURSOR_FORMATTERS[formatter]
        format_opts = dict(
            itertools.chain(self._default_format_opts.items(), kwargs.items())
        )
        return formatter(cursor, **format_opts)

    def stream_to_file(
        self,
        statement: str,
        file: str | Any,
        format: str = "csv",
        fs: FileSystemClient | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Execute a statement against this database and stream results to a file.

        This method is a wrapper around `DatabaseClient.stream` that enables the
        iterative writing of cursor results to a file. This is especially useful
        when there are a very large number of results, and loading them all into
        memory would require considerable resources. Note that 'csv' is the
        default format for this method (rather than `pandas`).

        Args:
            statement: The statement to be executed against the database.
            file: The filename where the data should be written, or an open
                file-like resource.
            format: The format to be used ('csv' by default). Format options
                can be passed via `**kwargs`.
            fs: The filesystem within which the nominated file should be found.
                If `None`, the local filesystem will be used.
            **kwargs: Additional keyword arguments to pass onto
                `DatabaseClient.stream`.
        """
        close_later = False
        if isinstance(file, str):
            file = (fs or LocalFsClient()).open(file, "w")
            close_later = True

        try:
            file.writelines(self.stream(statement, format=format, **kwargs))
        finally:
            if close_later:
                file.close()

    def execute_from_file(
        self,
        file: str | Any,
        fs: FileSystemClient | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a statement stored in a file.

        Args:
            file: The path of the file containing the query statement to be
                executed against the database, or an open file-like resource.
            fs: The filesystem within which the nominated file should be found.
                If `None`, the local filesystem will be used.
            **kwargs: Extra keyword arguments to pass on to
                `DatabaseClient.execute`.

        Returns:
            A DBAPI2 compatible cursor instance.
        """
        close_later = False
        if isinstance(file, str):
            file = (fs or LocalFsClient()).open(file, "r")
            close_later = True

        try:
            return self.execute(file.read(), **kwargs)
        finally:
            if close_later:
                file.close()

    def query_from_file(
        self,
        file: str | Any,
        fs: FileSystemClient | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Query using a statement stored in a file.

        Args:
            file: The path of the file containing the query statement to be
                executed against the database, or an open file-like resource.
            fs: The filesystem within which the nominated file should be found.
                If `None`, the local filesystem will be used.
            **kwargs: Extra keyword arguments to pass on to
                `DatabaseClient.query`.

        Returns:
            The results of the query formatted as nominated.
        """
        close_later = False
        if isinstance(file, str):
            file = (fs or LocalFsClient()).open(file, "r")
            close_later = True

        try:
            return self.query(file.read(), **kwargs)
        finally:
            if close_later:
                file.close()

    @property
    def template_names(self) -> list[str]:
        """
        list: A list of names associated with the templates associated with this
        client.
        """
        return list(self._templates)

    def template_add(self, name: str, body: str) -> Self:
        """
        Add a named template to the internal dictionary of templates.

        Note: Templates are interpreted as `jinja2` templates. See
        `.template_render` for more information.

        Args:
            name: The name of the template.
            body: The (typically) multiline body of the template.

        Returns:
            A reference to this object.
        """
        self._templates[name] = body
        return self

    def template_get(self, name: str) -> str:
        """
        Retrieve a named template.

        Args:
            name: The name of the template to retrieve.

        Raises:
            ValueError: If `name` is not associated with a template.

        Returns:
            The requested template.
        """
        if name not in self._templates:
            raise ValueError(f"No such template named: '{name}'.")
        return self._templates[name]

    def template_variables(
        self,
        name_or_statement: str,
        by_name: bool = False,
    ) -> set[str]:
        """
        Return the set of undeclared variables required for this template.

        Args:
            name_or_statement: The name of a template (if `by_name` is True) or
                else a string representation of a `jinja2` template.
            by_name: `True` if `name_or_statement` should be interpreted as a
                template name, or `False` (default) if `name_or_statement`
                should be interpreted as a template body.

        Returns:
            A set of names which the template requires to be rendered.
        """
        ast = jinja2.Environment().parse(  # noqa: S701
            self.template_render(name_or_statement, by_name=by_name, meta_only=True)
        )
        return jinja2.meta.find_undeclared_variables(ast)  # type: ignore[no-any-return]

    def template_render(
        self,
        name_or_statement: str,
        context: dict[str, Any] | None = None,
        by_name: bool = False,
        cleanup: bool = False,
        meta_only: bool = False,
    ) -> str:
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
            name_or_statement: The name of a template (if `by_name` is True) or
                else a string representation of a `jinja2` template.
            context: A dictionary to use as the template context. If not
                specified, an empty dictionary is used.
            by_name: `True` if `name_or_statement` should be interpreted as a
                template name, or `False` (default) if `name_or_statement`
                should be interpreted as a template body.
            cleanup: `True` if the rendered statement should be formatted,
                `False` (default) otherwise
            meta_only: `True` if rendering should only progress as far as
                rendering nested templates (i.e. don't actually substitute in
                variables from the context); `False` (default) otherwise.

        Returns:
            The rendered template.
        """
        if by_name:
            if name_or_statement not in self._templates:
                raise ValueError(f"No such template of name: '{name_or_statement}'.")
            statement = self._templates[name_or_statement]
        else:
            statement = name_or_statement

        try:
            from sqlalchemy.sql.base import Executable

            if isinstance(statement, Executable):
                statement = str(
                    statement.compile(compile_kwargs={"literal_binds": True})
                )
        except ImportError:
            pass

        if context is None or context is False:
            context = {}

        template_context: dict[str, Any] = {}
        template_context.update(self._template_context)  # default context
        template_context.update(context)  # context passed in
        intersection = set(self._template_context.keys()) & set(context.keys())
        if intersection:
            logger.warning(
                f"The following default template context keys have been overridden by the local context: {intersection}."
            )

        # Substitute in any other named statements recursively
        while "{{{" in statement or "{{%" in statement:
            statement = jinja2.Template(
                statement,
                block_start_string="{{%",
                block_end_string="%}}",
                variable_start_string="{{{",
                variable_end_string="}}}",
                comment_start_string="{{#",
                comment_end_string="#}}",
                undefined=jinja2.StrictUndefined,
            ).render(getattr(self, "_templates", {}))

        if not meta_only:
            statement = jinja2.Template(
                statement, undefined=jinja2.StrictUndefined
            ).render(template_context)

        if cleanup:
            statement = self.statement_cleanup(statement)

        return statement

    def execute_from_template(
        self,
        name: str,
        context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Render and then execute a named template.

        Args:
            name: The name of the template to be rendered and executed.
            context: The context in which the template should be rendered.
            **kwargs: Additional parameters to pass to `.execute()`.

        Returns:
            A DBAPI2 compatible cursor instance.
        """
        statement = self.template_render(name, context, by_name=True)
        return self.execute(statement, **kwargs)

    def query_from_template(
        self,
        name: str,
        context: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Render and then query using a named tempalte.

        Args:
            name: The name of the template to be rendered and used to query
                the database.
            context: The context in which the template should be rendered.
            **kwargs: Additional parameters to pass to `.query()`.

        Returns:
           The results of the query formatted as nominated.
        """
        statement = self.template_render(name, context, by_name=True)
        return self.query(statement, **kwargs)

    # Uploading/querying data into data store
    @logging_scope("Query [CTAS]", timed=True)
    @inherit_docs("_query_to_table")
    def query_to_table(
        self,
        statement: str,
        table: str | ParsedNamespaces,
        if_exists: Literal["fail", "replace", "append", "delete_rows"] = "fail",
        **kwargs: Any,
    ) -> Any:
        """
        Run a query and store the results in a table in this database.

        Args:
            statement: The statement to be executed.
            table: The name of the table into which the dataframe should be
                uploaded.
            if_exists: if nominated table already exists: 'fail' to do nothing,
                'replace' to drop, recreate and insert data into new table, and
                'append' to add data from this table into the existing table.
            **kwargs: Additional keyword arguments to pass onto
                `DatabaseClient._query_to_table`.

        Returns:
            The cursor object associated with the execution.
        """
        if if_exists not in {"fail", "replace", "append"}:
            raise ValueError(
                f"Invalid value for `if_exists`: {if_exists!r}. Choose from: 'fail', 'replace', 'append'."
            )
        table = self._parse_namespaces(table, write=True)
        return self._query_to_table(statement, table, if_exists=if_exists, **kwargs)

    @logging_scope("Dataframe Upload", timed=True)
    @inherit_docs("_dataframe_to_table")
    @require_connection
    def dataframe_to_table(
        self,
        df: pd.DataFrame,
        table: str | ParsedNamespaces,
        if_exists: Literal["fail", "replace", "append", "delete_rows"] = "fail",
        **kwargs: Any,
    ) -> None:
        """
        Upload a local pandas dataframe into a table in this database.

        Args:
            df: The dataframe to upload into the database.
            table: The name of the table into which the dataframe should be
                uploaded.
            if_exists: if nominated table already exists: 'fail' to do nothing,
                'replace' to drop, recreate and insert data into new table, and
                'append' to add data from this table into the existing table.
            **kwargs: Additional keyword arguments to pass onto
                `DatabaseClient._dataframe_to_table`.
        """
        if if_exists not in {"fail", "replace", "append"}:
            raise ValueError(
                f"Invalid value for `if_exists`: {if_exists!r}. Choose from: 'fail', 'replace', 'append'."
            )
        self._dataframe_to_table(
            df, self._parse_namespaces(table, write=True), if_exists=if_exists, **kwargs
        )

    # Table properties

    @abstractmethod
    def _execute(
        self,
        statement: str,
        cursor: Any,
        wait: bool,
        session_properties: dict[str, Any],
    ) -> Any:
        pass

    def _query_to_table(
        self,
        statement: str,
        table: ParsedNamespaces,
        if_exists: Literal["fail", "replace", "append", "delete_rows"],
        **kwargs: Any,
    ) -> Any:
        raise NotImplementedError

    def _dataframe_to_table(
        self,
        df: pd.DataFrame,
        table: ParsedNamespaces,
        if_exists: Literal["fail", "replace", "append", "delete_rows"] = "fail",
        **kwargs: Any,
    ) -> None:
        raise NotImplementedError

    def _cursor_empty(self, cursor: Any) -> bool:
        return cursor is None

    def _parse_namespaces(
        self,
        name: str | ParsedNamespaces,
        level: int = 0,
        defaults: dict[str, str] | None = None,
        write: bool = False,
    ) -> ParsedNamespaces:
        return ParsedNamespaces.from_name(
            name,
            self.NAMESPACE_NAMES[:-level] if level > 0 else self.NAMESPACE_NAMES,
            quote_char=self.NAMESPACE_QUOTECHAR,
            separator=self.NAMESPACE_SEPARATOR,
            defaults=defaults
            if defaults
            else (
                self.NAMESPACE_DEFAULTS_WRITE if write else self.NAMESPACE_DEFAULTS_READ
            ),
        )

    @inherit_docs("_table_list")
    def table_list(
        self,
        namespace: str,
        renew: bool = True,
        **kwargs: Any,
    ) -> Any:
        """
        Return a list of table names in the data source as a DataFrame.

        Args:
            namespace: The namespace in which to look for tables.
            renew: Whether to renew the table list or use cached results
                (default: True).
            **kwargs: Additional arguments passed through to implementation.

        Returns:
            The names of schemas in this database.
        """
        return self._table_list(
            self._parse_namespaces(namespace, level=1), renew=renew, **kwargs
        )

    @abstractmethod
    def _table_list(self, namespace: ParsedNamespaces, **kwargs: Any) -> Any:
        pass

    @inherit_docs("_table_exists")
    def table_exists(
        self,
        table: str | ParsedNamespaces,
        renew: bool = True,
        **kwargs: Any,
    ) -> bool:
        """
        Check whether a table exists.

        Args:
            table: The table for which to check.
            renew: Whether to renew the table list or use cached results
                (default: True).
            **kwargs: Additional arguments passed through to implementation.

        Returns:
            `True` if table exists, and `False` otherwise.
        """
        return self._table_exists(
            table=self._parse_namespaces(table), renew=renew, **kwargs
        )

    @abstractmethod
    def _table_exists(self, table: ParsedNamespaces, **kwargs: Any) -> bool:
        pass

    @inherit_docs("_table_drop")
    def table_drop(self, table: str | ParsedNamespaces, **kwargs: Any) -> Any:
        """
        Remove a table from the database.

        Args:
            table: The table to drop.
            **kwargs: Additional arguments passed through to implementation.

        Returns:
            The cursor associated with this execution.
        """
        return self._table_drop(
            table=self._parse_namespaces(table, write=True), **kwargs
        )

    @abstractmethod
    def _table_drop(self, table: ParsedNamespaces, **kwargs: Any) -> Any:
        pass

    @inherit_docs("_table_desc")
    def table_desc(
        self,
        table: str | ParsedNamespaces,
        renew: bool = True,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Describe a table in the database.

        Args:
            table: The table to describe.
            renew: Whether to renew the results (default: True).
            **kwargs: Additional arguments passed through to implementation.

        Returns:
            A dataframe description of the table.
        """
        return self._table_desc(
            table=self._parse_namespaces(table), renew=renew, **kwargs
        )

    @abstractmethod
    def _table_desc(self, table: ParsedNamespaces, **kwargs: Any) -> pd.DataFrame:
        pass

    @inherit_docs("_table_partition_cols")
    def table_partition_cols(
        self,
        table: str | ParsedNamespaces,
        renew: bool = True,
        **kwargs: Any,
    ) -> list[str]:
        """
        Extract the columns by which a table is partitioned (if database supports partitions).

        Args:
            table: The table from which to extract data.
            renew: Whether to renew the results (default: True).
            **kwargs: Additional arguments passed through to implementation.

        Returns:
            A list of columns by which table is partitioned.
        """
        return self._table_partition_cols(
            table=self._parse_namespaces(table), renew=renew, **kwargs
        )

    def _table_partition_cols(
        self, table: ParsedNamespaces, **kwargs: Any
    ) -> list[str]:
        raise NotImplementedError(
            f"Database backend `{self.__class__.__name__}` does not support, or has not implemented, support for extracting partition columns."
        )

    @inherit_docs("_table_head")
    def table_head(
        self,
        table: str | ParsedNamespaces,
        n: int = 10,
        renew: bool = True,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve the first `n` rows from a table.

        Args:
            table: The table from which to extract data.
            n: The number of rows to extract.
            renew: Whether to renew the table list or use cached results
                (default: True).
            **kwargs: Additional arguments passed through to implementation.

        Returns:
            A dataframe representation of the first `n` rows of the nominated
            table.
        """
        return self._table_head(
            table=self._parse_namespaces(table), n=n, renew=renew, **kwargs
        )

    @abstractmethod
    def _table_head(
        self, table: ParsedNamespaces, n: int = 10, **kwargs: Any
    ) -> pd.DataFrame:
        pass

    @inherit_docs("_table_props")
    def table_props(
        self,
        table: str | ParsedNamespaces,
        renew: bool = True,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve the properties associated with a table.

        Args:
            table: The table from which to extract data.
            renew: Whether to renew the table list or use cached results
                (default: True).
            **kwargs: Additional arguments passed through to implementation.

        Returns:
            A dataframe representation of the table properties.
        """
        return self._table_props(
            table=self._parse_namespaces(table), renew=renew, **kwargs
        )

    @abstractmethod
    def _table_props(self, table: ParsedNamespaces, **kwargs: Any) -> pd.DataFrame:
        pass

    @override
    def _register_magics(self, base_name: str) -> None:
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
        from IPython.core.magic import (
            register_cell_magic,
            register_line_cell_magic,
            register_line_magic,
        )

        def statement_executor_magic(
            executor: str,
            statement: str | None,
            variable: str | None = None,
            show: str | int = "head",
            transpose: bool = False,
            template: bool = True,
            context: dict[str, Any] | None = None,
            **kwargs: Any,
        ) -> Any:
            ip = get_ipython()

            if context is None:
                context = ip.user_ns

            # Line magic
            if statement is None:
                if variable is None:
                    raise ValueError(
                        "A template name must be provided when using line magic."
                    )
                return self.query_from_template(variable, context=context, **kwargs)

            # Cell magic
            result = getattr(self, executor)(
                statement, template=template, context=context, **kwargs
            )

            if variable is not None:
                ip.user_ns[variable] = result

            if executor != "query":
                if variable is None:
                    return result
                return None
            if variable is None:
                return result

            format = kwargs.get("format", self.DEFAULT_CURSOR_FORMATTER)
            if show == "head":
                show = 10
            if isinstance(show, int):
                r = result.head(show) if format == "pandas" else result[:show]
            elif show == "all":
                r = result
            elif show in (None, "none"):
                return None
            else:
                raise ValueError(
                    f"Omniduct does not recognise the argument show='{show}' in cell magic."
                )

            if format == "pandas" and transpose:
                return r.T
            return r

        @register_line_cell_magic(base_name)
        @process_line_cell_arguments
        def query_magic(*args: Any, **kwargs: Any) -> Any:
            return statement_executor_magic("query", *args, **kwargs)

        @register_line_cell_magic(f"{base_name}.execute")
        @process_line_cell_arguments
        def execute_magic(*args: Any, **kwargs: Any) -> Any:
            return statement_executor_magic("execute", *args, **kwargs)

        @register_line_cell_magic(f"{base_name}.stream")
        @process_line_cell_arguments
        def stream_magic(*args: Any, **kwargs: Any) -> Any:
            return statement_executor_magic("stream", *args, **kwargs)

        @register_cell_magic(f"{base_name}.template")
        @process_line_arguments
        def template_add(body: str, name: str) -> None:
            self.template_add(name, body)

        @register_line_cell_magic(f"{base_name}.render")
        @process_line_cell_arguments
        def template_render_magic(
            body: str | None = None,
            name: str | None = None,
            context: dict[str, Any] | None = None,
            show: bool = True,
            cleanup: bool = False,
            meta_only: bool = False,
        ) -> Any:
            ip = get_ipython()

            if body is None:
                if name is None:
                    raise ValueError("Name must be specified in line-mode.")
                rendered = self.template_render(
                    name,
                    context=context or ip.user_ns,
                    by_name=True,
                    cleanup=cleanup,
                    meta_only=meta_only,
                )
            else:
                rendered = self.template_render(
                    body,
                    context=context or ip.user_ns,
                    by_name=False,
                    cleanup=cleanup,
                    meta_only=meta_only,
                )
                if name is not None:
                    ip.user_ns[name] = rendered

            if show:
                return print(rendered)
            return rendered

        @register_line_magic(f"{base_name}.desc")
        @process_line_arguments
        def table_desc(table_name: str, **kwargs: Any) -> Any:
            return self.table_desc(table_name, **kwargs)

        @register_line_magic(f"{base_name}.head")
        @process_line_arguments
        def table_head(table_name: str, **kwargs: Any) -> Any:
            return self.table_head(table_name, **kwargs)

        @register_line_magic(f"{base_name}.props")
        @process_line_arguments
        def table_props(table_name: str, **kwargs: Any) -> Any:
            return self.table_props(table_name, **kwargs)
