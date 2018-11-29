from __future__ import absolute_import

import json
import os
import re
import shutil
import tempfile
import time

import pandas as pd
from jinja2 import Template

from omniduct.utils.debug import logger
from omniduct.utils.processes import Timeout, run_in_subprocess

from .base import DatabaseClient
from ._schemas import SchemasMixin
from . import _pandas


class HiveServer2Client(DatabaseClient, SchemasMixin):
    """
    This Duct connects to an Apache HiveServer2 server instance using the
    `pyhive` or `impyla` libraries.

    Attributes:
        schema (str, None): The default schema to use for queries (will
            default to server-default if not specified).
        driver (str): One of 'pyhive' (default) or 'impyla', which specifies
            how the client communicates with Hive.
        auth_mechanism (str): The authorisation protocol to use for connections.
            Defaults to 'NOSASL'. Authorisation methods differ between drivers.
            Please refer to `pyhive` and `impyla` documentation for more details.
        push_using_hive_cli (bool): Whether the `.push()` operation should
            directly add files using `LOAD DATA LOCAL INPATH` rather than the
            `INSERT` operation via SQLAlchemy. Note that this requires the
            presence of the `hive` executable on the local PATH, or if
            connecting via a `RemoteClient` instance, on the remote's PATH.
            This is mostly useful for older versions of Hive which do not
            support the `INSERT` statement.
        default_table_props (dict): A dictionary of table properties to use by
            default when creating tables.
        connection_options (dict): Additional options to pass through to the
            `.connect()` methods of the drivers.
    """

    PROTOCOLS = ['hiveserver2']
    DEFAULT_PORT = 3623
    SUPPORTS_SESSION_PROPERTIES = True
    NAMESPACE_NAMES = ['schema', 'table']
    NAMESPACE_QUOTECHAR = '`'
    NAMESPACE_SEPARATOR = '.'

    def _init(self, schema=None, driver='pyhive', auth_mechanism='NOSASL',
              push_using_hive_cli=False, default_table_props=None, **connection_options):
        """
        schema (str, None): The default database/schema to use for queries (will
            default to server-default if not specified).
        driver (str): One of 'pyhive' (default) or 'impyla', which specifies
            how the client communicates with Hive.
        auth_mechanism (str): The authorisation protocol to use for connections.
            Defaults to 'NOSASL'. Authorisation methods differ between drivers.
            Please refer to `pyhive` and `impyla` documentation for more details.
        push_using_hive_cli (bool): Whether the `.push()` operation should
            directly add files using `LOAD DATA LOCAL INPATH` rather than the
            `INSERT` operation via SQLAlchemy. Note that this requires the
            presence of the `hive` executable on the local PATH, or if
            connecting via a `RemoteClient` instance, on the remote's PATH.
            This is mostly useful for older versions of Hive which do not
            support the `INSERT` statement. False by default.
        default_table_props (dict): A dictionary of table properties to use by
            default when creating tables (default is an empty dict).
        **connection_options (dict): Additional options to pass through to the
            `.connect()` methods of the drivers.
        """
        self.schema = schema
        self.driver = driver
        self.auth_mechanism = auth_mechanism
        self.connection_options = connection_options
        self.push_using_hive_cli = push_using_hive_cli
        self.default_table_props = default_table_props or {}
        self.__hive = None
        self.connection_fields += ('schema',)

        assert self.driver in ('pyhive', 'impyla'), "Supported drivers are pyhive and impyla."

    def _connect(self):
        from sqlalchemy import create_engine, MetaData
        if self.driver == 'pyhive':
            try:
                import pyhive.hive
            except ImportError:
                raise ImportError("""
                    Omniduct is attempting to use the 'pyhive' driver, but it
                    is not installed. Please either install the pyhive package,
                    or reconfigure this Duct to use the 'impyla' driver.
                    """)
            self.__hive = pyhive.hive.connect(host=self.host,
                                              port=self.port,
                                              auth=self.auth_mechanism,
                                              database=self.schema,
                                              username=self.username,
                                              password=self.password,
                                              **self.connection_options)
            self._sqlalchemy_engine = create_engine('hive://{}:{}/{}'.format(self.host, self.port, self.schema))
            self._sqlalchemy_metadata = MetaData(self._sqlalchemy_engine)
        elif self.driver == 'impyla':
            try:
                import impala.dbapi
            except ImportError:
                raise ImportError("""
                    Omniduct is attempting to use the 'impyla' driver, but it
                    is not installed. Please either install the impyla package,
                    or reconfigure this Duct to use the 'pyhive' driver.
                    """)
            self.__hive = impala.dbapi.connect(host=self.host,
                                               port=self.port,
                                               auth_mechanism=self.auth_mechanism,
                                               database=self.schema,
                                               user=self.username,
                                               password=self.password,
                                               **self.connection_options)
            self._sqlalchemy_engine = create_engine('impala://{}:{}/{}'.format(self.host, self.port, self.schema))
            self._sqlalchemy_metadata = MetaData(self._sqlalchemy_engine)

    def __hive_cursor(self):
        if self.driver == 'impyla':  # Impyla seems to have all manner of connection issues, attempt to restore connection
            try:
                with Timeout(1):
                    return self.__hive.cursor()
            except:
                self._connect()
        return self.__hive.cursor()

    def _is_connected(self):
        return self.__hive is not None

    def _disconnect(self):
        logger.info('Disconnecting from Hive coordinator...')
        try:
            self.__hive.close()
        except:
            pass
        self.__hive = None
        self._sqlalchemy_engine = None
        self._sqlalchemy_metadata = None
        self._schemas = None

    def _statement_prepare(self, statement, session_properties, **kwargs):
        return (
            "\n".join(
                "SET {key} = {value};".format(key=key, value=value)
                for key, value in session_properties.items()
            ) + statement
        )

    def _execute(self, statement, cursor, wait, session_properties, poll_interval=1):
        """
        Additional Args:
            poll_interval (int): Default delay in seconds between consecutive
                query status (defaults to 1).
        """
        cursor = cursor or self.__hive_cursor()
        log_offset = 0

        if self.driver == 'pyhive':
            from TCLIService.ttypes import TOperationState  # noqa: F821
            cursor.execute(statement, **{'async': True})

            if wait:
                status = cursor.poll().operationState
                while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
                    log_offset = self._log_status(cursor, log_offset)
                    time.sleep(poll_interval)
                    status = cursor.poll().operationState

        elif self.driver == 'impyla':
            cursor.execute_async(statement)
            if wait:
                while cursor.is_executing():
                    log_offset = self._log_status(cursor, log_offset)
                    time.sleep(poll_interval)

        return cursor

    def _cursor_empty(self, cursor):
        if self.driver == 'impyla':
            return not cursor.has_result_set
        elif self.driver == 'pyhive':
            return cursor.description is None
        return False

    def _cursor_wait(self, cursor, poll_interval=1):
        from TCLIService.ttypes import TOperationState  # noqa: F821
        status = cursor.poll().operationState
        while status in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE):
            time.sleep(poll_interval)
            status = cursor.poll().operationState

    def _log_status(self, cursor, log_offset=0):
        matcher = re.compile('[0-9/]+ [0-9:]+ (INFO )?')

        if self.driver == 'pyhive':
            log = cursor.fetch_logs()
        else:
            log = cursor.get_log().strip().split('\n')

        for line in log[log_offset:]:
            if not line:
                continue
            m = matcher.match(line)
            if m:
                line = line[len(m.group(0)):]
            logger.info(line)

        return len(log)

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

    def _dataframe_to_table(
        self, df, table, if_exists='fail', use_hive_cli=None,
        partition=None, sep=chr(1), table_props=None, dtype_overrides=None, **kwargs
    ):
        """
        If `use_hive_cli` (or if not specified `.push_using_hive_cli`) is
        `True`, a `CREATE TABLE` statement will be automatically generated based
        on the datatypes of the DataFrame (unless overwritten by
        `dtype_overrides`). The `DataFrame` will then be exported to a CSV
        compatible with Hive and uploaded (if necessary) to the remote, before
        being loaded into Hive using a `LOAD DATA LOCAL INFILE ...` query using
        the `hive` cli executable. Note that if a table is not partitioned, you
        cannot convert it to a parititioned table without deleting it first.

        If `use_hive_cli` (or if not specified `.push_using_hive_cli`) is
        `False`, an attempt will be made to push the `DataFrame` to Hive using
        `pandas.DataFrame.to_sql` and the SQLAlchemy binding provided by
        `pyhive` and `impyla`. This may be slower, does not support older
        versions of Hive, and does not support table properties or partitioning.

        If if the schema namespace is not specified, `table.schema` will be
        defaulted to your username.

        Additional Args:
            use_hive_cli (bool, None): A local override for the global
                `.push_using_hive_cli` attribute. If not specified, the global
                default is used. If True, then pushes are performed using the
                `hive` CLI executable on the local/remote PATH.
            **kwargs (dict): Additional arguments to send to `pandas.DataFrame.to_sql`.

        Further Parameters for CLI method (specifying these for the pandas
        method will cause a `RuntimeError` exception):
            partition (dict): A mapping of column names to values that specify
                the partition into which the provided data should be uploaded,
                as well as providing the fields by which new tables should be
                partitioned.
            sep (str): Field delimiter for data (defaults to CTRL-A, or `chr(1)`).
            table_props (dict): Properties to set on any newly created tables
                (extends `.default_table_props`).
            dtype_overrides (dict): Mapping of column names to Hive datatypes to
                use instead of default mapping.
        """
        table = self._parse_namespaces(table, defaults={'schema': self.username})
        use_hive_cli = use_hive_cli or self.push_using_hive_cli
        partition = partition or {}
        table_props = table_props or {}
        dtype_overrides = dtype_overrides or {}

        # Try using SQLALchemy method
        if not use_hive_cli:
            if partition or table_props or dtype_overrides:
                raise RuntimeError(
                    "At least one of `partition` or `table_props` or "
                    "`dtype_overrides` has been specified. Setting table "
                    "properties or partition information is not supported "
                    "via the SQLAlchemy backend. If this is important, please "
                    "pass `use_hive_cli=True`, otherwise remove these values "
                    "and try again."
                )
            try:
                return _pandas.to_sql(
                    df=df, name=table.table, schema=table.schema, con=self._sqlalchemy_engine,
                    index=False, if_exists=if_exists, **kwargs
                )
            except Exception as e:
                raise RuntimeError(
                    "Push unsuccessful. Your version of Hive may be too old to "
                    "support the `INSERT` keyword. You might want to try setting "
                    "`.push_using_hive_cli = True` if your local or remote "
                    "machine has access to the `hive` CLI executable. The "
                    "original exception was: {}".format(e.args[0])
                )

        # Try using Hive CLI

        # If `partition` is specified, the associated columns must not be
        # present in the dataframe.
        assert len(set(partition).intersection(df.columns)) == 0, "The dataframe to be uploaded must not have any partitioned fields. Please remove the field(s): {}.".format(','.join(set(partition).intersection(df.columns)))

        # Save dataframe to file and send it to the remote server if necessary
        temp_dir = tempfile.mkdtemp(prefix='omniduct_hiveserver2')
        tmp_fname = os.path.join(temp_dir, 'data_{}.csv'.format(time.time()))
        logger.info('Saving dataframe to file... {}'.format(tmp_fname))
        df.fillna(r'\N').to_csv(tmp_fname, index=False, header=False,
                                sep=sep, encoding='utf-8')

        if self.remote:
            logger.info("Uploading data to remote host...")
            self.remote.upload(tmp_fname)

        # Generate create table statement.
        auto_table_props = set(self.default_table_props).difference(table_props)
        if len(auto_table_props) > 0:
            logger.warning(
                "In addition to any specified table properties, this "
                "HiveServer2Client has added the following default table "
                "properties:\n{default_props}\nTo override them, please "
                "specify overrides using: `.push(..., table_props={{...}}).`"
                .format(default_props=json.dumps({
                    prop: value for prop, value in self.default_table_props.items()
                    if prop in auto_table_props
                }, indent=True))
            )

        tblprops = self.default_table_props.copy()
        tblprops.update(table_props or {})
        cts = self._create_table_statement_from_df(
            df=df,
            table=table,
            drop=(if_exists == 'replace') and not partition,
            text=True,
            sep=sep,
            table_props=tblprops,
            partition_cols=list(partition),
            dtype_overrides=dtype_overrides
        )

        # Generate load data statement.
        partition_clause = (
            ''
            if not partition
            else 'PARTITION ({})'.format(
                ','.join("{key} = '{value}'".format(key=key, value=value) for key, value in partition.items())
            )
        )
        lds = '\nLOAD DATA LOCAL INPATH "{path}" {overwrite} INTO TABLE {table} {partition_clause};'.format(
            path=os.path.basename(tmp_fname) if self.remote else tmp_fname,
            overwrite="OVERWRITE" if if_exists == "replace" else "",
            table=table,
            partition_clause=partition_clause
        )

        # Run create table statement and load data statments
        logger.info(
            "Creating hive table `{table}` if it does not "
            "already exist, and inserting the provided data{partition}."
            .format(
                table=table,
                partition=" into {}".format(partition_clause) if partition_clause else ""
            )
        )
        try:
            stmts = '\n'.join([cts, lds])
            logger.debug(stmts)
            proc = self._run_in_hivecli(stmts)
            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.decode('utf-8'))
        finally:
            # Clean up files
            if self.remote:
                self.remote.execute('rm -rf {}'.format(tmp_fname))
            shutil.rmtree(temp_dir, ignore_errors=True)

        logger.info("Successfully uploaded dataframe {partition}`{table}`.".format(
            table=table,
            partition="into {} of ".format(partition_clause) if partition_clause else ""
        ))

    def _table_list(self, namespace, like='*', **kwargs):
        schema = namespace.name or self.schema
        return self.query("SHOW TABLES IN {0} '{1}'".format(schema, like),
                          **kwargs)

    def _table_exists(self, table, **kwargs):
        logger.disabled = True
        try:
            self.table_desc(table, **kwargs)
            return True
        except:
            return False
        finally:
            logger.disabled = False

    def _table_drop(self, table, **kwargs):
        return self.execute("DROP TABLE {table}".format(table=table))

    def _table_desc(self, table, **kwargs):
        records = self.query("DESCRIBE {0}".format(table), **kwargs)

        # pretty hacky but hive doesn't return DESCRIBE results in a nice format
        # TODO is there any information we should pull out of DESCRIBE EXTENDED
        for i, record in enumerate(records):
            if record[0] == '':
                break

        columns = ['col_name', 'data_type', 'comment']
        fields_df = pd.DataFrame(records[:i], columns=columns)

        partitions_df = pd.DataFrame(records[i + 4:], columns=columns)
        partitions_df['comment'] = "PARTITION " + partitions_df['comment']

        return pd.concat((fields_df, partitions_df))

    def _table_head(self, table, n=10, **kwargs):
        return self.query("SELECT * FROM {} LIMIT {}".format(table, n), **kwargs)

    def _table_props(self, table, **kwargs):
        return self.query('SHOW TBLPROPERTIES {0}'.format(table), **kwargs)

    def _run_in_hivecli(self, cmd):
        """Run a query using hive cli in a subprocess."""
        # Turn hive command into quotable string.
        double_escaped = re.sub('\\' * 2, '\\' * 4, cmd)
        backtick_escape = '\\\\\\`' if self.remote else '\\`'
        sys_cmd = 'hive -e "{0}"'.format(re.sub('"', '\\"', double_escaped)) \
                                 .replace('`', backtick_escape)
        # Execute command in a subprocess.
        if self.remote:
            proc = self.remote.execute(sys_cmd)
        else:
            proc = run_in_subprocess(sys_cmd, check_output=True)
        return proc

    @classmethod
    def _create_table_statement_from_df(cls, df, table, drop=False,
                                        text=True, sep=chr(1), loc=None,
                                        table_props=None, partition_cols=None,
                                        dtype_overrides=None):
        """
        Return create table statement for new hive table based on pandas dataframe.

        Args:
            df (pandas.DataFrame, pandas.Series): Used to determine column names
                and types for create table statement.
            table (ParsedNamespaces): The parsed name of the target table.
            drop (bool): Whether to include a drop table statement before the
                create table statement.
            text (bool): Whether data will be stored as a textfile.
            sep (str): The separator used by the text data store (defaults to
                CTRL-A, i.e. `chr(1)`, which is the default Hive separator).
            loc (str): Desired HDFS location (if not the default).
            table_props (dict): The table properties (if any) to set on the table.
            partition_cols (list): The columns by which the created table should
                be partitioned.

        Returns:
            str: The Hive SQL required to create the table with the above
                configuration.
        """
        table_props = table_props or {}
        partition_cols = partition_cols or []
        dtype_overrides = dtype_overrides or {}

        # dtype kind to hive type mapping dict.
        DTYPE_KIND_HIVE_TYPE = {
            'b': 'BOOLEAN',  # boolean
            'i': 'BIGINT',   # signed integer
            'u': 'BIGINT',   # unsigned integer
            'f': 'DOUBLE',   # floating-point
            'c': 'STRING',   # complex floating-point
            'O': 'STRING',   # object
            'S': 'STRING',   # (byte-)string
            'U': 'STRING',   # Unicode
            'V': 'STRING'    # void
        }

        # Sanitise column names and map numpy/pandas data-types to hive types.
        columns = []
        for col, dtype in df.dtypes.iteritems():
            col_sanitized = re.sub(r'\W', '', col.lower().replace(' ', '_'))
            hive_type = dtype_overrides.get(col) or DTYPE_KIND_HIVE_TYPE.get(dtype.kind)
            if hive_type is None:
                hive_type = DTYPE_KIND_HIVE_TYPE['O']
                logger.warning(
                    'Unable to determine hive type for dataframe column {col} of pandas dtype {dtype}. '
                    'Defaulting to hive type {hive_type}. If other column type is desired, '
                    'please specify via `dtype_overrides`'
                    .format(**locals())
                )
            columns.append(
                '  {column}  {type}'.format(column=col_sanitized, type=hive_type)
            )

        partition_columns = ['{} STRING'.format(col) for col in partition_cols]

        tblprops = ["'{key}' = '{value}'".format(key=key, value=value) for key, value in table_props.items()]
        tblprops = "TBLPROPERTIES({})".format(",".join(tblprops)) if len(tblprops) > 0 else ""

        cmd = Template("""
        {% if drop %}
        DROP TABLE IF EXISTS {{ table }};
        {% endif -%}
        CREATE TABLE IF NOT EXISTS {{ table }} (
            {%- for col in columns %}
            {{ col }} {% if not loop.last %}, {% endif %}
            {%- endfor %}
        )
        {%- if partition_columns %}
        PARTITIONED BY (
            {%- for col in partition_columns %}
            {{ col }} {% if not loop.last %}, {% endif %}
            {%- endfor %}
        )
        {%- endif %}
        {%- if text %}
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY "{{ sep }}"
        STORED AS TEXTFILE
        {% endif %}
        {%- if loc %}
        LOCATION "{{ loc }}"
        {%- endif %}
        {{ tblprops }}
        ;
        """).render(**locals())

        return cmd
