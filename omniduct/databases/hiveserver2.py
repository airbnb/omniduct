from __future__ import absolute_import

import logging
import os
import re
import tempfile
import time

import pandas as pd
from jinja2 import Template

from omniduct.utils.config import config
from omniduct.utils.debug import logger
from omniduct.utils.processes import Timeout, run_in_subprocess

from .base import DatabaseClient


class HiveServer2Client(DatabaseClient):

    PROTOCOLS = ['hiveserver2']
    DEFAULT_PORT = 3623

    def _init(self, schema=None, driver='pyhive', auth_mechanism='NOSASL', **connection_options):
        self.schema = schema
        self.driver = driver
        self.auth_mechanism = auth_mechanism
        self.connection_options = connection_options
        self.__hive = None
        self.connection_fields += ('schema',)

        assert self.driver in ('pyhive', 'impyla'), "Supported drivers are pyhive and impyla."

    def _connect(self):
        if self.driver == 'pyhive':
            import pyhive.hive
            self.__hive = pyhive.hive.connect(host=self.host,
                                              port=self.port,
                                              auth=self.auth_mechanism,
                                              database=self.schema,
                                              **self.connection_options)
        elif self.driver == 'impyla':
            import impala.dbapi
            self.__hive = impala.dbapi.connect(host=self.host,
                                               port=self.port,
                                               auth_mechanism=self.auth_mechanism,
                                               database=self.schema,
                                               **self.connection_options)

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

    def _execute(self, statement, query=True, cursor=None, poll_interval=1, wait=True):
        """
        Execute command

        poll_interval : int, optional
            Default delay in polling for query status
        """
        cursor = cursor or self.__hive_cursor()
        log_offset = 0

        if self.driver == 'pyhive':
            from TCLIService.ttypes import TOperationState
            cursor.execute(statement, async=True)

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
        return False

    def _log_status(self, cursor, log_offset=0):
        matcher = re.compile('[0-9/]+ [0-9\:]+ (INFO )?')

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

    def _push(self, df, table, partition_clause='', overwrite=False, schema='omniduct', sep='\t'):
        """
        Create a new table in hive from pandas DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame or Series
            Data to be push into a hive table.
        table : str
            Table name for new hive table.
        schema : str
            Schema (or database) for new hive table.
        partition_clause : str
            The hive partition clause specifying which partitions to load data into.
        overwrite : bool, optional
            Whether to overwrite the table data if it exists. Default: False.
        sep : str
            Field delimiter for data.

        See Also
        --------
        https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML
        """
        # Save dataframe to file.
        _, tmp_path = tempfile.mkstemp(dir='.')
        tmp_fname = os.path.basename(tmp_path)

        logger.info('Saving dataframe to file... {}'.format(tmp_fname))
        df.to_csv(tmp_fname, index=False, header=False, sep=sep, encoding='utf-8')

        # Create table statement.
        cts = _create_table_statement_from_df(df=df, table=table,
                                              schema=schema, drop=overwrite and not partition_clause,
                                              text=True, sep=sep)
        # Load data statement.
        lds = '\nLOAD DATA LOCAL INPATH "{path}" {overwrite} INTO TABLE {schema}.{table} {partition_clause};'.format(
            path=tmp_fname,
            overwrite="OVERWRITE" if overwrite else "",
            schema=schema,
            table=table,
            partition_clause=partition_clause)

        # SCP data if SSHClient is set.
        if self.remote:
            logger.info('Uploading data to remote host...')
            self.remote.copy_from_local(tmp_fname, tmp_fname)
        # Run create table statement and load data statment.
        logger.info('Creating hive table and loading data...')
        proc = self._run_in_hivecli('\n'.join([cts, lds]))
        if proc.returncode != 0:
            logger.error(proc.stderr)

        # Clean up files.
        logger.info('Cleaning up files...')
        rm_cmd = 'rm -rf {0}'.format(tmp_fname)
        run_in_subprocess(rm_cmd)
        if self.remote:
            self.remote.execute(rm_cmd)
        return proc

    def _table_list(self, schema=None, like='*', **kwargs):
        schema = schema or self.schema or 'default'
        return self.query("SHOW TABLES IN {0} '{1}'".format(schema, like),
                          **kwargs)

    def _table_exists(self, table, schema=None):
        return (self.table_list(renew=True, schema=schema)['tab_name'] == table).any()

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
        return self.query('SHOW TBLPROPERTIES `{0}`'.format(table), **kwargs)

    def _run_in_hivecli(self, cmd):
        """Run a query using hive cli in a subprocess."""
        # Turn hive command into quotable string.
        double_escaped = re.sub('\\' * 2, '\\' * 4, cmd)
        sys_cmd = 'hive -e "{0}"'.format(re.sub('"', '\\"', double_escaped))
        # Execute command in a subprocess.
        if self.remote:
            proc = self.remote.execute(sys_cmd)
        else:
            proc = run_in_subprocess(sys_cmd, check_output=True)
        return proc


def _create_table_statement_from_df(df, table, schema='default', drop=False,
                                    text=True, sep=None, loc=None):
    """
    Return create table statement for new hive table based on pandas dataframe.

    Parameters
    ----------
    df : pandas.DataFrame or pandas.Series
        Used to determine column names and types for create table statement.
    table : str
        Table name for create table statement.
    schema : str
        Schema for create table statement
    drop : bool
        Whether to include a drop table statement along with create table statement.
    text : bool
        Whether data will be stored as a text file.
    sep : str
        Field delimiter for text file (only used if text==True).
    loc : str, optional
        Desired hdfs location.

    Returns
    -------
    cmd : str
        A create table statement.
    """
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
    sep = sep or "\t"

    # Sanitive column names and map data types to hive types.
    columns = []
    for col, dtype in df.dtypes.iteritems():
        col_sanitized = re.sub('\W', '', col.lower().replace(' ', '_'))
        hive_type = DTYPE_KIND_HIVE_TYPE[dtype.kind]
        columns.append('  {column}  {type}'.format(column=col_sanitized,
                                                   type=hive_type))

    cmd = Template("""
    {% if drop %}
    DROP TABLE IF EXISTS {{ schema }}.{{ table }};
    {% endif -%}
    CREATE TABLE IF NOT EXISTS {{ schema }}.{{ table }} (
    {%- for col in columns %}
     {{ col }} {% if not loop.last %}, {% endif %}
    {%- endfor %}
    )
    {%- if text %}
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY "{{ sep }}"
    STORED AS TEXTFILE
    {% endif %}
    {%- if loc %}
    LOCATION "{{ loc }}"
    {%- endif %}
    ;
    """).render(drop=drop, table=table, schema=schema, columns=columns, text=text, sep=sep)

    logger.debug('Create Table Statement: {}'.format(cmd))
    return cmd
