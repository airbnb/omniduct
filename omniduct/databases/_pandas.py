from pandas.io.sql import SQLTable, SQLDatabase


def to_sql(df, name, schema, con, index, if_exists, mode='default', **kwargs):
    """
    Override the default `pandas.to_sql` method to allow for insertion of
    multiple rows of data at once. This is derived from the upstream patch at
    https://github.com/pandas-dev/pandas/pull/21401, and can be deprecated
    once it is merged and released in a new version of `pandas`.
    """
    assert mode in ('default', 'multi'), 'unexpected `to_sql` mode {}'.format(mode)
    if mode == 'default':
        return df.to_sql(
            name=name, schema=schema, con=con, index=index, if_exists=if_exists, **kwargs
        )
    else:
        nrows = len(df)
        if nrows == 0:
            return

        chunksize = kwargs.get('chunksize', nrows)
        if chunksize == 0:
            raise ValueError('chunksize argument should be non-zero')
        chunks = int(nrows / chunksize) + 1

        pd_sql = SQLDatabase(con)
        pd_table = SQLTable(
            name, pd_sql, frame=df, index=index, if_exists=if_exists,
            index_label=kwargs.get('insert_label'), schema=schema, dtype=kwargs.get('dtype')
        )
        pd_table.create()
        keys, data_list = pd_table.insert_data()

        with pd_sql.run_transaction() as conn:
            for i in range(chunks):
                start_i = i * chunksize
                end_i = min((i + 1) * chunksize, nrows)
                if start_i >= end_i:
                    break

                chunk_iter = zip(*[arr[start_i:end_i] for arr in data_list])
                data = [{k: v for k, v in zip(keys, row)} for row in chunk_iter]
                conn.execute(pd_table.table.insert(data))  # multivalues insert
