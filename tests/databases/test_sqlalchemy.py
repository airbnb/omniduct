import pandas as pd
import pytest
import sqlalchemy as sa

from omniduct.databases.sqlalchemy import SQLAlchemyClient


@pytest.fixture
def db_client(mocker):
    """
    A `SQLAlchemyClient` backed by a real in-memory SQLite database, so that
    `dataframe_to_table` and friends are exercised against genuine SQL
    execution rather than mocks.
    """
    # Skip the real network reachability check performed by `Duct.connect()`,
    # since there is no real host/port for an in-memory SQLite database.
    mocker.patch("omniduct.duct.is_port_bound", return_value=True)

    # SQLite's URL scheme doesn't support host/port/username the way
    # `SQLAlchemyClient.db_uri` assumes, so build the in-memory engine
    # directly rather than going through the standard `_connect` flow.
    engine = sa.create_engine("sqlite:///:memory:")

    def _connect(self):
        self.engine = engine
        self.connection = engine.connect()

    mocker.patch.object(SQLAlchemyClient, "_connect", _connect)

    client = SQLAlchemyClient(protocol="sqlite", host="localhost", port=1)
    client.connect()
    yield client
    client.disconnect()


def _read_table(client, table):
    with client.engine.connect() as conn:
        return list(conn.execute(sa.text(f"SELECT * FROM {table}")))


class TestSQLAlchemyDataframeToTable:
    def test_create_new_table(self, db_client):
        """Uploading a dataframe to a table that does not yet exist works."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        db_client.dataframe_to_table(df, table="my_table", if_exists="fail")
        assert _read_table(db_client, "my_table") == [(1,), (2,), (3,)]

    def test_fail_if_exists(self, db_client):
        """`if_exists='fail'` raises when the table is already present.

        Note: this currently surfaces as a raw `sqlalchemy.exc.OperationalError`
        rather than a clean `RuntimeError`, which is a related symptom of the
        same underlying issue as https://github.com/airbnb/omniduct/issues/110.
        """
        df = pd.DataFrame({"a": [1, 2, 3]})
        db_client.dataframe_to_table(df, table="my_table", if_exists="fail")
        with pytest.raises(sa.exc.DBAPIError):
            db_client.dataframe_to_table(df, table="my_table", if_exists="fail")

    @pytest.mark.xfail(
        reason=(
            "Known bug: appending to an existing table raises instead of "
            "inserting the new rows. See "
            "https://github.com/airbnb/omniduct/issues/110"
        ),
        strict=True,
    )
    def test_append_to_existing_table(self, db_client):
        """`if_exists='append'` should add rows to an existing table."""
        df1 = pd.DataFrame({"a": [1, 2, 3]})
        df2 = pd.DataFrame({"a": [4, 5, 6]})
        db_client.dataframe_to_table(df1, table="my_table", if_exists="fail")
        db_client.dataframe_to_table(df2, table="my_table", if_exists="append")
        assert _read_table(db_client, "my_table") == [
            (1,),
            (2,),
            (3,),
            (4,),
            (5,),
            (6,),
        ]

    @pytest.mark.xfail(
        reason=(
            "Known bug: replacing an existing table raises instead of "
            "dropping and recreating it. See "
            "https://github.com/airbnb/omniduct/issues/110"
        ),
        strict=True,
    )
    def test_replace_existing_table(self, db_client):
        """`if_exists='replace'` should drop and recreate the table."""
        df1 = pd.DataFrame({"a": [1, 2, 3]})
        df2 = pd.DataFrame({"a": [4, 5, 6]})
        db_client.dataframe_to_table(df1, table="my_table", if_exists="fail")
        db_client.dataframe_to_table(df2, table="my_table", if_exists="replace")
        assert _read_table(db_client, "my_table") == [(4,), (5,), (6,)]