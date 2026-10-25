import pandas as pd
import pytest

from omniduct.databases.sqlalchemy import SQLAlchemyClient


class TestSQLAlchemySchema:
    @pytest.fixture
    def db_client(self, mocker):
        # Use SQLite without a network connection or SQLAlchemyClient's URL format.
        mocker.patch("omniduct.duct.is_port_bound", return_value=True)
        mocker.patch.object(
            SQLAlchemyClient,
            "db_uri",
            new_callable=mocker.PropertyMock,
            return_value="sqlite:///:memory:",
        )
        client = SQLAlchemyClient(
            protocol="sqlalchemy", dialect="sqlite", host="localhost", port=1
        )
        client.connect()
        engine = client.engine
        try:
            with engine.begin() as connection:
                connection.exec_driver_sql("ATTACH DATABASE ':memory:' AS other")
            yield client
        finally:
            client.disconnect()
            engine.dispose()

    @pytest.mark.parametrize(
        "database, table, schema",
        [
            ("", "items", None),
            ("", "other.items", "other"),
            ("other", "items", "other"),
            ("other", "main.items", "main"),
        ],
    )
    @pytest.mark.parametrize(
        "if_exists, expected",
        [("append", [1, 2, 3]), ("replace", [3]), ("fail", [1, 2])],
    )
    def test_dataframe_to_table_schema(
        self, db_client, database, table, schema, if_exists, expected
    ):
        db_client.database = database
        db_client.dataframe_to_table(pd.DataFrame({"value": [1, 2]}), table)
        df = pd.DataFrame({"value": [3]})

        if if_exists == "fail":
            with pytest.raises(ValueError, match="already exists"):
                db_client.dataframe_to_table(df, table, if_exists=if_exists)
        else:
            db_client.dataframe_to_table(df, table, if_exists=if_exists)

        result = pd.read_sql_table("items", db_client.engine, schema=schema)
        assert sorted(result["value"]) == expected
