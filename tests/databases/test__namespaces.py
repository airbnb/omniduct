import pytest

from omniduct.databases._namespaces import ParsedNamespaces


class TestParseNamespaces:

    def test_simple(self):
        namespace = ParsedNamespaces.from_name(
            name='my_db.my_table',
            namespaces=['database', 'table']
        )

        assert namespace.database == 'my_db'
        assert namespace.table == 'my_table'
        assert namespace.as_dict() == {
            'database': 'my_db',
            'table': 'my_table'
        }

    def test_quoted_names(self):
        namespace = ParsedNamespaces.from_name(
            name='`my_db`.`my . table`',
            namespaces=['catalog', 'database', 'table'],
            quote_char='`'
        )

        assert namespace.catalog is None
        assert namespace.database == 'my_db'
        assert namespace.table == 'my . table'
        assert namespace.as_dict() == {
            'catalog': None,
            'database': 'my_db',
            'table': 'my . table'
        }

    def test_separator(self):
        namespace = ParsedNamespaces.from_name(
            name='cat|my_db|my_table',
            namespaces=['catalog', 'database', 'table'],
            separator='|'
        )

        assert namespace.catalog == 'cat'
        assert namespace.database == 'my_db'
        assert namespace.table == 'my_table'
        assert namespace.as_dict() == {
            'catalog': 'cat',
            'database': 'my_db',
            'table': 'my_table'
        }

    def test_parsing_failure(self):
        with pytest.raises(ValueError):
            ParsedNamespaces.from_name(
                name='my_db.my_table',
                namespaces=['table']
            )

    def test_nonexistent_namespace(self):
        with pytest.raises(AttributeError):
            ParsedNamespaces.from_name(
                name='my_table',
                namespaces=['table']
            ).database

    def test_not_encapsulated(self):
        namespace = ParsedNamespaces.from_name('my_db.my_table', ['database', 'table'])
        assert namespace.as_dict() == {'database': 'my_db', 'table': 'my_table'}

        with pytest.raises(ValueError):
            ParsedNamespaces.from_name(namespace, ['schema', 'table'])

    def test_empty(self):
        namespace = ParsedNamespaces.from_name("", ['database', 'table'])

        assert bool(namespace) is False
        assert namespace.database is None
        assert namespace.table is None
        assert str(namespace) == ''
        assert repr(namespace) == 'Namespace<>'

    def test_parent(self):
        namespace = ParsedNamespaces.from_name(
            name='my_db.my_table',
            namespaces=['catalog', 'database', 'table']
        )

        assert namespace.parent.name == '"my_db"'
        assert namespace.parent.as_dict() == {
            'catalog': None,
            'database': 'my_db'
        }

    def test_casting(self):
        namespace = ParsedNamespaces.from_name(
            name='my_db.my_table',
            namespaces=['catalog', 'database', 'table']
        )

        assert str(namespace) == '"my_db"."my_table"'
        assert bool(namespace) is True
        assert namespace.__bool__() == namespace.__nonzero__()  # Python 2/3 compatibility
        assert repr(namespace) == 'Namespace<"my_db"."my_table">'
