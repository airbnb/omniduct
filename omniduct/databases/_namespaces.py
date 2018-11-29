import re
from collections import OrderedDict


class ParsedNamespaces:
    """
    A namespace parser for DatabaseClient subclasses.

    This is a utility class that encodes the translation of table names in form
    "<name>.<name>...." into instances, given a particular hierarchy of
    namespaces.

    For example, if a particular database has a namespace hierarchy of
    "<database>.<table>", and a specific table name "my_db.my_table"
    was provided, an instance of this class would translate this internally
    into {'database': 'my_db', 'table': 'my_table'}; and expose them by
    <instance>.database == 'my_db' and <instance>.table == 'my_table'.
    Partially complete namespaces (such as 'my_table') will also be parsed,
    interpreting provided names as the least general, and setting the more general
    namespaces to `None` (e.g. in this case, the 'database' namespace to `None`).
    """

    @classmethod
    def from_name(cls, name, namespaces, quote_char='"', separator='.', defaults=None):
        """
        Return an instance of `ParsedNamespaces` from a given name.

        If `name` is an instance of `ParsedNamespaces`, it is checked whether
        the `namespaces` are a subset of the namespaces provided to this
        constructor. If not, a `ValueError` is raised. Note that the quote
        charactors, separators and defaults will of the passed
        `ParsedNamespaces` will be ignored.

        Args:
            name (str, ParsedNamespaces): The name to be parsed.
            namespaces (list<str>): The namespaces into which the name should be
                parsed.
            defaults (None, dict): Default values for namespaces. Note that if a
                default is provided for a namespace, it will only be used if all
                sub-namespaces also resolve to a value (either via defaults or
                by being explicitly passed).
            quote_char (str): The character to used for optional encapsulation
                of namespace names. (default='"')
            separator (str): The character used to separate namespaces.
                (default='.')

        Returns:
            ParsedNamespaces: The `ParsedNamespaces` instance representing the
                parsed value of the nominated name.
        """
        defaults = defaults or {}

        if isinstance(name, ParsedNamespaces):
            extra_namespaces = set(name.namespaces).difference(namespaces)
            if extra_namespaces:
                raise ValueError(
                    "ParsedNamespace is not encapsulated by the namespaces "
                    "provided to this constructor. It has extra namespaces: {}."
                    .format(extra_namespaces)
                )
            parsed = name.as_dict()

        elif isinstance(name, str):
            namespace_matcher = re.compile(
                r"([^{sep}{qc}]+)|{qc}([^`]*?){qc}".format(
                    qc=re.escape(quote_char),
                    sep=re.escape(separator)
                )
            )

            names = [''.join(t) for t in namespace_matcher.findall(name)] if name else []
            if len(names) > len(namespaces):
                raise ValueError(
                    "Name '{}' has too many namespaces. Should be of form: <{}>."
                    .format(name, ">{sep}<".format(sep=separator).join(namespaces))
                )

            parsed = OrderedDict(reversed([
                (namespace, names.pop() if names else None)
                for namespace in namespaces[::-1]
            ]))

        else:
            raise ValueError("Cannot construct `ParsedNamespaces` instance from "
                             "name of type: `{}`.".format(type(name)))

        for namespace in namespaces[::-1]:
            if not parsed.get(namespace) and namespace in defaults:
                parsed[namespace] = defaults[namespace]
            elif not parsed.get(namespace):
                break

        return cls(parsed, quote_char=quote_char, separator=separator)

    def __init__(self, names, namespaces=None, quote_char='"', separator='.'):
        if namespaces:
            names = OrderedDict(
                (namespace, names.get(namespace, None))
                for namespace in namespaces
            )

        self.__names = names
        self.__quote_char = quote_char
        self.__separator = separator

    def __getattr__(self, name):
        if name in self.__names:
            return self.__names[name]
        raise AttributeError(name)

    def __bool__(self):
        return bool(self.name)

    def __nonzero__(self):  # Python 2 support for bool
        return bool(self.name)

    @property
    def namespaces(self):
        """list<str> The namespaces parsed in order of most to least specific."""
        return list(self.__names)

    @property
    def name(self):
        """str: The full name provided (with quotes)."""
        return self.render()

    @property
    def parent(self):
        """ParsedNamespaces: An instance of `ParsedNamespaces` with the most specific namespace truncated."""
        names = self.__names.copy()
        names.popitem()
        return ParsedNamespaces(
            names=names,
            quote_char=self.__quote_char,
            separator=self.__separator
        )

    def as_dict(self):
        """dict: Returns the parsed namespaces as an OrderedDict from most to least general."""
        return self.__names

    def render(self, quote_char=None, separator=None):
        if quote_char is None:
            quote_char = self.__quote_char
        if separator is None:
            separator = self.__separator

        names = [
            self.__names[namespace]
            for namespace, name in self.__names.items()
            if name
        ]
        if len(names) == 0:
            return ""
        return (
            quote_char
            + "{qc}{sep}{qc}".format(qc=quote_char, sep=separator).join(names)
            + quote_char
        )

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Namespace<{}>".format(self.name)
