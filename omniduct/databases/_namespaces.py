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
    def from_name(cls, name, namespaces, quote_char='"', separator='.'):
        """
        This classmethod returns an instance of `ParsedNamespaces` which represents
        the parsed namespace corresponding to `name`.
        """
        if isinstance(name, ParsedNamespaces):
            return name
        return cls(name, namespaces, quote_char=quote_char, separator=separator)

    def __init__(self, name, namespaces, quote_char='"', separator='.'):
        """
        Parse `name` into provided `namespaces`.

        Parameters:
            name (str): The name to be parsed.
            namespaces (list<str>): The namespaces into which the name should be
                parsed.
            quote_char (str): The character to used for optional encapsulation
                of namespace names. (default='"')
            separator (str): The character used to separate namespaces.
                (default='.')
        """
        self.__name = name
        self.__namespaces = namespaces
        self.__quote_char = quote_char
        self.__separator = separator

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
                .format(name, ">{sep}<".format(sep=self.__separator).join(namespaces))
            )

        self.__dict = {
            level: names.pop() if names else None
            for level in namespaces[::-1]
        }

    def __getattr__(self, name):
        if name in self.__dict:
            return self.__dict[name]
        raise AttributeError(name)

    def __bool__(self):
        return bool(self.name)

    def __nonzero__(self):  # Python 2 support for bool
        return bool(self.name)

    @property
    def name(self):
        """The full name provided (with quotes)."""
        names = [
            self.__dict[namespace]
            for namespace in self.__namespaces
            if self.__dict.get(namespace)
        ]
        if len(names) == 0:
            return ""
        return (
            self.__quote_char
            + "{qc}.{qc}".format(qc=self.__quote_char).join(names)
            + self.__quote_char
        )

    @property
    def parent(self):
        """An instance of `ParsedNamespaces` with the most specific namespace truncated."""
        return ParsedNamespaces(
            name=self.__separator.join(self.__name.split(self.__separator)[:-1]),
            namespaces=self.__namespaces[:-1],
            quote_char=self.__quote_char,
            separator=self.__separator
        )

    def as_dict(self):
        """Returns the parsed namespaces as an OrderedDict from most to least general."""
        d = OrderedDict()
        for namespace in self.__namespaces:
            d[namespace] = self.__dict[namespace]
        return d

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Namespace<{}>".format(self.name)
