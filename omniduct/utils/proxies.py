class TreeProxy:
    """
    A read-only proxy object for a dictionary tree structure that allows accessing
    of keys via attributes and indexing.

    Once parsed using the `_for_dict` constructor, or if used directly using the
    `_for_tree` constructor, trees should have form:

    ```
    {'key': {'nested': {None: <object>}}}
    ```
    """

    __slots__ = ('__tree__', '__nodename__')

    @classmethod
    def _for_dict(cls, dct, key_parser=None, name=None):
        return cls._for_tree(cls.__dict_to_tree(dct, key_parser=key_parser), name=name)

    @classmethod
    def _for_tree(cls, tree, name=None):
        if None in tree:
            return tree[None]
        return cls(tree, name=name)

    def __init__(self, tree, name=None):
        self.__tree__ = tree
        self.__nodename__ = str(name) if name else None

    def __getitem__(self, name):
        if name in self.__tree__:
            if not isinstance(self.__tree__[name], TreeProxy):
                return TreeProxy._for_tree(self.__tree__[name], name=self.__name_of_child(name))
            return self.__tree__[name]
        raise KeyError('Invalid child node `{node_name}`.'.format(node_name=name))

    def __iter__(self):
        return iter(self.__tree__)

    def __len__(self):
        return len(self.__tree__)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError('Invalid child node `{node_name}`.'.format(node_name=name))

    def __dir__(self):
        return list(self.__tree__)

    def __repr__(self):
        if self.__nodename__:
            return "<TreeProxy of '{}' with {} nodes>".format(self.__nodename__, len(self.__tree__))
        return "<TreeProxy of dictionary with {} nodes>".format(len(self.__tree__))

    # Helpers

    def __name_of_child(self, child):
        if self.__nodename__:
            return ".".join([self.__nodename__, str(child)])
        return str(child)

    @classmethod
    def __dict_to_tree(cls, dct, key_parser):
        tree = {}
        for key, value in dct.items():
            cls.__add_nested_key_value(tree, keys=key_parser(key, value) if key_parser else [key], value=value)
        return tree

    @classmethod
    def __add_nested_key_value(cls, tree, keys, value):
        for key in keys:
            if key not in tree:
                tree[key] = {}
            tree = tree[key]
        if len(tree) and None not in tree:
            raise ValueError(
                "`TreeProxy` objects can only proxy trees with values only on leaf "
                "nodes; error encounted while trying to add value to node {}."
                .format(keys)
            )
        tree[None] = value
