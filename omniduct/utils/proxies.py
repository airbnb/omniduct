from wrapt import ObjectProxy


class NestedDictObjectProxy(ObjectProxy):

    @classmethod
    def _from_tree(cls, tree, is_flat=False, get_nesting=None):
        if None in tree and len(tree) == 1:
            return tree[None]
        return cls(tree, is_flat=is_flat, get_nesting=get_nesting)

    def __init__(self, tree, is_flat=False, get_nesting=None):
        ObjectProxy.__init__(self, tree.get(None))
        self._self_tree = tree
        self._self_tree_is_flat = is_flat
        self._self_tree_get_nesting = get_nesting

    @property
    def __tree__(self):
        def add_nested(tree, key, value):
            if self._self_tree_get_nesting is not None:
                key = self._self_tree_get_nesting(key, value)
            else:
                key = key.split('/')
            for k in key:
                if k not in tree:
                    tree[k] = {}
                tree = tree[k]
            tree[None] = value

        def flat_to_nested(tree):
            out = {}
            for k, v in tree.items():
                add_nested(out, k, v)
            return out

        tree = self._self_tree
        if self._self_tree_is_flat:
            tree = flat_to_nested(tree)

        return tree

    def __dir__(self):
        return [l for l in list(self.__tree__) + ObjectProxy.__dir__(self) if l is not None]

    def __getattr__(self, name):
        if name in self.__tree__:
            if not isinstance(self.__tree__[name], NestedDictObjectProxy):
                return NestedDictObjectProxy._from_tree(self.__tree__[name])
            return self.__tree__[name]
        if None in self.__tree__:
            return getattr(self.__tree__[None], name)
        raise AttributeError('Invalid child node.'.format(name))

    def __repr__(self):
        if None in self.__tree__:
            return self.__wrapped__.__repr__()
        return ObjectProxy.__repr__(self)
