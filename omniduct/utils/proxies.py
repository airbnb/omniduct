from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any


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

    __slots__ = ("__tree__", "__nodename__")

    @classmethod
    def _for_dict(
        cls,
        dct: dict[str, Any],
        key_parser: Callable[[str, Any], list[str]] | None = None,
        name: str | None = None,
    ) -> TreeProxy | Any:
        return cls._for_tree(cls.__dict_to_tree(dct, key_parser=key_parser), name=name)

    @classmethod
    def _for_tree(
        cls, tree: dict[Any, Any], name: str | None = None
    ) -> TreeProxy | Any:
        if None in tree:
            return tree[None]
        return cls(tree, name=name)

    def __init__(self, tree: dict[Any, Any], name: str | None = None) -> None:
        self.__tree__ = tree
        self.__nodename__ = str(name) if name else None

    def __getitem__(self, name: str) -> TreeProxy | Any:
        if name in self.__tree__:
            if not isinstance(self.__tree__[name], TreeProxy):
                return TreeProxy._for_tree(
                    self.__tree__[name], name=self.__name_of_child(name)
                )
            return self.__tree__[name]
        raise KeyError(f"Invalid child node `{name}`.")

    def __iter__(self) -> Iterator[str]:
        return iter(self.__tree__)

    def __len__(self) -> int:
        return len(self.__tree__)

    def __getattr__(self, name: str) -> TreeProxy | Any:
        try:
            return self[name]
        except KeyError as e:
            raise AttributeError(f"Invalid child node `{name}`.") from e

    def __dir__(self) -> list[str]:
        return list(self.__tree__)

    def __repr__(self) -> str:
        if self.__nodename__:
            return (
                f"<TreeProxy of '{self.__nodename__}' with {len(self.__tree__)} nodes>"
            )
        return f"<TreeProxy of dictionary with {len(self.__tree__)} nodes>"

    # Helpers

    def __name_of_child(self, child: str) -> str:
        if self.__nodename__:
            return ".".join([self.__nodename__, str(child)])
        return str(child)

    @classmethod
    def __dict_to_tree(
        cls,
        dct: dict[str, Any],
        key_parser: Callable[[str, Any], list[str]] | None,
    ) -> dict[Any, Any]:
        tree: dict[Any, Any] = {}
        for key, value in dct.items():
            cls.__add_nested_key_value(
                tree, keys=key_parser(key, value) if key_parser else [key], value=value
            )
        return tree

    @classmethod
    def __add_nested_key_value(
        cls, tree: dict[Any, Any], keys: list[str], value: Any
    ) -> None:
        for key in keys:
            if key not in tree:
                tree[key] = {}
            tree = tree[key]
        if len(tree) and None not in tree:
            raise ValueError(
                f"`TreeProxy` objects can only proxy trees with values only on leaf nodes; error encounted while trying to add value to node {keys}."
            )
        tree[None] = value
