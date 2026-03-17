from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from typing import Any

from interface_meta import inherit_docs


def process_line_arguments(f: Callable[..., Any]) -> Callable[..., Any]:
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        arg_list = list(args)
        new_args, new_kwargs = _process_line_arguments(arg_list.pop(0))
        arg_list += new_args
        kwargs.update(new_kwargs)
        return f(*arg_list, **kwargs)

    return wrapped


def process_line_cell_arguments(f: Callable[..., Any]) -> Callable[..., Any]:
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        arg_list = list(args)
        new_args, new_kwargs = _process_line_arguments(arg_list.pop(0))
        if len(arg_list) == 0:
            arg_list += [None]
        arg_list += new_args
        kwargs.update(new_kwargs)
        return f(*arg_list, **kwargs)

    return wrapped


def _process_line_arguments(line_arguments: str) -> tuple[list[Any], dict[str, Any]]:
    from IPython import get_ipython

    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    reached_kwargs = False
    for arg in line_arguments.split():
        if "=" in arg:
            reached_kwargs = True
            key, value = arg.split("=")
            value = eval(value, get_ipython().user_ns)  # noqa: S307
            if key in kwargs:
                raise ValueError(f"Duplicate keyword argument `{key}`.")
            kwargs[key] = value
        else:
            if reached_kwargs:
                raise ValueError(f"Positional argument `{arg}` after keyword argument.")
            args.append(arg)
    return args, kwargs


class MagicsProvider(metaclass=ABCMeta):
    name: str

    @inherit_docs("_register_magics")
    def register_magics(self, base_name: str | None = None) -> None:
        base_name = base_name or self.name
        if base_name is None:
            raise RuntimeError("Cannot register magics without a base_name.")

        try:
            from IPython import get_ipython

            ip = get_ipython()
            if ip is None:
                raise RuntimeError("IPython kernel is not running.")
            has_ipython = True
        except Exception:
            has_ipython = False

        if has_ipython:
            self._register_magics(base_name)

    @abstractmethod
    def _register_magics(self, base_name: str) -> None:
        pass
