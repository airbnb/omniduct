from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

import decorator


def function_args_as_kwargs(
    func: Callable[..., Any], *args: Any, **kwargs: Any
) -> dict[str, Any]:
    arguments = inspect.signature(func).parameters.keys()
    kwargs.update(dict(zip(list(arguments), args)))
    return kwargs


@decorator.decorator
def require_connection(
    f: Callable[..., Any], self: Any, *args: Any, **kwargs: Any
) -> Any:
    """
    A wrapper to allow restoring of connection status in the event that
    connection issues result in failures. If so, we will attempt to retry the
    failed function call once more.
    """
    if not self._Duct__connected:
        self.connect()

    try:
        return f(self, *args, **kwargs)
    except Exception:
        # Check to see if it is possible that we failed due to connection issues.
        # If so, try again once more. If we fail again, raise.
        # TODO: Explore adding a DuctConnectionError class and filter this
        # handling to errors of that class.
        if not self.is_connected():
            self.connect()
            return f(self, *args, **kwargs)
        raise
