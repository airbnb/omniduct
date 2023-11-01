import inspect

import decorator


def function_args_as_kwargs(func, *args, **kwargs):
    arguments = inspect.signature(func).parameters.keys()
    kwargs.update(dict(zip(list(arguments), args)))
    return kwargs


@decorator.decorator
def require_connection(f, self, *args, **kwargs):
    """
    A wrapper to allow restoring of connection status in the event that
    connection issues result in failures. If so, we will attempt to retry the
    failed function call once more.
    """
    if not self._Duct__connected:
        self.connect()

    try:
        return f(self, *args, **kwargs)
    except Exception:  # pylint: disable=broad-exception-caught
        # Check to see if it is possible that we failed due to connection issues.
        # If so, try again once more. If we fail again, raise.
        # TODO: Explore adding a DuctConnectionError class and filter this
        # handling to errors of that class.
        if not self.is_connected():
            self.connect()
            return f(self, *args, **kwargs)
        raise
