import inspect
import sys

import decorator
import six
from future.utils import raise_with_traceback


def function_args_as_kwargs(func, *args, **kwargs):
    if six.PY3 and not hasattr(sys, 'pypy_version_info'):
        arguments = inspect.signature(func).parameters.keys()
    else:
        arguments = inspect.getargspec(func).args
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
    except Exception as e:
        # Check to see if it is possible that we failed due to connection issues.
        # If so, try again once more. If we fail again, raise.
        # TODO: Explore adding a DuctConnectionError class and filter this
        # handling to errors of that class.
        if not self.is_connected():
            self.connect()
            return f(self, *args, **kwargs)
        raise_with_traceback(e)
