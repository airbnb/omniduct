import inspect
import sys

import six


def function_args_as_kwargs(func, *args, **kwargs):
    if six.PY3 and not hasattr(sys, 'pypy_version_info'):
        arguments = inspect.signature(func).parameters.keys()
    else:
        arguments = inspect.getargspec(func).args
    kwargs.update(dict(zip(list(arguments), args)))
    return kwargs
