from abc import ABCMeta, abstractmethod

from interface_meta import quirk_docs


def process_line_arguments(f):
    def wrapped(*args, **kwargs):
        args = list(args)
        new_args, new_kwargs = _process_line_arguments(args.pop(0))
        args += new_args
        kwargs.update(new_kwargs)
        return f(*args, **kwargs)

    return wrapped


def process_line_cell_arguments(f):
    def wrapped(*args, **kwargs):
        args = list(args)
        new_args, new_kwargs = _process_line_arguments(args.pop(0))
        if len(args) == 0:
            args += [None]
        args += new_args
        kwargs.update(new_kwargs)
        return f(*args, **kwargs)

    return wrapped


def _process_line_arguments(line_arguments):
    from IPython import get_ipython

    args = []
    kwargs = {}
    reached_kwargs = False
    for arg in line_arguments.split():
        if "=" in arg:
            reached_kwargs = True
            key, value = arg.split("=")
            value = eval(value, get_ipython().user_ns)
            if key in kwargs:
                raise ValueError(f"Duplicate keyword argument `{key}`.")
            kwargs[key] = value
        else:
            if reached_kwargs:
                raise ValueError(f"Positional argument `{arg}` after keyword argument.")
            args.append(arg)
    return args, kwargs


class MagicsProvider(metaclass=ABCMeta):
    @quirk_docs("_register_magics")
    def register_magics(self, base_name=None):
        base_name = base_name or self.name
        if base_name is None:
            raise RuntimeError("Cannot register magics without a base_name.")

        try:
            from IPython import get_ipython

            ip = get_ipython()
            assert ip is not None
            has_ipython = True
        except Exception:  # pylint: disable=broad-exception-caught
            has_ipython = False

        if has_ipython:
            self._register_magics(base_name)

    @abstractmethod
    def _register_magics(self, base_name):
        pass
