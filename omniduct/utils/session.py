import inspect


def get_importing_modules():
    caller_frame = inspect.currentframe()
    first = True
    while caller_frame is not None:
        try:
            host = inspect.getmodule(caller_frame).__name__
            if not first:
                yield host
            else:
                first = False
        except GeneratorExit:
            return
        except:
            pass

        caller_frame = caller_frame.f_back


def is_directly_imported():
    for module in get_importing_modules():
        return module.startswith('IPython')
    return True
