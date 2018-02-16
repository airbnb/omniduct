def quirk_docs(method):
    """
    Use this decorator to wrap methods which should inherit documentation from
    protocol specific implementations of methods. For example, if the protocol specific
    implementation of `.connect()` is implemented in `._connect`, you can
    decorate the connect method with `omniduct.utils.documentation.quirk_docs`
    using `@quirk_docs('_connect')`, the the documentation from the `_connect`
    method will be appended to the `connect` docs under a heading "<cls> Quirks:".
    """
    def doc_wrapper(f):
        f._quirks_method = method
        if not hasattr(f, '__doc_orig__'):
            f.__doc_orig__ = f.__doc__
        return f
    return doc_wrapper
