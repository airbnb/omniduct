class DuctAuthenticationError(RuntimeError):
    pass


class DuctConnectionError(RuntimeError):
    pass


class DuctServerUnreachable(RuntimeError):
    pass


class DuctProtocolUnknown(RuntimeError):
    pass
