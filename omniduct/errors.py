# Connection Errors
class DuctAuthenticationError(RuntimeError):
    pass


class DuctConnectionError(RuntimeError):
    pass


class DuctServerUnreachable(RuntimeError):
    pass


# Lookups
class DuctNotFound(RuntimeError):
    pass


class DuctProtocolUnknown(RuntimeError):
    pass
