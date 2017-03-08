import socket

from omniduct.utils.debug import logger


def is_local_port_free(local_port):
    """
    Checks if local port is free.

    Parameters
    ----------
    local_port : int
        Local port to check.

    Returns
    -------
    out : boolean
        Whether local port is free.
    """
    s = socket.socket()
    try:
        s.bind(("", local_port))
    except socket.error:
        return False
    finally:
        s.close()
    return True


def get_free_local_port():
    """
    Return a random free port

    Returns
    -------
    free_port : int
        A free local port
    """
    s = socket.socket()
    s.bind(("", 0))
    free_port = s.getsockname()[1]
    s.close()
    logger.info('found port {0}'.format(free_port))
    return free_port


def is_port_bound(hostname, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((hostname, port))
    except:
        return False
    finally:
        s.close()
    return True
