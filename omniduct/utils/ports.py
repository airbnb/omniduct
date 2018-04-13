import random
import re
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


def is_port_bound(hostname, port, timeout=None):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if timeout:
        s.settimeout(timeout)
    try:
        s.connect((hostname, port))
    except:
        return False
    finally:
        s.close()
    return True


# Random hosts for ssh gateway nodes
def naive_load_balancer(hosts, port):
    # Shuffle hosts randomly
    hosts = hosts.copy()
    random.shuffle(hosts)

    # Check if host is available and if so return it
    pattern = re.compile(r'(?P<host>[^\:]+)(?::(?P<port>[0-9]{1,5}))?')
    for host in hosts:
        m = pattern.match(host)
        if is_port_bound(m.group('host'), int(m.group('port') or port), timeout=1):
            return host
        else:
            logger.warning("Avoiding down or inaccessible host: '{}'.".format(host))

    raise RuntimeError(
        "Unable to connect to any of the hosts associated with this service. "
        "This may be due to networking issues, such as not being connected to "
        "the internet or your company's VPN.".format(host)
    )
