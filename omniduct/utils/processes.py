import os
import signal
import sys

from omniduct.utils.config import config as omniduct_config
from omniduct.utils.debug import logger

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
    from subprocess32 import TimeoutExpired
else:
    import subprocess
    from subprocess import TimeoutExpired

__all__ = ['run_in_subprocess', 'TimeoutExpired', 'Timeout', 'TimeoutError']

DEFAULT_SUBPROCESS_CONFIG = {
    'shell': True,
    'close_fds': False,
    'stdin': None,
    'stdout': subprocess.PIPE,
    'stderr': subprocess.PIPE,
    'preexec_fn': os.setsid  # Set the process as the group leader, so we can kill recursively
}


class SubprocessResults(object):

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def run_in_subprocess(cmd, check_output=False, **kwargs):
    """
    Execute command using default subprocess configuration.

    Parameters
    ----------
    cmd : string
        Command to be executed in subprocess.
    kwargs : keywords
        Options to pass to subprocess.Popen.

    Returns
    -------
    proc : Popen subprocess
        Subprocess used to run command.
    """

    logger.debug('Executing command: {0}'.format(cmd))
    config = DEFAULT_SUBPROCESS_CONFIG.copy()
    config.update(kwargs)
    if not check_output:
        if omniduct_config.logging_level < 20:
            config['stdout'] = None
            config['stderr'] = None
        else:
            config['stdout'] = open(os.devnull, 'w')
            config['stderr'] = open(os.devnull, 'w')
    timeout = config.pop('timeout', None)

    process = subprocess.Popen(cmd, **config)
    try:
        stdout, stderr = process.communicate(None, timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(process.pid), signal.SIGINT)  # send signal to the process group, recursively killing all children
        output, unused_err = process.communicate()
        raise subprocess.TimeoutExpired(process.args, timeout, output=output)
    return SubprocessResults(returncode=process.returncode, stdout=stdout or b'', stderr=stderr or b'')


class TimeoutError(Exception):
    pass


class Timeout(object):

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)
