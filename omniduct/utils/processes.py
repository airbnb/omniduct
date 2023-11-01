import os
import signal
import subprocess
from subprocess import TimeoutExpired

from omniduct.utils.config import config as omniduct_config
from omniduct.utils.debug import logger


__all__ = ["run_in_subprocess", "TimeoutExpired", "Timeout"]

DEFAULT_SUBPROCESS_CONFIG = {
    "shell": True,
    "close_fds": False,
    "stdin": None,
    "stdout": subprocess.PIPE,
    "stderr": subprocess.PIPE,
    "preexec_fn": os.setsid,  # Set the process as the group leader, so we can kill recursively
}


class SubprocessResults:
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

    with open(os.devnull, "w", encoding="utf-8") as devnull:
        logger.debug(f"Executing command: {cmd}")
        config = DEFAULT_SUBPROCESS_CONFIG.copy()
        config.update(kwargs)
        if not check_output:
            if omniduct_config.logging_level < 20:
                config["stdout"] = None
                config["stderr"] = None
            else:
                config["stdout"] = devnull
                config["stderr"] = devnull
        timeout = config.pop("timeout", None)

        with subprocess.Popen(cmd, **config) as process:
            try:
                stdout, stderr = process.communicate(None, timeout=timeout)
                returncode = process.returncode
            except subprocess.TimeoutExpired as e:
                os.killpg(
                    os.getpgid(process.pid), signal.SIGINT
                )  # send signal to the process group, recursively killing all children
                output, unused_err = process.communicate()
                raise subprocess.TimeoutExpired(
                    process.args, timeout, output=output
                ) from e
        return SubprocessResults(
            returncode=returncode, stdout=stdout or b"", stderr=stderr or b""
        )


class Timeout:
    def __init__(self, seconds=1, error_message="Timeout"):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    # pylint: disable-next=redefined-builtin
    def __exit__(self, type, value, traceback):
        signal.alarm(0)
