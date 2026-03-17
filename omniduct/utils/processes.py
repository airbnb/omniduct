from __future__ import annotations

import os
import signal
import subprocess
from subprocess import TimeoutExpired
from types import FrameType
from typing import Any

from omniduct.utils.config import config as omniduct_config
from omniduct.utils.debug import logger

__all__ = ["run_in_subprocess", "TimeoutExpired", "Timeout"]

DEFAULT_SUBPROCESS_CONFIG: dict[str, Any] = {
    "shell": True,
    "close_fds": False,
    "stdin": None,
    "stdout": subprocess.PIPE,
    "stderr": subprocess.PIPE,
    "preexec_fn": os.setsid,  # Set the process as the group leader, so we can kill recursively
}


class SubprocessResults:
    returncode: int
    stdout: bytes
    stderr: bytes

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


def run_in_subprocess(
    cmd: str, check_output: bool = False, **kwargs: Any
) -> SubprocessResults:
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
    seconds: int
    error_message: str

    def __init__(self, seconds: int = 1, error_message: str = "Timeout") -> None:
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum: int, frame: FrameType | None) -> None:
        raise TimeoutError(self.error_message)

    def __enter__(self) -> Timeout:
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        signal.alarm(0)
