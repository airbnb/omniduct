from __future__ import annotations

import os
import re
from importlib.metadata import PackageNotFoundError, requires

__version__: str
__version_tuple__: tuple[int | str, ...]

try:
    from ._version_info import __version__, __version_tuple__
except ImportError:
    __version__ = "unknown"
    __version_tuple__ = (0, 0, 0, "+unknown")

__all__ = [
    "__author__",
    "__author_email__",
    "__version__",
    "__version_tuple__",
    "__logo__",
    "__docs_url__",
]

__author__: str = "Matthew Wardrop, Dan Frank"
__author_email__: str = "mpwardrop@gmail.com, danfrankj@gmail.com"
__logo__: str | None = (
    os.path.join(os.path.dirname(__file__), "logo.png")
    if "__file__" in globals()
    else None
)
__docs_url__: str = "https://omniduct.readthedocs.io/"


def _load_dependencies() -> tuple[list[str], dict[str, list[str]]]:
    try:
        reqs = requires("omniduct") or []
    except PackageNotFoundError:
        return [], {}

    core: list[str] = []
    optional: dict[str, list[str]] = {}

    for req_str in reqs:
        pkg, _, marker = req_str.partition(";")
        pkg = pkg.strip()
        marker = marker.strip()

        if not marker:
            core.append(pkg)
        else:
            extra_match = re.search(r'extra\s*==\s*["\']([^"\']+)["\']', marker)
            if extra_match:
                optional.setdefault(extra_match.group(1), []).append(pkg)
            else:
                # Non-extra marker (e.g. a Python-version condition) - treat as core
                core.append(req_str)

    return core, optional


__dependencies__, __optional_dependencies__ = _load_dependencies()
