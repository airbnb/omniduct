from __future__ import annotations

import importlib
import pkgutil
import sys
import types


def import_submodules(package_name: str) -> dict[str, types.ModuleType]:
    """
    Import all submodules of a module, recursively.

    Args:
        package_name: The name of the package to import submodules from.
    """
    package = sys.modules[package_name]
    return {
        name: importlib.import_module(package_name + "." + name)
        for loader, name, is_pkg in pkgutil.walk_packages(package.__path__)
    }
