import importlib
import re
from typing import Optional

import packaging.requirements

from omniduct._version import __optional_dependencies__
from omniduct.utils.debug import logger


def get_package_version(package_name: str) -> Optional[str]:
    """
    Return the version of the given package, or None if the package is not
    installed.
    """
    try:  # Python 3.8+
        import importlib.metadata

        return importlib.metadata.version(package_name)
    except ImportError:  # Python <3.12
        import pkg_resources

        return pkg_resources.get_distribution(package_name).version


def check_dependencies(protocols, message=None):
    if protocols is None:
        return
    dependencies = []
    for protocol in protocols:
        dependencies.extend(__optional_dependencies__.get(protocol, []))
    missing_deps = []
    warning_deps = {}

    for dep in dependencies:
        m = re.match("^[a-z_][a-z0-9]*", dep)
        if not m:
            logger.warning(f"Invalid dependency requested: {dep}")

        package_name = m.group(0)
        accept_any_version = package_name == dep

        dep_req = packaging.requirements.Requirement(dep)
        package_name = dep_req.name
        package_version = get_package_version(package_name)

        if package_version is None:
            # Some packages may be available, but not installed. If so, we
            # should accept them with warnings (if version specified in dep).
            try:
                importlib.import_module(package_name)
                if not accept_any_version:
                    warning_deps[dep] = f"{package_name}==<not installed>"
            except ModuleNotFoundError:
                missing_deps.append(dep)
        elif dep_req.specifier and not dep_req.specifier.contains(package_version):
            warning_deps[dep] = f"{package_name}=={package_version}"

    if warning_deps:
        message = "You may have some outdated packages:\n"
        for key in sorted(warning_deps):
            message += f"\t- Want {key}, found {warning_deps[key]}"
        logger.warning(message)
    if missing_deps:
        message = (
            message or "Whoops! You do not seem to have all the dependencies required."
        )
        fix = (
            "You can fix this by running:\n\n"
            f"\tpip install --upgrade {' '.join(missing_deps)}\n\n"
            "Note: Depending on your system's installation of Python, you may "
            "need to use `pip3` instead of `pip`."
        )
        raise RuntimeError("\n\n".join([message, fix]))
