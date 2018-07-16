import importlib
import re

import pkg_resources
from pkg_resources import VersionConflict

from omniduct._version import __optional_dependencies__
from omniduct.utils.debug import logger


def check_dependencies(protocols, message=None):
    if protocols is None:
        return
    dependencies = []
    for protocol in protocols:
        dependencies.extend(__optional_dependencies__.get(protocol, []))
    missing_deps = []
    warning_deps = {}

    for dep in dependencies:
        m = re.match('^[a-z_][a-z0-9]*', dep)
        if not m:
            logger.warning('Invalid dependency requested: {}'.format(dep))

        package_name = m.group(0)
        accept_any_version = package_name == dep

        try:
            pkg_resources.get_distribution(dep)
        except VersionConflict:
            warning_deps[dep] = "{}=={}".format(package_name, pkg_resources.get_distribution(m.group(0)).version)
        except:
            # Some packages may be available, but not installed. If so, we
            # should accept them with warnings (if version specified in dep).
            try:
                importlib.import_module(package_name)
                if not accept_any_version:
                    warning_deps.append('{}==<not installed>'.format(package_name))
            except:  # ImportError in python 2, ModuleNotFoundError in Python 3
                missing_deps.append(dep)

    if warning_deps:
        message = "You may have some outdated packages:\n"
        for key in sorted(warning_deps):
            message += '\t- Want {}, found {}'.format(key, warning_deps[key])
        logger.warning(message)
    if missing_deps:
        message = message or "Whoops! You do not seem to have all the dependencies required."
        fix = ("You can fix this by running:\n\n"
               "\t{install_command}\n\n"
               "Note: Depending on your system's installation of Python, you may "
               "need to use `pip2` or `pip3` instead of `pip`.").format(install_command='pip install --upgrade ' + ' '.join(missing_deps))
        raise RuntimeError('\n\n'.join([message, fix]))
