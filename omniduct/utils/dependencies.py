import re

import pkg_resources
from pkg_resources import VersionConflict

from omniduct._version import __optional_dependencies__
from omniduct.utils.debug import logger


def check_dependencies(protocols, message=None):
    dependencies = []
    for protocol in protocols:
        dependencies.extend(__optional_dependencies__.get(protocol, []))
    missing_deps = []
    warning_deps = {}
    for dep in dependencies:
        try:
            pkg_resources.get_distribution(dep)
        except VersionConflict:
            m = re.match('^[a-z_][a-z0-9]*', dep)
            if m:
                warning_deps[dep] = "{}=={}".format(m.group(0), pkg_resources.get_distribution(m.group(0)).version)
            else:
                logger.warning("Could not find distribution for '{}'.".format(m.group(0)))
        except:
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
