import os
import sys

__all__ = ['__author__', '__author_email__', '__version__']

__author__ = "Matthew Wardrop, Dan Frank"
__author_email__ = "matthew.wardrop@airbnb.com, dan.frank@airbnb.com"
__version__ = "0.9.3"


# These are the core dependencies, and should not include those which are used only in handling specific protocols.
# Order matters since installation happens from the end of the list
__dependencies__ = [
    "future",  # Python 2/3 support
    "six",  # Python 2/3 support
    "enum34",  # Python 3.4+ style enums in older versions of python

    "pyyaml",  # YAML configuration parsing
    "decorator",  # Decorators used by caching and documentation routines
    "progressbar2>=3.30.0",  # Support for progressbars in logging routines
    "wrapt",  # Object proxying for conveniently exposing ducts in registry

    # Database querying libraries
    "jinja2",  # Templating support in databases
    "pandas>=0.17.1",  # Various results including database queries are returned as pandas dataframes
    "sqlparse",  # Neatening of SQL based queries (mainly to avoid missing the cache)
    "sqlalchemy",  # Various integration endpoints in the database stack
]

PY2 = sys.version_info[0] == 2
if os.name == 'posix' and PY2:
    __dependencies__.append('subprocess32')  # Python 3.2+ subprocess handling for Python 2

__optional_dependencies__ = {
    # Databases
    'druid': [
        'pydruid>=0.4.0',  # Primary client
    ],

    'hiveserver2': [
        'pyhive[hive]>=0.4',  # Primary client
        'impyla>=0.14.0',  # Primary client
        'thrift>=0.10.0',  # Thrift dependency which seems not to be installed with upstream deps
    ],

    'presto': [
        'pyhive[presto]>=0.4',  # Primary client
        'werkzeug',  # Schema traversal
    ],

    'pyspark': [
        'pyspark',  # Primary client
    ],

    'snowflake': [
        'snowflake-sqlalchemy',
    ],

    # Filesystems
    'webhdfs': [
        'pywebhdfs',  # Primary client
        'requests',  # For rerouting redirect queries to our port-forwarded services
    ],

    's3': [
        'boto3',  # AWS client library
    ],

    # Remotes
    'ssh': [
        'pexpect',  # Command line handling (including smartcard activation)
    ],

    'ssh_paramiko': [
        'paramiko',  # Primary client
        'pexpect',  # Command line handling (including smartcard activation)
    ],

    # Rest clients
    'rest': [
        'requests',  # Library to handle underlying REST queries
    ],

    'test': [
        'nose',      # test runner
        'mock',      # mocking
        'pyfakefs',  # mock filesystem
        'coverage',  # test coverage monitoring
        'flake8'     # Code linting
    ]
}
__optional_dependencies__['all'] = [dep for deps in __optional_dependencies__.values() for dep in deps]
