import os
import sys

__all__ = ['__author__', '__author_email__', '__version__', '__logo__', '__docs_url__']

__author__ = "Matthew Wardrop, Dan Frank"
__author_email__ = "mpwardrop@gmail.com, danfrankj@gmail.com"
__version__ = "1.1.19"
__logo__ = os.path.join(os.path.dirname(__file__), 'logo.png') if '__file__' in globals() else None
__docs_url__ = "https://omniduct.readthedocs.io/"


# These are the core dependencies, and should not include those which are used only in handling specific protocols.
# Order matters since installation happens from the end of the list
__dependencies__ = [
    "future",  # Python 2/3 support
    "six",  # Python 2/3 support

    "interface_meta>=1.1.0,<2",  # Metaclass for creating an extensible well-documented architecture
    "pyyaml",  # YAML configuration parsing
    "decorator",  # Decorators used by caching and documentation routines
    "progressbar2>=3.30.0",  # Support for progressbars in logging routines
    "wrapt",  # Object proxying for conveniently exposing ducts in registry

    # Database querying libraries
    "jinja2",  # Templating support in databases
    "pandas>=0.17.1",  # Various results including database queries are returned as pandas dataframes
    "sqlparse",  # Neatening of SQL based queries (mainly to avoid missing the cache)
    "sqlalchemy",  # Various integration endpoints in the database stack

    # Utility libraries
    "python-dateutil",  # Used for its `relativedelta` class for Cache instances
    "lazy-object-proxy",  # Schema traversal
]
if sys.version_info.major < 3 or sys.version_info.major == 3 and sys.version_info.minor < 4:
    __dependencies__.append("enum34")  # Python 3.4+ style enums in older versions of python

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
        'thrift>=0.10.0',  # Thrift dependency which seems not to be installed with upstream deps
    ],

    'presto': [
        'pyhive[presto]>=0.4',  # Primary client
    ],

    'pyspark': [
        'pyspark',  # Primary client
    ],

    'snowflake': [
        'snowflake-sqlalchemy',
    ],

    'exasol': ['pyexasol'] if sys.version_info.major > 2 else [],

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

    # Documentation requirements
    'docs': [
        'sphinx',  # The documentation engine
        'sphinx_autobuild',  # A Sphinx plugin used during development of docs
        'sphinx_rtd_theme',  # The Spinx theme used by the docs
    ],

    'test': [
        'nose',      # test runner
        'mock',      # mocking
        'pyfakefs',  # mock filesystem
        'coverage',  # test coverage monitoring
        'flake8',    # Code linting
    ]
}
__optional_dependencies__['all'] = [dep for deps in __optional_dependencies__.values() for dep in deps]
