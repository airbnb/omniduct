# <img src="https://raw.githubusercontent.com/airbnb/omniduct/master/omniduct/logo.png" height=200> Omniduct
[![PyPI - Version](https://img.shields.io/pypi/v/omniduct.svg)](https://pypi.org/project/omniduct/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/omniduct.svg)
![PyPI - Status](https://img.shields.io/pypi/status/omniduct.svg)
[![Build Status](https://travis-ci.org/airbnb/omniduct.svg?branch=master)](https://travis-ci.org/airbnb/omniduct)
[![Documentation Status](https://readthedocs.org/projects/omniduct/badge/?version=latest)](http://omniduct.readthedocs.io/en/latest/?badge=latest)


`omniduct` provides uniform interfaces for connecting to and extracting data from a wide variety of (potentially remote) data stores (including HDFS, Hive, Presto, MySQL, etc).

- **Documentation:** http://omniduct.readthedocs.io
- **Source:** https://github.com/airbnb/omniduct
- **Bug reports:** https://github.com/airbnb/omniduct/issues

It provides:

- A generic plugin-based programmatic API to access data in a consistent manner across different services (see [supported protocols](http://omniduct.readthedocs.io/en/latest/protocols.html)).
- A framework for lazily connecting to data sources and maintaining these connections during the entire lifetime of the relevant Python session.
- Automatic port forwarding of remote services over SSH where connections cannot be made directly.
- Convenient IPython magic functions for interfacing with data providers from within IPython and Jupyter Notebook sessions.
- Utility classes and methods to assist in maintaining registries of useful services.

**Note:** Omniduct 1.1.x is the last version series to support Python 2. Going
forward it will support Python 3.6+.
