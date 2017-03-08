# Omniduct

`omniduct` is a Python 2/3 package that provides a uniform interface for connecting to and extracting data from a wide variety of (potentially remote) data stores (including HDFS, Hive, Presto, MySQL, etc). It is especially useful in contexts where the data stores are only available via remote gateway nodes, where `omniduct` can automatically manage port forwarding over SSH to make these data stores available locally. It also provides convenient magic functions for use in IPython and Jupyter Notebooks.

`omniduct` has been extensively tested internally, but until our 1.0.0 release, we offer no guarantee of API stability.

Documentation for both users and developers will be arriving shortly, but the code is currently being offered for early adopters.
