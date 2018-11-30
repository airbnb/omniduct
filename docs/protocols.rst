Supported protocols
===================

The currently supported protocols are listed below. The string inside
the square brackets after the protocol name (if present) indicates that support
for this protocol requires external packages which are not hard-dependencies of
`omniduct`. To install them with omniduct, simply add these strings to the list
of desired extras as indicated in :doc:`installation`.

- Databases
    - Druid [druid]
    - HiveServer2 [hiveserver2]
    - Neo4j (experimental)
    - Presto [presto]
    - PySpark [pyspark]
    - Any SQL database supported by SQL Alchemy (e.g. MySQL, Postgres, Oracle, etc) [sqlalchemy]
- Filesystems
    - HDFS [webhdfs]
    - S3 [s3]
    - Local filesystem
- Remotes (also act as filesystems)
    - SSH servers, via CLI backend [ssh] or via Paramiko backend [ssh_paramiko]
- REST Services (generic interface)

Adding support for new protocols is straightforward. If your favourite protocol
is missing, feel free to contact us for help writing a patch to support it.

Within each class of protocol (database, filesystem, etc), a certain
subset of functionality is guaranteed to be consistent across protocols, making
them largely interchangeable programmatically. The common API for each
protocol class is documented in the :doc:`api/overview` section, along with any
exceptions, caveats and extensions for each implementation.
