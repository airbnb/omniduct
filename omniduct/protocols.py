# flake8: noqa

# Omniduct's automatic registration of Duct protocols requires that the subclass
# implementation be loaded into memory. Any protocol that should be enabled by
# default should be imported here.

from .caches.filesystem import FileSystemCache
from .databases.druid import DruidClient
from .databases.exasol import ExasolClient
from .databases.hiveserver2 import HiveServer2Client
from .databases.neo4j import Neo4jClient
from .databases.presto import PrestoClient
from .databases.pyspark import PySparkClient
from .databases.sqlalchemy import SQLAlchemyClient
from .filesystems.local import LocalFsClient
from .filesystems.s3 import S3Client
from .filesystems.webhdfs import WebHdfsClient
from .remotes.ssh import SSHClient
from .remotes.ssh_paramiko import ParamikoSSHClient
from .restful.base import RestClient
