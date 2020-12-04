import logging
import time
import livy
from interface_meta import override
from livy.models import SessionKind
from omniduct.databases.base import DatabaseClient

logger = logging.getLogger(__file__)


class LivyCursor(object):
    """
    This DBAPI2 compatible cursor wraps around livy output
    """

    def __init__(self, livy_output):
        self.out = livy_output

    def close(self):
        pass

    arraysize = 1

    def fetchall(self):
        return [tuple(row) for row in self.out.json["data"]]

    @property
    def description(self):
        return tuple(
            [
                (desc["name"], desc["type"], None, None, None, None, None)
                for desc in self.out.json["schema"]["fields"]
            ]
        )


class LivyClient(DatabaseClient):

    PROTOCOLS = ["livy"]
    DEFAULT_PORT = 8998

    def _init(self, database=None):
        self._livy_session = None
        self._database = database
        self._session = None

    # Connection management

    @override
    def _connect(self):
        start = time.time()
        logger.info("Acquiring Livy session... this may take a few seconds")
        self._session = livy.LivySession.create(
            f"http://{self.host}:{self.port}", kind=SessionKind.SQL
        )
        self._session.wait()  # ensure livy is ready to accept statements
        logger.info(f"Acquired Livy session in {time.time() - start:.2f} seconds")

    @override
    def _is_connected(self):
        return self._session is not None

    @override
    def _disconnect(self):
        if self._session is not None:
            self._session.close()
        self._session = None

    # Database operations
    @override
    def _execute(self, statement, cursor, wait, session_properties, **kwargs):
        out = self._session._execute(statement)
        if out.status.value != "ok":
            raise Exception(f"Livy query failed: {out.ename} {out.evalue}")
        return LivyCursor(out)

    @override
    def _table_list(self, namespace, **kwargs):
        raise NotImplementedError

    @override
    def _table_exists(self, table, **kwargs):
        raise NotImplementedError

    @override
    def _table_desc(self, table, **kwargs):
        raise NotImplementedError

    @override
    def _table_head(self, table, n=10, **kwargs):
        raise NotImplementedError

    @override
    def _table_props(self, table, **kwargs):
        raise NotImplementedError

    @override
    def _table_drop(self, table, **kwargs):
        raise NotImplementedError
