from interface_meta import quirk_docs

from omniduct.duct import Duct


class NoSqlClient(Duct):
    """
    A simple generic NoSQL database client, providing a minimal common API across
    NoSQL databases. Subclasses can extend this functionality either by exposing
    another python client directly, or by adding additional methods as needed.
    """

    DUCT_TYPE = Duct.Type.NOSQL

    @quirk_docs('_init')
    def __init__(self, server_protocol='http', assume_json=False, endpoint_prefix='', **kwargs):
        """
        This is a shim __init__ function that passes all arguments onto
        `self._init`, which is implemented by subclasses. This allows subclasses
        to instantiate themselves with arbitrary parameters.
        """
        Duct.__init_with_kwargs__(self, kwargs, port=80)

        self.server_protocol = server_protocol
        self.assume_json = assume_json
        self.endpoint_prefix = endpoint_prefix

        self._init(**kwargs)

    def _init(self):
        pass

    def get(self):
        pass

    def put(self):
        pass

    def find(self):
        pass
