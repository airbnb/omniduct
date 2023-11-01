import json
from urllib.parse import urljoin

from interface_meta import quirk_docs, override

from omniduct.duct import Duct
from omniduct.utils.decorators import require_connection


class RestClientBase(Duct):
    """
    A simple wrapper around the `requests` library to simplify the creation of
    REST clients.

    This allows all the automatic features around port forwarding from remote
    hosts to be inherited. This client can be used directly, or inherited by
    subclasses which can add methods specific to any REST service; and
    internally use `request` and `request_json` methods to access various
    endpoints.

    Attributes:
        server_protocol (str): The protocol to use when connecting to the
            remote host (default: `'http'`).
        assume_json (bool): Assume that responses will be JSON
            (default: `False`).
        endpoint_prefix (str): The base_url path relative to the host at
            which the API is accessible (default: `''`).
    """

    DUCT_TYPE = Duct.Type.RESTFUL

    @quirk_docs("_init", mro=True)
    def __init__(  # pylint: disable=super-init-not-called
        self,
        server_protocol="http",
        assume_json=False,
        endpoint_prefix="",
        default_timeout=None,
        **kwargs,
    ):
        """
        Args:
            server_protocol (str): The protocol to use when connecting to the
                remote host (default: `'http'`).
            assume_json (bool): Assume that responses will be JSON when calling
                instances of this class (default: `False`).
            endpoint_prefix (str): The base_url path relative to the host at
                which the API is accessible (default: `''`).
            default_timeout (optional float): The number of seconds to wait for
                a response. Will be used except where overridden by specific
                requests.
            **kwargs (dict): Additional keyword arguments passed on to
                subclasses.
        """
        Duct.__init_with_kwargs__(self, kwargs, port=80)

        self.server_protocol = server_protocol
        self.assume_json = assume_json
        self.endpoint_prefix = endpoint_prefix
        self.default_timeout = default_timeout

        self._init(**kwargs)

    def _init(self):
        pass

    def __call__(self, endpoint, method="get", **kwargs):
        if self.assume_json:
            return self.request_json(endpoint, method=method, **kwargs)
        return self.request(endpoint, method=method, **kwargs)

    @property
    def base_url(self):
        """str: The base url of the REST API."""
        url = urljoin(
            f"{self.server_protocol}://{self.host}:{self.port or 80}",
            self.endpoint_prefix,
        )
        if not url.endswith("/"):
            url += "/"
        return url

    @require_connection
    def request(self, endpoint, method="get", **kwargs):
        """
        Request data from a nominated endpoint.

        Args:
            endpoint (str): The endpoint from which to receive data.
            method (str): The method to use when requestion this resource.
            **kwargs (dict): Additional arguments to pass through to
                `requests.request`.

        Returns:
            requests.Response: The response object associated with this request.
        """
        import requests

        url = urljoin(self.base_url, endpoint)
        return requests.request(
            method, url, **{"timeout": self.default_timeout, **kwargs}
        )

    def request_json(self, endpoint, method="get", **kwargs):
        """
        Request JSON data from a nominated endpoint.

        Args:
            endpoint (str): The endpoint from which to receive data.
            method (str): The method to use when requestion this resource.
            **kwargs (dict): Additional arguments to pass through to
                `requests.request`.

        Returns:
            list, dict: The representation of the JSON response from the server.
        """
        request = self.request(endpoint, method=method, **kwargs)
        if not request.status_code == 200:
            try:
                raise RuntimeError(
                    f"Server responded with HTTP response code {request.status_code}, with content: {json.dumps(request.json())}."
                )
            except Exception as e:  # pylint: disable=broad-exception-caught
                raise RuntimeError(
                    f"Server responded with HTTP response code {request.status_code}, with content: {request.content.decode('utf-8')}."
                ) from e
        return request.json()

    @override
    def _connect(self):
        pass

    @override
    def _is_connected(self):
        return True

    @override
    def _disconnect(self):
        pass


class RestClient(RestClientBase):
    """
    A trivial implementation of `RestClientBase` for basic REST access.
    """

    PROTOCOLS = ["rest"]
