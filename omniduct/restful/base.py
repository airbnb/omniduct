from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

from interface_meta import inherit_docs, override

from omniduct.duct import Duct
from omniduct.utils.decorators import require_connection

if TYPE_CHECKING:
    import requests as requests_lib


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

    server_protocol: str
    assume_json: bool
    endpoint_prefix: str
    default_timeout: float | None

    @inherit_docs("_init", mro=True)
    def __init__(
        self,
        server_protocol: str = "http",
        assume_json: bool = False,
        endpoint_prefix: str = "",
        default_timeout: float | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            server_protocol: The protocol to use when connecting to the
                remote host (default: `'http'`).
            assume_json: Assume that responses will be JSON when calling
                instances of this class (default: `False`).
            endpoint_prefix: The base_url path relative to the host at
                which the API is accessible (default: `''`).
            default_timeout: The number of seconds to wait for
                a response. Will be used except where overridden by specific
                requests.
            **kwargs: Additional keyword arguments passed on to
                subclasses.
        """
        Duct.__init_with_kwargs__(self, kwargs, port=80)

        self.server_protocol = server_protocol
        self.assume_json = assume_json
        self.endpoint_prefix = endpoint_prefix
        self.default_timeout = default_timeout

        self._init(**kwargs)

    def _init(self) -> None:
        pass

    def __call__(self, endpoint: str, method: str = "get", **kwargs: Any) -> Any:
        if self.assume_json:
            return self.request_json(endpoint, method=method, **kwargs)
        return self.request(endpoint, method=method, **kwargs)

    @property
    def base_url(self) -> str:
        """str: The base url of the REST API."""
        url = urljoin(
            f"{self.server_protocol}://{self.host}:{self.port or 80}",
            self.endpoint_prefix,
        )
        if not url.endswith("/"):
            url += "/"
        return url

    @require_connection
    def request(
        self, endpoint: str, method: str = "get", **kwargs: Any
    ) -> requests_lib.Response:
        """
        Request data from a nominated endpoint.

        Args:
            endpoint: The endpoint from which to receive data.
            method: The method to use when requesting this resource.
            **kwargs: Additional arguments to pass through to
                `requests.request`.

        Returns:
            The response object associated with this request.
        """
        import requests

        url = urljoin(self.base_url, endpoint)
        return requests.request(  # noqa: S113
            method, url, **{"timeout": self.default_timeout, **kwargs}
        )

    def request_json(self, endpoint: str, method: str = "get", **kwargs: Any) -> Any:
        """
        Request JSON data from a nominated endpoint.

        Args:
            endpoint: The endpoint from which to receive data.
            method: The method to use when requesting this resource.
            **kwargs: Additional arguments to pass through to
                `requests.request`.

        Returns:
            The representation of the JSON response from the server.
        """
        request = self.request(endpoint, method=method, **kwargs)
        if not request.status_code == 200:
            try:
                raise RuntimeError(
                    f"Server responded with HTTP response code {request.status_code}, with content: {json.dumps(request.json())}."
                )
            except Exception as e:
                raise RuntimeError(
                    f"Server responded with HTTP response code {request.status_code}, with content: {request.content.decode('utf-8')}."
                ) from e
        return request.json()

    @override
    def _connect(self) -> None:
        pass

    @override
    def _is_connected(self) -> bool:
        return True

    @override
    def _disconnect(self) -> None:
        pass


class RestClient(RestClientBase):
    """
    A trivial implementation of `RestClientBase` for basic REST access.
    """

    PROTOCOLS = ["rest"]
