import unittest
from unittest import mock

import pytest

from omniduct.restful.base import RestClient


class TestRestClient(unittest.TestCase):
    @mock.patch.object(RestClient, "connect")
    @mock.patch("requests.request")
    def test_default_request(self, mock_request, mock_connect):
        client = RestClient(server_protocol="http", host="localhost", port=80)
        client.request("/")
        mock_connect.assert_called_with()
        mock_request.assert_called_with("get", "http://localhost:80/", timeout=None)


@pytest.mark.parametrize(
    "protocol,kwargs,expected_port",
    [
        ("http", {}, 80),
        ("https", {}, 443),
        ("https", {"port": 8443}, 8443),
        ("http", {"port": 8080}, 8080),
        ("https", {"host": "localhost:9443"}, 9443),
    ],
)
def test_rest_default_port(protocol, kwargs, expected_port):
    client = RestClient(server_protocol=protocol, **{"host": "localhost", **kwargs})
    assert client.port == expected_port
    assert client.base_url == f"{protocol}://localhost:{expected_port}/"
