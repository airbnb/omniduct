import unittest
import mock

from omniduct.restful.base import RestClient


class TestRestClient(unittest.TestCase):

    @mock.patch.object(RestClient, 'connect')
    @mock.patch('requests.request')
    def test_default_request(self, mock_request, mock_connect):
        client = RestClient(server_protocol='http', host='localhost', port=80)
        client.request('/')
        mock_connect.assert_called_with()
        mock_request.assert_called_with("get", "http://localhost:80/")
