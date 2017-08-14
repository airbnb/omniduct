import requests
from future.moves.urllib.parse import urlparse, urlencode, urljoin

from omniduct.duct import Duct
from omniduct.utils.magics import MagicsProvider, process_line_arguments


class RestClientBase(Duct):
    '''
    This is a simple wrapper around the requests library to simplify the use
    of RESTful clients with omniduct. This allows all the automatic features
    around port forwarding from remote hosts to be inherited. This client can
    be used directly, or decorated by subclasses which can add methods
    specific to any REST service; and internally use `request` and `request_json`
    to access various endpoints.
    '''

    DUCT_TYPE = Duct.Type.RESTFUL

    def __init__(self, *args, server_protocol='http', assume_json=False, endpoint_prefix='', **kwargs):
        '''
        This is a shim __init__ function that passes all arguments onto
        `self._init`, which is implemented by subclasses. This allows subclasses
        to instantiate themselves with arbitrary parameters.
        '''
        Duct.__init_with_kwargs__(self, kwargs, port=80)

        self.server_protocol = server_protocol
        self.assume_json = assume_json
        self.endpoint_prefix = endpoint_prefix

        self._init(*args, **kwargs)

    def _init(self):
        pass

    def __call__(self, endpoint, method='get', **kwargs):
        if self.assume_json:
            return self.request_json(endpoint, method=method, **kwargs)
        return self.request(endpoint, method=method, **kwargs)

    @property
    def base_url(self):
        url = urljoin('{}://{}:{}'.format(self.server_protocol, self.host, self.port or 80), self.endpoint_prefix)
        if not url.endswith('/'):
            url += '/'
        return url

    def request(self, endpoint, method='get', **kwargs):
        self.connect()
        url = urljoin(self.base_url, endpoint)
        return requests.request(method, url, **kwargs)

    def request_json(self, endpoint, method='get', **kwargs):
        request = self.request(endpoint, method=method, **kwargs)
        if not request.status_code == 200:
            raise RuntimeError("Server responded with HTTP response code {}, with content: {}.".format(request.status_code, request.content.decode()))
        return request.json()

    def _connect(self):
        pass

    def _is_connected(self):
        return True

    def _disconnect(self):
        pass


class RestClient(RestClientBase):
    PROTOCOLS = ['rest']
