from omniduct.duct import Duct
from omniduct.utils.magics import MagicsProvider, process_line_arguments


class RestClient(Duct):
    '''
    This is a simple wrapper around the requests library to simplify the use
    of RESTful clients with omniduct. This allows all the automatic features
    around port forwarding from remote hosts to be inherited. This client can
    be used directly, or decorated by subclasses which can add methods
    specific to any REST service; and internally use `request` and `request_json`
    to access various endpoints.
    '''

    def __init__(self, *args, **kwargs):
        '''
        This is a shim __init__ function that passes all arguments onto
        `self._init`, which is implemented by subclasses. This allows subclasses
        to instantiate themselves with arbitrary parameters.
        '''
        Duct.__init_with_kwargs__(self, kwargs)
        self._init(*args, **kwargs)

    def _init(self, base_url):
        self.base_url = base_url

    def request(self, method, endpoint, **kwargs):
        self.connect()
        url = urljoin(self.base_url, endpoint)
        return requests.request(method, url, **kwargs)

    def request_json(self, method, endpoint, **kwargs):
        request = self.request(method, endpoint, **kwargs)
        if not request.status_code == 200:
            raise RuntimeError("Server responded with HTTP response code {}, with content: {}.".format(request.status_code, request.content.decode()))
        return request.json()

    def _connect(self):
        pass

    def _is_connected(self):
        return True

    def _disconnect(self):
        pass


class BasicRestClient(RestClient):
    PROTOCOLS = ['rest']
