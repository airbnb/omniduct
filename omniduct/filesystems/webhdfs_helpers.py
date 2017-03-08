import json

import requests
from pywebhdfs import errors, operations
from pywebhdfs.webhdfs import (PyWebHdfsClient, _is_standby_exception,
                               _move_active_host_to_head)

from six.moves import http_client


class OmniductPyWebHdfsClient(PyWebHdfsClient):

    def __init__(self, remote=None, namenodes=None, **kwargs):
        self.remote = remote
        self.namenodes = namenodes or []

        PyWebHdfsClient.__init__(self, **kwargs)

        if self.namenodes and 'path_to_hosts' not in kwargs:
            self.path_to_hosts = [('.*', self.namenodes)]

        # Override base uri
        self.base_uri_pattern = kwargs.get('base_uri_pattern', "http://{host}/webhdfs/v1/").format(
            host="{host}")

    @property
    def host(self):
        host = 'localhost' if self.remote else self._host
        return '{}:{}'.format(host, str(self.port))

    @host.setter
    def host(self, host):
        self._host = host

    @property
    def port(self):
        if self.remote:
            return self.remote.port_forward('{}:{}'.format(self._host, self._port))
        return self._port

    @port.setter
    def port(self, port):
        self._port = port

    @property
    def namenodes(self):
        if self.remote:
            return ['localhost:{}'.format(self.remote.port_forward('{}:{}'.format(nn_host, self._port))) for nn_host in self._namenodes]
        else:
            return ['{}:{}'.format(nn, self._port) for nn in self._namenodes]

    @namenodes.setter
    def namenodes(self, namenodes):
        self._namenodes = namenodes

    def _make_uri_local(self, uri):
        if not self.remote:
            return uri
        uri = self.remote.get_local_uri(uri)
        return uri

    def get_home_directory(self):
        response = self._resolve_host(requests.get, True, '/', operation='GETHOMEDIRECTORY')
        if response.ok:
            return json.loads(response.content)['Path']
        return '/'

    def _resolve_host(self, req_func, allow_redirect,
                      path, operation, **kwargs):
        """
        internal function used to resolve federation and HA and
        return response of resolved host.
        """
        import requests
        uri_without_host = self._create_uri(path, operation, **kwargs)
        hosts = self._resolve_federation(path)
        for host in hosts:
            uri = uri_without_host.format(host=host)
            try:
                while True:
                    response = req_func(uri, allow_redirects=False,
                                        timeout=self.timeout,
                                        **self.request_extra_opts)

                    if allow_redirect and response.status_code == http_client.TEMPORARY_REDIRECT:
                        uri = self._make_uri_local(response.headers['location'])
                    else:
                        break

                if not allow_redirect and response.status_code == http_client.TEMPORARY_REDIRECT:
                    response.headers['location'] = self._make_uri_local(response.headers['location'])

                if not _is_standby_exception(response):
                    _move_active_host_to_head(hosts, host)
                    return response
            except requests.exceptions.RequestException:
                pass
        raise errors.ActiveHostNotFound(msg="Could not find active host")
