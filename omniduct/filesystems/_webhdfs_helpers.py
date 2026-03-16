from __future__ import annotations

import http.client
import json
import xml.dom.minidom
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast
from xml.dom.minidom import Text

import requests
from pywebhdfs import errors
from pywebhdfs.webhdfs import (
    PyWebHdfsClient,
    _is_standby_exception,
    _move_active_host_to_head,
)

if TYPE_CHECKING:
    from omniduct.filesystems.base import FileSystemClient


class OmniductPyWebHdfsClient(PyWebHdfsClient):
    """
    A wrapper around `pywebhdfs.PyWebHdfsClient` to handle redirects requested
    by the namenodes when taking advantage of Omniduct's automatic
    port-forwarding of remote services.
    """

    def __init__(
        self,
        remote: Any | None = None,
        namenodes: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        self.remote = remote
        self.namenodes = namenodes or []

        PyWebHdfsClient.__init__(self, **kwargs)

        if self.namenodes and "path_to_hosts" not in kwargs:
            self.path_to_hosts = [(".*", self.namenodes)]

        # Override base uri
        self.base_uri_pattern = kwargs.get(
            "base_uri_pattern", "http://{host}/webhdfs/v1/"
        ).format(host="{host}")

    @property
    def host(self) -> str:
        host = "localhost" if self.remote else self._host
        return f"{host}:{str(self.port)}"

    @host.setter
    def host(self, host: str) -> None:
        self._host = host

    @property
    def port(self) -> int | str:
        if self.remote:
            return self.remote.port_forward(f"{self._host}:{self._port}")  # type: ignore[no-any-return]
        return self._port

    @port.setter
    def port(self, port: int | str) -> None:
        self._port = port

    @property
    def namenodes(self) -> list[str]:
        if self.remote:
            return [
                f"localhost:{self.remote.port_forward(nn)}" for nn in self._namenodes
            ]
        return self._namenodes

    @namenodes.setter
    def namenodes(self, namenodes: list[str]) -> None:
        self._namenodes = namenodes

    def _make_uri_local(self, uri: str) -> str:
        if not self.remote:
            return uri
        uri = self.remote.get_local_uri(uri)
        return uri

    def get_home_directory(self) -> str:
        response = self._resolve_host(
            requests.get, True, "/", operation="GETHOMEDIRECTORY"
        )
        if response.ok:
            return json.loads(response.content)["Path"]  # type: ignore[no-any-return]
        return "/"

    def _resolve_host(
        self,
        req_func: Callable[..., Any],
        allow_redirect: bool,
        path: str,
        operation: str,
        **kwargs: Any,
    ) -> Any:
        """
        This is where the magic happens, and where omniduct handles redirects
        during federation and HA.
        """
        uri_without_host = self._create_uri(path, operation, **kwargs)
        hosts = self._resolve_federation(path)
        for host in hosts:
            uri = uri_without_host.format(host=host)
            try:
                while True:
                    response = req_func(
                        uri,
                        allow_redirects=False,
                        timeout=self.timeout,
                        **self.request_extra_opts,
                    )

                    if (
                        allow_redirect
                        and response.status_code == http.client.TEMPORARY_REDIRECT
                    ):
                        uri = self._make_uri_local(response.headers["location"])
                    else:
                        break

                if (
                    not allow_redirect
                    and response.status_code == http.client.TEMPORARY_REDIRECT
                ):
                    response.headers["location"] = self._make_uri_local(
                        response.headers["location"]
                    )

                if not _is_standby_exception(response):
                    _move_active_host_to_head(hosts, host)
                    return response
            except requests.exceptions.RequestException:
                pass
        raise errors.ActiveHostNotFound(msg="Could not find active host")


class CdhHdfsConfParser:
    """
    This class serves to automatically extract HDFS cluster information from
    Cloudera configuration files.
    """

    fs: FileSystemClient
    conf_path: str

    def __init__(self, fs: FileSystemClient, conf_path: str | None = None) -> None:
        """
        Args:
            fs: The filesystem on which the configuration
                file should be found.
            conf_path: The path of the configuration file to be parsed.
        """
        self.fs = fs
        self.conf_path = conf_path or "/etc/hadoop/conf.cloudera.hdfs2/hdfs-site.xml"

    @property
    def config(self) -> dict[str, str]:
        if not hasattr(self, "_config"):
            self._config = self._get_config()
        return self._config

    def _get_config(self) -> dict[str, str]:
        with self.fs.open(self.conf_path) as f:
            d = xml.dom.minidom.parseString(f.read())  # noqa: S318

        properties = d.getElementsByTagName("property")

        return {
            cast(
                Text, prop.getElementsByTagName("name")[0].childNodes[0]
            ).wholeText: cast(
                Text, prop.getElementsByTagName("value")[0].childNodes[0]
            ).wholeText
            for prop in properties
        }

    @property
    def clusters(self) -> list[str]:
        clusters: list[str] = []
        for key in self.config:
            if key.startswith("dfs.ha.namenodes."):
                clusters.append(key[len("dfs.ha.namenodes.") :])
        return clusters

    def namenodes(self, cluster: str) -> list[str]:
        namenodes = self.config[f"dfs.ha.namenodes.{cluster}"].split(",")
        return [
            self.config[f"dfs.namenode.http-address.{cluster}.{namenode}"]
            for namenode in namenodes
        ]
