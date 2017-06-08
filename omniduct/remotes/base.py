"""
Connection Module
-----------------

This module contains functionality for connecting to remote locations including
ssh utilities (port forwarding) and execution of commands remotely.

Also includes constants for remote clusters.
"""

import datetime
import getpass
import os
import re
import types
from abc import abstractmethod

import six
from future.utils import raise_with_traceback
import pandas as pd

from omniduct.duct import Duct
from omniduct.errors import DuctAuthenticationError, DuctServerUnreachable
from omniduct.filesystems.base import FileSystemClient
from omniduct.utils.debug import logger
from omniduct.utils.ports import get_free_local_port, is_local_port_free
from omniduct.utils.processes import run_in_subprocess

try:  # Python 3
    from urllib.parse import urlparse, urlunparse
except ImportError:  # Python 2
    from urlparse import urlparse, urlunparse


class PortForwardingRegister(object):

    def __init__(self):
        self._register = {}

    def lookup(self, remote_host, remote_port):
        return self._register.get('{}:{}'.format(remote_host, remote_port))

    def lookup_port(self, remote_host, remote_port):
        entry = self.lookup(remote_host, remote_port)
        if entry is not None:
            return entry[0]

    def reverse_lookup(self, local_port):
        for key, (port, connection) in self._register.items():
            if port == local_port:
                return key.split(':') + [connection]
        return None

    def register(self, remote_host, remote_port, local_port, connection):
        key = '{}:{}'.format(remote_host, remote_port)
        if key in self._register:
            raise RuntimeError("Remote host/port combination ({}) is already registered.".format(key))
        self._register[key] = (local_port, connection)

    def unregister(self, remote_host, remote_port):
        return self._register.pop('{}:{}'.format(remote_host, remote_port))


class RemoteClient(FileSystemClient):
    '''
    SSHClient is an abstract class that can be subclassed into a fully-functional
    SSH client.
    '''

    DUCT_TYPE = Duct.Type.REMOTE

    def __init__(self, smartcards=None, **kwargs):
        """
        Create a new SSHClient.

        Parameters
        ----------
        host : string
            Remote host for ssh.
        user : string
            User name for ssh.
        kwargs : dict
            Extra parameters passed on to SSHClient._init, as implemented by subclasses.

        Returns
        -------
        self : SSHClient
            An SSHClient object with the connection details specified.
        """
        FileSystemClient.__init_with_kwargs__(self, kwargs, port=22)

        self.smartcards = smartcards
        self.__port_forwarding_register = PortForwardingRegister()

        self._init(**kwargs)

    @abstractmethod
    def _init(self, **kwargs):
        '''
        To be defined by subclasses, and called immediately after __init__. This allows
        subclasses to initialise themselves.
        '''
        pass

    # SSH commands
    def connect(self):
        """
        If a connection to the ssh server does not already exist, calling
        this method creates it. It first attempts to connect directly. If it fails, it attempts to initialise and keys
        in case they had not already been initialised. (It does not do this before creating the connection so as to
        minimise needless re-preparation of the keys.

        NOTE: It is not normally necessary for a user to manually call this function,
        since when a connection is required, it is automatically made.
        """
        try:
            Duct.connect(self)
        except DuctServerUnreachable as e:
            raise_with_traceback(e)
        except DuctAuthenticationError as e:
            if self.smartcards and self.prepare_smartcards():
                Duct.connect(self)
            else:
                raise_with_traceback(e)
        return self

    def prepare_smartcards(self, *args, **more_smartcards):
        """
        This method checks attempts to ensure that the each of the provided keys is available. If not keys are not
        specified then the list of keys is taken from the inited arguments, or if not specified, this method does nothing.
        """

        smartcards = {}
        for arg in args:
            smartcards.update(arg)
        smartcards.update(more_smartcards)

        if len(smartcards) == 0:
            smartcards = self.smartcards or {}
        if len(smartcards) == 0:
            return False

        for name, filename in smartcards.items():
            self._prepare_smartcard(name, filename)

        return True

    def _prepare_smartcard(self, name, filename):
        import pexpect

        remover = pexpect.spawn('ssh-add -e "{}"'.format(filename))
        i = remover.expect(["Card removed:", "Could not remove card", pexpect.TIMEOUT])
        if i == 2:
            raise RuntimeError("Unable to reset card using ssh-agent. Output of ssh-agent was: \n{}\n\n"
                               "Please report this error!".format(remover.before))

        adder = pexpect.spawn('ssh-add -s "{}" -t 14400'.format(filename))
        i = adder.expect(['Enter passphrase for PKCS#11:', pexpect.TIMEOUT])
        if i == 0:
            adder.sendline(getpass.getpass('Please enter your passcode to unlock your "{}" smartcard: '.format(name)))
        else:
            raise RuntimeError("Unable to add card using ssh-agent. Output of ssh-agent was: \n{}\n\n"
                               "Please report this error!".format(remover.before))
        i = adder.expect(['Card added:', pexpect.TIMEOUT])
        if i != 0:
            raise RuntimeError("Unexpected error while adding card. Check your passcode and try again.")

        return True

    def execute(self, cmd, **kwargs):
        '''
        Execute `cmd` on the remote shell via ssh. Additional keyword arguments are
        passed on to subclasses.
        '''
        return self.connect()._execute(cmd, **kwargs)

    @abstractmethod
    def _execute(self, cmd, **kwargs):
        '''
        Should return a tuple of:
        (<status code>, <data printed to stdout>, <data printed to stderr>)
        '''
        raise NotImplementedError

    # Port forwarding code

    def __extract_host_and_ports(self, remote_host, remote_port, local_port):
        assert remote_host is None or isinstance(remote_host, six.string_types), "Remote host, if specified, must be a string of form 'hostname(:port)'."
        assert remote_port is None or isinstance(remote_port, int), "Remote port, if specified, must be an integer."
        assert local_port is None or isinstance(local_port, int), "Local port, if specified, must be an integer."

        host = port = None
        if remote_host is not None:
            m = re.match('(?P<host>[a-zA-Z0-9\-\.]+)(?:\:(?P<port>[0-9]+))?', remote_host)
            assert m, "Host not valid: {}. Must be a string of form 'hostname(:port)'.".format(remote_host)

            host = m.group('host')
            port = m.group('port') or remote_port
        return host, port, local_port

    def port_forward(self, remote_host, remote_port=None, local_port=None):
        '''
        Establishes a local port forwarding from local port `local` to remote
        port `remote`. If `local` is `None`, automatically find an available local
        port, and forward it. This method returns the used local port.

        If the remote port is already forwarded, a new connection is not created.
        '''

        # Hostname and port extraction
        remote_host, remote_port, local_port = self.__extract_host_and_ports(remote_host, remote_port, local_port)
        assert remote_host is not None, "Remote host must be specified."
        assert remote_port is not None, "Remote port must be specified."

        # Actual port forwarding
        registered_port = self.__port_forwarding_register.lookup_port(remote_host, remote_port)
        if registered_port is not None:
            if local_port is not None and registered_port != local_port:
                self.port_forward_stop(registered_port)
            else:
                return registered_port

        if local_port is None:
            local_port = get_free_local_port()
        else:
            assert is_local_port_free(local_port), "Specified local port is in use, and cannot be used."

        if not self.is_port_bound(remote_host, remote_port):
            raise DuctServerUnreachable("Server specified for port forwarding ({}:{}) in unreachable.".format(remote_host, remote_port))
        connection = self.connect()._port_forward_start(local_port, remote_host, remote_port)
        self.__port_forwarding_register.register(remote_host, remote_port, local_port, connection)

        return local_port

    def has_port_forward(self, remote_host=None, remote_port=None, local_port=None):
        # Hostname and port extraction
        remote_host, remote_port, local_port = self.__extract_host_and_ports(remote_host, remote_port, local_port)

        assert remote_host is not None and remote_port is not None or local_port is not None, "Either remote host and port must be specified, or the local port must be specified."

        if remote_host is not None and remote_port is not None:
            return self.__port_forwarding_register.lookup(remote_host, remote_port) is not None
        else:
            return self.__port_forwarding_register.reverse_lookup(local_port) is not None

    def port_forward_stop(self, local_port=None, remote_host=None, remote_port=None):
        # Hostname and port extraction
        remote_host, remote_port, local_port = self.__extract_host_and_ports(remote_host, remote_port, local_port)

        assert remote_host is not None and remote_port is not None or local_port is not None, "Either remote host and port must be specified, or the local port must be specified."

        if remote_host is not None and remote_port is not None:
            local_port, connection = self.__port_forwarding_register.lookup(remote_host, remote_port)
        else:
            remote_host, remote_port, connection = self.__port_forwarding_register.reverse_lookup(local_port)

        self._port_forward_stop(local_port, remote_host, remote_port, connection)
        self.__port_forwarding_register.unregister(remote_host, remote_port)

    def port_forward_stopall(self):
        '''
        Stop all port forwarding.
        '''
        for remote_host in self.__port_forwarding_register._register:
            self.port_forward_stop(remote_host=remote_host)

    def get_local_uri(self, uri):
        parsed_uri = urlparse(uri)
        return urlunparse(parsed_uri._replace(netloc='localhost:{}'.format(self.port_forward(parsed_uri.netloc))))

    def show_port_forwards(self):
        '''
        Return a list of active port forwards, in the form:
        (local, remote, obj)
        where `obj` is the value returned from `_port_forward_start`.
        '''
        if len(self.__port_forwarding_register._register) == 0:
            print("No port forwards currently in use.")
        for remote_host, (local_port, _) in self.__port_forwarding_register._register.items():
            print("localhost:{}".format(local_port), "->", remote_host, "(on {})".format(self._host))

    @abstractmethod
    def _port_forward_start(self, local_port, remote_host, remote_port):
        raise NotImplementedError

    @abstractmethod
    def _port_forward_stop(self, local_port, remote_host, remote_port, connection):
        raise NotImplementedError

    def is_port_bound(self, host, port):
        return self.connect()._is_port_bound(host, port)

    @abstractmethod
    def _is_port_bound(self, host, port):
        pass
