import select
import threading

from omniduct.remotes.base import RemoteClient
from omniduct.utils.debug import logger

try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer


class ParamikoSSHClient(RemoteClient):

    PROTOCOLS = ['ssh_paramiko']

    def _init(self):
        logger.warning("The Paramiko SSH client is still under development, \
                        and is not ready for use as a daily driver.")

    def _connect(self):
        import paramiko  # Imported here due to relatively slow import
        self.__client = paramiko.SSHClient()
        self.__client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        self.__client.load_system_host_keys()

        self.__client.connect(self.host, username=self.username)

    def _is_connected(self):
        try:
            return self.__client.get_transport().is_active()
        except:
            return False

    def _disconnect(self):
        try:
            return self.__client.close()
        except:
            pass

    def _execute(self, cmd, **kwargs):
        return self.__client.exec_command(cmd)

    def _copy_to_local(self, source, dest):
        raise NotImplementedError

    def _copy_from_local(self, source, dest):
        raise NotImplementedError

    def _port_forward_start(self, local_port, remote_host, remote_port):
        logger.debug('Now forwarding port {} to {}:{} ...'.format(local_port, remote_host, remote_port))

        remote_host, remote_port = get_host_port(remote_host, remote_port)

        try:
            server = forward_tunnel(local_port, remote_host, remote_port, self.__client.get_transport())
        except KeyboardInterrupt:
            print('C-c: Port forwarding stopped.')
        return server

    def _port_forward_stop(self, local_port, remote_host, remote_port, server):
        server.shutdown()

    def _is_port_bound(self, host, port):
        return True

    # FileSystem methods

    def _exists(self, path):
        raise NotImplementedError

    def _isdir(self, path):
        raise NotImplementedError

    def _isfile(self, path):
        raise NotImplementedError

    def _listdir(self, path):
        raise NotImplementedError

    def _showdir(self, path):
        raise NotImplementedError

    # File handling

    def _file_read_(self, path, size=-1, offset=0, binary=False):
        raise NotImplementedError

    def _file_append_(self, path, s, binary):
        raise NotImplementedError

    def _file_write_(self, path, s, binary):
        raise NotImplementedError


# Port Forwarding Utility Code
# Largely based on code from: https://github.com/paramiko/paramiko/blob/master/demos/forward.py

SSH_PORT = 22
DEFAULT_PORT = 4000

g_verbose = True


class ForwardServer (SocketServer.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler (SocketServer.BaseRequestHandler):

    def handle(self):
        try:
            chan = self.ssh_transport.open_channel('direct-tcpip',
                                                   (self.chain_host, self.chain_port),
                                                   self.request.getpeername())
        except Exception as e:
            logger.info('Incoming request to %s:%d failed: %s' % (self.chain_host,
                                                                  self.chain_port,
                                                                  repr(e)))
            return
        if chan is None:
            logger.info('Incoming request to %s:%d was rejected by the SSH server.' %
                        (self.chain_host, self.chain_port))
            return

        logger.info('Connected!  Tunnel open %r -> %r -> %r' % (self.request.getpeername(),
                                                                chan.getpeername(), (self.chain_host, self.chain_port)))
        while True:
            r, w, x = select.select([self.request, chan], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                self.request.send(data)

        peername = self.request.getpeername()
        chan.close()
        self.request.close()
        logger.info('Tunnel closed from %r' % (peername,))


def forward_tunnel(local_port, remote_host, remote_port, transport):
    # this is a little convoluted, but lets me configure things for the Handler
    # object.  (SocketServer doesn't give Handlers any way to access the outer
    # server normally.)
    class SubHander (Handler):
        chain_host = remote_host
        chain_port = remote_port
        ssh_transport = transport
    server = ForwardServer(('', local_port), SubHander)

    t = threading.Thread(target=server.serve_forever)
    t.setDaemon(True)  # don't hang on exit
    t.start()

    return server


def get_host_port(spec, default_port=22):
    "parse 'hostname:22' into a host and port, with the port optional"
    args = (spec.split(':', 1) + [default_port])[:2]
    args[1] = int(args[1])
    return args[0], args[1]
