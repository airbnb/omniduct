import posixpath
import select
import stat
import threading

from omniduct.errors import DuctAuthenticationError
from omniduct.filesystems.base import FileSystemFileDesc
from omniduct.remotes.base import RemoteClient
from omniduct.utils.debug import logger
from omniduct.utils.processes import SubprocessResults

# Python 2 compatibility imports
try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

__all__ = ['ParamikoSSHClient']


class ParamikoSSHClient(RemoteClient):
    """
    An experimental SSH client that uses a `paramiko` rather than command-line
    SSH backend. This client has been fully implemented and should work as is,
    but until it receives further testing, we recommend using the cli backed SSH
    client.
    """

    PROTOCOLS = ['ssh_paramiko']
    DEFAULT_PORT = 22

    def _init(self):
        logger.warning("The Paramiko SSH client is still under development, \
                        and is not ready for use as a daily driver.")

    def _connect(self):
        import paramiko  # Imported here due to relatively slow import
        self.__client = paramiko.SSHClient()
        self.__client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        self.__client.load_system_host_keys()

        try:
            self.__client.connect(self.host, username=self.username)
            self.__client_sftp = paramiko.SFTPClient.from_transport(self.__client.get_transport())
        except paramiko.SSHException as e:
            if len(e.args) == 1 and e.args[0] == 'No authentication methods available':
                raise DuctAuthenticationError(e.args[0])
            raise e

    def _is_connected(self):
        try:
            return self.__client.get_transport().is_active()
        except:
            return False

    def _disconnect(self):
        try:
            self.__client_sftp.close()
            return self.__client.close()
        except:
            pass

    def _execute(self, cmd, **kwargs):
        stdin, stdout, stderr = self.__client.exec_command(cmd)
        returncode = stdout.channel.recv_exit_status()
        return SubprocessResults(
            returncode=returncode,
            stdout=stdout.read(),
            stderr=stderr.read()
        )

    def _port_forward_start(self, local_port, remote_host, remote_port):
        logger.debug('Now forwarding port {} to {}:{} ...'.format(local_port, remote_host, remote_port))

        try:
            server = forward_tunnel(local_port, remote_host, remote_port, self.__client.get_transport())
        except KeyboardInterrupt:
            print('C-c: Port forwarding stopped.')
        return server

    def _port_forward_stop(self, local_port, remote_host, remote_port, server):
        server.shutdown()

    def _is_port_bound(self, host, port):
        return True

    # Path properties and helpers

    def _path_home(self):
        return self.execute('echo ~', skip_cwd=True).stdout.decode('utf-8').strip()

    def _path_separator(self):
        return '/'

    # File node properties

    def _exists(self, path):
        try:
            self.__client_sftp.stat(path)
            return True
        except FileNotFoundError:
            return False

    def _isdir(self, path):
        try:
            return stat.S_ISDIR(self.__client_sftp.stat(path).st_mode)
        except FileNotFoundError:
            return False

    def _isfile(self, path):
        try:
            return not stat.S_ISDIR(self.__client_sftp.stat(path).st_mode)
        except FileNotFoundError:
            return False

    # Directory handling and enumeration

    def _dir(self, path):
        for attrs in self.__client_sftp.listdir_attr(path):
            yield FileSystemFileDesc(
                fs=self,
                path=posixpath.join(path, attrs.filename),
                name=attrs.filename,
                type='directory' if stat.S_ISDIR(attrs.st_mode) else 'file',  # TODO: What about links, which are of form: lrwxrwxrwx?
                bytes=attrs.st_size,
                owner=attrs.st_uid,
                group=attrs.st_gid,
                last_modified=attrs.st_mtime,
            )

    def _mkdir(self, path, recursive, exist_ok):
        if exist_ok and self.isdir(path):
            return
        assert self.execute('mkdir ' + ('-p ' if recursive else '') + '"{}"'.format(path)).returncode == 0, "Failed to create directory at: `{}`".format(path)

    def _remove(self, path, recursive):
        assert self.execute('rm -f ' + ('-r ' if recursive else '') + '"{}"'.format(path)).returncode == 0, "Failed to remove file(s) at: `{}`".format(path)

    # File handling

    def _open(self, path, mode):
        """
        Paramiko offers a complete file-like abstraction for files opened over
        sftp, so we use that abstraction rather than a `FileSystemFile`. Results
        should be indistinguishable.
        """
        return self.__client_sftp.open(path, mode=mode)


# Port Forwarding Utility Code
# Largely based on code from: https://github.com/paramiko/paramiko/blob/master/demos/forward.py

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
    class SubHandler(Handler):
        chain_host = remote_host
        chain_port = remote_port
        ssh_transport = transport
    server = ForwardServer(('', local_port), SubHandler)

    t = threading.Thread(target=server.serve_forever)
    t.setDaemon(True)  # don't hang on exit
    t.start()

    return server
