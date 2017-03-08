import getpass
import os

from omniduct.remotes.base import RemoteClient
from omniduct.utils.debug import logger
from omniduct.utils.ports import is_local_port_free
from omniduct.utils.processes import run_in_subprocess

SSH_ASKPASS = '{omniduct_dir}/utils/ssh_askpass'.format(omniduct_dir=os.path.dirname(__file__))
SESSION_SSH_USERNAME = None
SESSION_REMOTE_HOST = None
SESSION_SSH_ASKPASS = False


class SSHClient(RemoteClient):
    """SSHClient manages a persistent connection to remote hosts allowing
    remote execution, port forwarding, file transfer, and cleanup

    References
    ----------
    https://puppetlabs.com/blog/speed-up-ssh-by-reusing-connections

    """

    PROTOCOLS = ['ssh']

    def _init(self):
        pass

    # Duct connection implementation

    def _connect(self):
        """
        Create persistent connection to remote host.

        The workflow to handle passwords and host keys is inspired by the pxssh module of pexpect
        (https://github.com/pexpect/pexpect). We have adjusted this workflow to our purposes.

        Returns
        -------
        proc : Popen subprocess
            Subprocess used to connect.
        """
        import pexpect

        # Create socket directory if it doesn't exist.
        socket_dir = os.path.dirname(self._socket_path)
        if not os.path.exists(socket_dir):
            os.makedirs(socket_dir)
        # Create persistent master connection and exit.
        cmd = ("ssh {login} -MT "
               "-S {socket} "
               "-o ControlPersist=yes "
               "-o StrictHostKeyChecking=no "
               "-o NoHostAuthenticationForLocalhost=yes "
               "-o ServerAliveInterval=60 "
               "-o ServerAliveCountMax=2 "
               "'exit'".format(login=self._login_info, socket=self._socket_path))
        expected = ["(?i)are you sure you want to continue connecting",    # 0
                    "(?i)(?:password)|(?:passphrase for key)",             # 1
                    "(?i)permission denied",                               # 2
                    "(?i)terminal type",                                   # 3
                    pexpect.TIMEOUT,                                       # 4
                    "(?i)connection closed by remote host",                # 5
                    pexpect.EOF]                                           # 6

        try:
            expect = pexpect.spawn(cmd)
            i = expect.expect(expected, timeout=10)

            # First phase
            if i == 0:  # Request to authorize host certificate (i.e. host not in the 'known_hosts' file)
                expect.sendline("yes")
                i = self.expect(expected)
            if i == 1:  # Request for password/passphrase
                expect.sendline(self.password or getpass.getpass('Password: '))
                i = self.expect(expected)
            if i == 3:  # Request for terminal type
                expect.sendline('ascii')
                i = self.expect(expected)

            # Second phase
            if i == 0:  # Another request to authorize host certificate (i.e. host not in the 'known_hosts' file)
                raise RuntimeError('Received a second request to authorize host key. This should not have happened!')
            elif i in (1, 2):  # Second request for password/passphrase or rejection of creditials. For now, give up.
                raise RuntimeError('Invalid username and/or password, or private key is not unlocked.')
            elif i == 3:  # Another request for terminal type.
                raise RuntimeError('Received a second request for terminal type. This should not have happened!')
            elif i == 4:  # Timeout
                # In our instance, this means that we have not handled some or another aspect of the login procedure.
                # Since we are expecting an EOF when we have successfully logged in, hanging means that the SSH login
                # procedure is waiting for more information. Since we have no more to give, this means our login
                # was unsuccessful.
                raise RuntimeError('SSH client seems to be awaiting more information, but we have no more to give. The '
                                   'messages received so far are:\n{}'.format(expect.before))
            elif i == 5:  # Connection closed by remote host
                raise RuntimeError("Remote closed SSH connection")
        finally:
            expect.close()

        # We should be logged in at this point, but let us make doubly sure
        assert self.is_connected(), 'Unexpected failure to establish a connection with the remote host with command: \n ' \
                                    '{}\n\n Please report this!'.format(cmd)

        logger.info('Connected')

    def _is_connected(self):
        """
        Return whether SSHClient is connected by checking the control socket.
        """
        logger.info('Checking control socket...')
        cmd = "ssh {login} -T -S {socket} -O check".format(login=self._login_info,
                                                           socket=self._socket_path)
        proc = run_in_subprocess(cmd)
        return proc.returncode == 0

    def _disconnect(self):
        """
        Exit persistent connection to remote host.
        """
        # Send exit request to control socket.
        logger.info('Disconnecting...')
        cmd = "ssh {login} -T -S {socket} -O exit".format(login=self._login_info,
                                                          socket=self._socket_path)
        proc = run_in_subprocess(cmd)

    # RemoteClient implementation

    def _execute(self, cmd, **kwargs):
        """
        Execute a command on a remote host.

        Parameters
        ----------
        cmd : string
            Command to be executed on remote host.
        kwargs : keywords
            Options to pass to subprocess.Popen.

        Returns
        -------
        proc : Popen subprocess
            Subprocess used to run remote command.
        """
        logger.info('Sending command...')
        template = 'ssh {login} -T -o ControlPath={socket} << EOF\n{cmd}\nEOF'
        config = dict(self._subprocess_config)
        config.update(kwargs)
        return run_in_subprocess(template.format(login=self._login_info,
                                                 socket=self._socket_path,
                                                 cmd=cmd),
                                 check_output=True,
                                 **config)

    def _copy_to_local(self, source, dest=None):
        """
        SCP remote file.

        Parameters
        ----------
        origin_file : string
            Path to local file to copy.
        destination_file : string
            Target location on remote host.
        """
        self.connect()
        logger.info('Copying file to local...')
        template = 'scp -o ControlPath={socket} {login}:{remote_file} {local_file}'
        destination_file = dest or os.path.split(source)[1]
        proc = run_in_subprocess(template.format(socket=self._socket_path,
                                                 login=self._login_info,
                                                 local_file=destination_file,
                                                 remote_file=source),
                                 check_output=True)
        logger.info(proc.stderr or 'Success')

    def _copy_from_local(self, source, dest=None):
        """
        SCP local file.

        Parameters
        ----------
        origin_file : string
            Path to remote file to copy.
        destination_file : string
            Target location on local machine.
        """
        self.connect()
        logger.info('Copying file from local...')
        template = 'scp -o ControlPath={socket} {local_file} {login}:{remote_file}'
        destination_file = dest or os.path.split(source)[1]
        proc = run_in_subprocess(template.format(socket=self._socket_path,
                                                 login=self._login_info,
                                                 local_file=source,
                                                 remote_file=destination_file),
                                 check_output=True)
        logger.info(proc.stderr or 'Success')

    def _port_forward_start(self, local_port, remote_host, remote_port):
        self.connect()
        logger.info('Establishing port forward...')
        cmd_template = 'ssh {login} -T -O forward -S {socket} -L localhost:{local_port}:{remote_host}:{remote_port}'
        cmd = cmd_template.format(login=self._login_info,
                                  socket=self._socket_path,
                                  local_port=local_port,
                                  remote_host=remote_host,
                                  remote_port=remote_port)
        proc = run_in_subprocess(cmd)
        if proc.returncode != 0:
            raise Exception('Unable to port forward with command: {}'.format(cmd))
        logger.info(proc.stderr or 'Success')
        return proc

    def _port_forward_stop(self, local_port, remote_host, remote_port, connection):
        logger.info('Cancelling port forward...')
        cmd_template = 'ssh {login} -T -O cancel -S {socket} -L localhost:{local_port}:{remote_host}:{remote_port}'
        cmd = cmd_template.format(login=self._login_info,
                                  socket=self._socket_path,
                                  local_port=local_port,
                                  remote_host=remote_host,
                                  remote_port=remote_port)
        proc = run_in_subprocess(cmd)
        logger.info('Success' if proc.returncode == 0 else 'Failure')

    def _is_port_bound(self, host, port):
        return self.execute('which nc; if [ $? -eq 0 ]; then  nc -z {} {}; fi'.format(host, port)).returncode == 0

    # Help methods

    @property
    def _login_info(self):
        return '@'.join([self.username, self.host])

    @property
    def _socket_path(self):
        # On Linux the maximum socket path length is 108 characters, and on Mac OS X it is 104 characters, including
        # the final sentinel character (or so it seems). SSH appends a '.' character, followed by random sequence of 16
        # characters. We therefore need the rest of the path to be less than 86 characters.
        return os.path.expanduser('~/.ssh/omniduct/{}'.format(self._login_info))[:86]

    @property
    def _subprocess_config(self):
        return {}
