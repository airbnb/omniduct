import datetime
import getpass
import os
import posixpath
import re
import tempfile
from builtins import input
from io import open

import pandas as pd
from interface_meta import override

from omniduct.errors import DuctAuthenticationError
from omniduct.filesystems.base import FileSystemFileDesc
from omniduct.remotes.base import RemoteClient
from omniduct.utils.debug import logger
from omniduct.utils.decorators import require_connection
from omniduct.utils.processes import run_in_subprocess

try:  # Python 3
    from shlex import quote as escape_path
except ImportError:  # Python 2.7
    from pipes import quote as escape_path


SSH_ASKPASS = '{omniduct_dir}/utils/ssh_askpass'.format(omniduct_dir=os.path.dirname(__file__))
SESSION_SSH_USERNAME = None
SESSION_REMOTE_HOST = None
SESSION_SSH_ASKPASS = False


class SSHClient(RemoteClient):
    """
    An implementation of the `RemoteClient` `Duct`, offering a persistent
    connection to remote hosts over SSH via the CLI. As such, it requires that
    `ssh` be installed and on your executable path.

    To speed up connections we use control sockets, which allows all connections
    to share one SSH transport. For more details, refer to:
    https://puppetlabs.com/blog/speed-up-ssh-by-reusing-connections

    Attributes:
        interactive (bool): Whether `SSHClient` should ask the user
            questions, if necessary, to establish the connection. Production
            deployments using this client should set this to False.
            (default: `False`)
    """

    PROTOCOLS = ['ssh', 'ssh_cli']
    DEFAULT_PORT = 22

    @override
    def _init(self, interactive=False, check_known_hosts=True):
        """
        interactive (bool):  Whether `SSHClient` should ask the user questions,
            if necessary, to establish the connection. Production deployments
            using this client should set this to False, which is the default.
        check_known_hosts (bool):  Whether `SSHClient` should check the
            known hosts file when establishing the connection. This option
            should only be set to False in trusted environments.
        """
        self.interactive = interactive
        self.check_known_hosts = check_known_hosts

    # Duct connection implementation
    @override
    def _connect(self):
        """
        The workflow to handle passwords and host keys used by this method is
        inspired by the `pxssh` module of `pexpect` (https://github.com/pexpect/pexpect).
        We have adjusted this workflow to our purposes.
        """
        import pexpect

        # Create socket directory if it doesn't exist.
        socket_dir = os.path.dirname(self._socket_path)
        if not os.path.exists(socket_dir):
            os.makedirs(socket_dir)
        # Create persistent master connection and exit.
        cmd = ''.join([
            "ssh {login} -MT ",
            "-S {socket} ",
            "-o ControlPersist=yes ",
            "-o StrictHostKeyChecking=no ",
            "-o UserKnownHostsFile=/dev/null " if not self.check_known_hosts else "",
            "-o NoHostAuthenticationForLocalhost=yes ",
            "-o ServerAliveInterval=60 ",
            "-o ServerAliveCountMax=2 ",
            "'exit'",
        ]).format(login=self._login_info, socket=self._socket_path)

        expected = [
            "WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!",    # 0
            "(?i)are you sure you want to continue connecting",    # 1
            "(?i)(?:(?:password)|(?:passphrase for key)):",        # 2
            "(?i)permission denied",                               # 3
            "(?i)terminal type",                                   # 4
            pexpect.TIMEOUT,                                       # 5
            "(?i)connection closed by remote host",                # 6
            "(?i)could not resolve hostname",                      # 7
            pexpect.EOF                                            # 8
        ]

        try:
            expect = pexpect.spawn(cmd)
            i = expect.expect(expected, timeout=10)

            # First phase
            if i == 0:  # If host identification changed, arrest any further attempts to connect
                error_message = (
                    'Host identification for {} has changed! This is most likely '
                    'due to the the server being redeployed or reconfigured but '
                    'may also be due to a man-in-the-middle attack. If you trust '
                    'your network connection, you should be safe to update the '
                    'host keys for this host. To do this manually, please remove '
                    'the line corresponding to this host in ~/.ssh/known_hosts; '
                    'or call the `update_host_keys` method of this client.'.format(self._host)
                )
                if self.interactive:
                    logger.error(error_message)
                    auto_fix = input('Would you like this client to do this for you? (y/n)')
                    if auto_fix == 'y':
                        self.update_host_keys()
                        return self.connect()
                    else:
                        raise RuntimeError("Host keys not updated. Please update keys manually.")
                else:
                    raise RuntimeError(error_message)
            if i == 1:  # Request to authorize host certificate (i.e. host not in the 'known_hosts' file)
                expect.sendline("yes")
                i = self.expect(expected)
            if i == 2:  # Request for password/passphrase
                expect.sendline(self.password or getpass.getpass('Password: '))
                i = self.expect(expected)
            if i == 4:  # Request for terminal type
                expect.sendline('ascii')
                i = self.expect(expected)

            # Second phase
            if i == 1:  # Another request to authorize host certificate (i.e. host not in the 'known_hosts' file)
                raise RuntimeError('Received a second request to authorize host key. This should not have happened!')
            elif i in (2, 3):  # Second request for password/passphrase or rejection of credentials. For now, give up.
                raise DuctAuthenticationError('Invalid username and/or password, or private key is not unlocked.')
            elif i == 4:  # Another request for terminal type.
                raise RuntimeError('Received a second request for terminal type. This should not have happened!')
            elif i == 5:  # Timeout
                # In our instance, this means that we have not handled some or another aspect of the login procedure.
                # Since we are expecting an EOF when we have successfully logged in, hanging means that the SSH login
                # procedure is waiting for more information. Since we have no more to give, this means our login
                # was unsuccessful.
                raise RuntimeError('SSH client seems to be awaiting more information, but we have no more to give. The '
                                   'messages received so far are:\n{}'.format(expect.before))
            elif i == 6:  # Connection closed by remote host
                raise RuntimeError("Remote closed SSH connection")
            elif i == 7:
                raise RuntimeError("Cannot connect to {} on your current network connection".format(self.host))
        finally:
            expect.close()

        # We should be logged in at this point, but let us make doubly sure
        assert self.is_connected(), 'Unexpected failure to establish a connection with the remote host with command: \n ' \
                                    '{}\n\n Please report this!'.format(cmd)

    @override
    def _is_connected(self):
        cmd = "ssh {login} -T -S {socket} -O check".format(login=self._login_info,
                                                           socket=self._socket_path)
        proc = run_in_subprocess(cmd)

        if proc.returncode != 0:
            if os.path.exists(self._socket_path):
                os.remove(self._socket_path)
            return False
        return True

    @override
    def _disconnect(self):
        # Send exit request to control socket.
        cmd = "ssh {login} -T -S {socket} -O exit".format(login=self._login_info,
                                                          socket=self._socket_path)
        run_in_subprocess(cmd)

    # RemoteClient implementation
    @override
    def _execute(self, cmd, skip_cwd=False, **kwargs):
        """
        Additional Args:
            skip_cwd (bool): Whether to skip changing to the current working
                directory associated with this client before executing the
                command. This is mainly useful to methods internal to this
                class.
        """
        template = 'ssh {login} -T -o ControlPath={socket} << EOF\n{cwd}{cmd}\nEOF'
        config = dict(self._subprocess_config)
        config.update(kwargs)

        cwd = 'cd "{path}"\n'.format(path=escape_path(self.path_cwd)) if not skip_cwd else ''
        return run_in_subprocess(template.format(login=self._login_info,
                                                 socket=self._socket_path,
                                                 cwd=cwd,
                                                 cmd=cmd),
                                 check_output=True,
                                 **config)

    @override
    @require_connection
    def _port_forward_start(self, local_port, remote_host, remote_port):
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

    @override
    def _port_forward_stop(self, local_port, remote_host, remote_port, connection):
        logger.info('Cancelling port forward...')
        cmd_template = 'ssh {login} -T -O cancel -S {socket} -L localhost:{local_port}:{remote_host}:{remote_port}'
        cmd = cmd_template.format(login=self._login_info,
                                  socket=self._socket_path,
                                  local_port=local_port,
                                  remote_host=remote_host,
                                  remote_port=remote_port)
        proc = run_in_subprocess(cmd)
        logger.info('Port forward succesfully stopped.' if proc.returncode == 0 else 'Failed to stop port forwarding.')

    @override
    def _is_port_bound(self, host, port):
        return self.execute('which nc; if [ $? -eq 0 ]; then nc -z -w2 {} {}; fi'.format(host, port)).returncode == 0

    # FileSystem methods

    # Path properties and helpers
    @override
    def _path_home(self):
        return self.execute('echo ~', skip_cwd=True).stdout.decode('utf-8').strip()

    @override
    def _path_separator(self):
        return '/'

    # File node properties
    @override
    def _exists(self, path):
        return self.execute('if [ ! -e {} ]; then exit 1; fi'.format(path)).returncode == 0

    @override
    def _isdir(self, path):
        return self.execute('if [ ! -d {} ]; then exit 1; fi'.format(path)).returncode == 0

    @override
    def _isfile(self, path):
        return self.execute('if [ ! -f {} ]; then exit 1; fi'.format(path)).returncode == 0

    # Directory handling and enumeration
    @override
    def _dir(self, path):
        # TODO: Currently we strip link annotations below with ...[:9]. Should we capture them?
        dir = pd.DataFrame(sorted([re.split(r'\s+', f)[:9] for f in self.execute('ls -Al {}'.format(path)).stdout.decode('utf-8').strip().split('\n')[1:]]),
                           columns=['file_mode', 'link_count', 'owner', 'group', 'bytes', 'month', 'day', 'time', 'path'])

        def convert_to_datetime(x):
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            year = datetime.datetime.now().year if ':' in x.time else x.time
            time = x.time if ':' in x.time else None
            return datetime.datetime(
                year=int(year),
                month=months.index(x.month) + 1,
                day=int(x.day),
                hour=int(time.split(':')[0]) if time is not None else 0,
                minute=int(time.split(':')[1]) if time is not None else 0
            )

        if len(dir) == 0:  # Directory is empty
            return

        dir = dir.assign(
            last_modified=lambda x: x.apply(convert_to_datetime, axis=1),
            type=lambda x: x.apply(lambda x: 'directory' if x.file_mode.startswith('d') else 'file', axis=1)
        ).drop(
            ['month', 'day', 'time'],
            axis=1
        ).sort_values(
            ['type', 'path']
        ).reset_index(drop=True)

        for i, row in dir.iterrows():
            yield FileSystemFileDesc(
                fs=self,
                path=posixpath.join(path, row.path),
                name=row.path,
                type='directory' if row.file_mode.startswith('d') else 'file',  # TODO: What about links, which are of form: lrwxrwxrwx?
                bytes=row.bytes,
                owner=row.owner,
                group=row.group,
                last_modified=row.last_modified,
            )

    @override
    def _mkdir(self, path, recursive, exist_ok):
        if exist_ok and self.isdir(path):
            return
        assert self.execute('mkdir ' + ('-p ' if recursive else '') + '"{}"'.format(path)).returncode == 0, "Failed to create directory at: `{}`".format(path)

    @override
    def _remove(self, path, recursive):
        assert self.execute('rm -f ' + ('-r ' if recursive else '') + '"{}"'.format(path)).returncode == 0, "Failed to remove file(s) at: `{}`".format(path)

    # File handling
    @override
    def _file_read_(self, path, size=-1, offset=0, binary=False):
        read = self.execute('cat {}'.format(path)).stdout
        if not binary:
            read = read.decode('utf-8')
        return read

    @override
    def _file_append_(self, path, s, binary):
        raise NotImplementedError

    @override
    def _file_write_(self, path, s, binary):
        if binary:
            fd, tmp_path = tempfile.mkstemp()
        else:
            fd, tmp_path = tempfile.mkstemp(text=True)
        os.close(fd)

        with open(tmp_path, 'w' + ('b' if binary else ''), encoding=None if binary else 'utf-8') as f:
            f.write(s)

        return self.upload(tmp_path, path, overwrite=True)

    # File transfer
    @override
    @require_connection
    def download(self, source, dest=None, overwrite=False, fs=None):
        """
        Download files to another filesystem.

        This method (recursively) downloads a file/folder from path `source` on
        this filesystem to the path `dest` on filesytem `fs`, overwriting any
        existing file if `overwrite` is `True`.

        Args:
            source (str): The path on this filesystem of the file to download to
                the nominated filesystem (`fs`). If `source` ends
                with '/' then contents of the the `source` directory will be
                copied into destination folder, and will throw an error if path
                does not resolve to a directory.
            dest (str): The destination path on filesystem (`fs`). If not
                specified, the file/folder is uploaded into the default path,
                usually one's home folder. If `dest` ends with '/',
                and corresponds to a directory, the contents of source will be
                copied instead of copying the entire folder. If `dest` is
                otherwise a directory, an exception will be raised.
            overwrite (bool): `True` if the contents of any existing file by the
                same name should be overwritten, `False` otherwise.
            fs (FileSystemClient): The FileSystemClient into which the nominated
                file/folder `source` should be downloaded. If not specified,
                defaults to the local filesystem.

        SSHClient Quirks:
            This method is overloaded so that remote-to-local downloads can be
            handled specially using `scp`. Downloads to any non-local filesystem
            are handled using the standard implementation.
        """
        from ..filesystems.local import LocalFsClient

        if fs is None or isinstance(fs, LocalFsClient):
            logger.info('Copying file to local...')
            dest = dest or posixpath.basename(source)
            cmd = (
                "scp -r -o ControlPath={socket} {login}:'{remote_file}' '{local_file}'".format(
                    socket=self._socket_path,
                    login=self._login_info,
                    remote_file=dest.replace('"', r'\"'),
                    local_file=source.replace('"', r'\"'),  # quote escaped for bash
                )
            )
            proc = run_in_subprocess(cmd, check_output=True)
            logger.info(proc.stderr or 'Success')
        else:
            return super(RemoteClient, self).download(source, dest, overwrite, fs)

    @override
    @require_connection
    def upload(self, source, dest=None, overwrite=False, fs=None):
        """
        Upload files from another filesystem.

        This method (recursively) uploads a file/folder from path `source` on
        filesystem `fs` to the path `dest` on this filesytem, overwriting any
        existing file if `overwrite` is `True`. This is equivalent to
        `fs.download(..., fs=self)`.

        Args:
            source (str): The path on the specified filesystem (`fs`) of the
                file to upload to this filesystem. If `source` ends with '/',
                and corresponds to a directory, the contents of source will be
                copied instead of copying the entire folder.
            dest (str): The destination path on this filesystem. If not
                specified, the file/folder is uploaded into the default path,
                usually one's home folder, on this filesystem. If `dest` ends
                with '/' then file will be copied into destination folder, and
                will throw an error if path does not resolve to a directory.
            overwrite (bool): `True` if the contents of any existing file by the
                same name should be overwritten, `False` otherwise.
            fs (FileSystemClient): The FileSystemClient from which to load the
                file/folder at `source`. If not specified, defaults to the local
                filesystem.

        SSHClient Quirks:
            This method is overloaded so that local-to-remote uploads can be
            handled specially using `scp`. Uploads to any non-local filesystem
            are handled using the standard implementation.
        """
        from ..filesystems.local import LocalFsClient

        if fs is None or isinstance(fs, LocalFsClient):
            logger.info('Copying file from local...')
            dest = dest or posixpath.basename(source)
            cmd = (
                "scp -r -o ControlPath={socket} '{local_file}' {login}:'{remote_file}'".format(
                    socket=self._socket_path,
                    local_file=source.replace('"', r'\"'),  # quote escaped for bash
                    login=self._login_info,
                    remote_file=dest.replace('"', r'\"'),
                )
            )
            proc = run_in_subprocess(cmd, check_output=True)
            logger.info(proc.stderr or 'Success')
        else:
            return super(RemoteClient, self).upload(source, dest, overwrite, fs)

    # Helper methods

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

    def update_host_keys(self):
        """
        Update host keys associated with this remote.

        This method updates the SSH host-keys stored in `~/.ssh/known_hosts`,
        allowing one to successfully connect to hosts when servers are,
        for example, redeployed and have different host keys.
        """
        assert not self.remote, "Updating host key only works for local connections."
        cmd = "ssh-keygen -R {host} && ssh-keyscan {host} >> ~/.ssh/known_hosts".format(host=self.host)
        proc = run_in_subprocess(cmd, True)
        if proc.returncode != 0:
            raise RuntimeError(
                "Could not update host keys! Please handle this manually. The "
                "error was:\n" + '\n'.join([proc.stdout.decode('utf-8'), proc.stderr.decode('utf-8')])
            )
