import datetime
import errno
import os
import shutil
import six
import sys
from io import open

from .base import FileSystemClient, FileSystemFileDesc


class LocalFsClient(FileSystemClient):
    """
    `LocalFsClient` is a `Duct` that implements the `FileSystemClient` common
    API, and exposes the local filesystem.

    Unlike most other filesystems, `LocalFsClient` defaults to the current
    working directory on the local machine, rather than the home directory
    as used on remote filesystems. To change this, you can always execute:
    ```
    local_fs.path_cwd = local_fs.path_home
    ```
    """

    PROTOCOLS = ['localfs']

    def _init(self):
        self._path_cwd = self._path_cwd or os.getcwd()

    def _prepare(self):
        assert self.remote is None, "LocalFsClient cannot be used in conjunction with a remote client."
        super(LocalFsClient, self)._prepare()

    def _connect(self):
        pass

    def _is_connected(self):
        return True

    def _disconnect(self):
        pass

    # File enumeration
    def _path_home(self):
        return os.path.expanduser('~')

    def _path_separator(self):
        return os.path.sep

    def _exists(self, path):
        return os.path.exists(path)

    def _isdir(self, path):
        return os.path.isdir(path)

    def _isfile(self, path):
        return os.path.isfile(path)

    def _dir(self, path):
        if not os.path.isdir(path):
            raise RuntimeError("No such folder.")
        for f in os.listdir(path):
            f_path = os.path.join(path, f)

            attrs = {}

            if os.name == 'posix':
                import grp
                import pwd

                stat = os.stat(f_path)

                attrs.update({
                    'owner': pwd.getpwuid(stat.st_uid).pw_name,
                    'group': grp.getgrgid(stat.st_gid).gr_name,
                    'permissions': oct(stat.st_mode),
                    'created': str(datetime.datetime.fromtimestamp(stat.st_ctime)),
                    'last_modified': str(datetime.datetime.fromtimestamp(stat.st_mtime)),
                    'last_accessed': str(datetime.datetime.fromtimestamp(stat.st_atime)),
                })

            yield FileSystemFileDesc(
                fs=self,
                path=f_path,
                name=f,
                type='directory' if os.path.isdir(f_path) else 'file',
                bytes=os.path.getsize(f_path),
                **attrs
            )

    def _walk(self, path):
        return os.walk(path)

    def _mkdir(self, path, recursive, exist_ok):
        try:
            os.makedirs(path) if recursive else os.mkdir(path)
        except OSError as exc:  # Python >2.5
            if exc.errno != errno.EEXIST or not exist_ok or not os.path.isdir(path):
                six.reraise(*sys.exc_info())

    def _remove(self, path, recursive):
        if recursive and self.isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)

    # File opening

    def _open(self, path, mode):
        return open(path, mode=mode, encoding=None if 'b' in mode else 'utf-8')
