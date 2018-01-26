import datetime
import errno
import os

from .base import FileSystemClient, FileSystemFileDesc


class LocalFsClient(FileSystemClient):

    PROTOCOLS = ['localfs']
    DEFAULT_PORT = 22

    def _init(self, cwd_as_home=True):
        self.__cwd_as_home = cwd_as_home

    def _connect(self):
        pass

    def _is_connected(self):
        return True

    def _disconnect(self):
        pass

    # File enumeration
    def _path_home(self):
        return os.getcwd() if self.__cwd_as_home else os.path.expanduser('~')

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

    def _mkdir(self, path, recursive):
        try:
            os.makedirs(path) if recursive else os.makedir(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    # File opening

    def _file_read_(self, path, size=-1, offset=0, binary=False):
        with open(path, 'r{}'.format('b' if binary else '')) as f:
            return f.read()

    def _file_append_(self, path, s, binary):
        raise NotImplementedError

    def _file_write_(self, path, s, binary):
        with open(path, 'w' + ('b' if binary else '')) as f:
            return f.write(s)
