from azure.datalake.store import core, lib
from omniduct.filesystems.base import FileSystemClient
from overrides import overrides
import os


class AzureDataLakeClient(FileSystemClient):

    PROTOCOLS = ['azure_data_lake']
    DEFAULT_PORT = None

    def _init(self, tenant_id=None, username=None, password=None, store_name=None, **kwargs):
        self.__tenant_id = tenant_id
        self.__username = username
        self.__password = password
        self.__store_name = store_name

        self.global_writes = True

        self._adls_account = None

    def _create_token(self):
        token = lib.auth(self.__tenant_id, self.__username, self.__password)
        return token
    @overrides
    def _connect(self):
        if self._adls_account is None:
            token = self._create_token()
            self._adls_account = core.AzureDLFileSystem(token, store_name=self.__store_name)
    @overrides
    def _is_connected(self):
        return self._adls_account is not None
    @overrides
    def _disconnect(self):
        self._adls_account = None

    # File operations
    @overrides
    def _dir(self, path):
        return os.path.dirname(path)
    @overrides
    def _path_home(self):
        return os.path.expanduser("~")
    @overrides
    def _path_separator(self):
        return os.path.sep
    
    def _list(self, path, details=False, **kwargs):
        files = self._adls_account.ls(path, detail=details, invalidate_cache=True)
        if details:
            return files
        else:
            return files

    @overrides
    def _exists(self, path):
        return self._adls_account.exists(path)
    @overrides
    def _isdir(self, path, **kwargs):
        try:
            path_info = self._adls_account.info(path)
            return path_info['type'] == 'DIRECTORY'
        except FileNotFoundError:
            return False


    @overrides
    def _isfile(self, path, **kwargs):
        try:
            path_info = self._adls_account.info(path)
            return path_info['type'] == 'FILE'
        except FileNotFoundError:
            return False

    @overrides
    def _mkdir(self, path, recursive, exist_ok):
        if recursive:
            paths_to_create = []
            current_path = path

            while not self._exists(current_path):
                paths_to_create.append(current_path)
                current_path = os.path.dirname(current_path)

            for p in reversed(paths_to_create):
                self._adls_account.mkdir(p)
        else:
            self._adls_account.mkdir(path)

    @overrides
    def _remove(self, path, recursive):
        if self._exists(path):
            self._adls_account.rm(path, recursive=recursive)
        else:
            print(f"Path '{path}' does not exist.")


    # Read/write operations
    @overrides
    def _open(self, path, mode):
        return self._adls_account.open(path, mode=mode)

    def _read(self, path, encoding='utf8', **kwargs):
        with self._open(path, mode='rb') as f:
            content = f.read()
        return content.decode(encoding)

    def _write(self, path, content, encoding='utf8', **kwargs):
        with self._open(path, mode='wb') as f:
            f.write(content.encode(encoding))
