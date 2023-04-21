from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from omniduct.filesystems.base import FileSystemClient, FileSystemFileDesc
import os


class AzureDataLakeClient(FileSystemClient):
    
    PROTOCOLS = ['azure_data_lake']
    DEFAULT_PORT = None

    def _init(self, tenant_id, client_id, client_secret, store_name):
        self.__tenant_id = tenant_id
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__store_name = store_name
        
        self.global_writes = True

        self._adls_account = None



    def _connect(self):
        if self._adls_account is None:
            credential = ClientSecretCredential(
                tenant_id=self.__tenant_id,
                client_id=self.__client_id,
                client_secret=self.__client_secret
            )
            self._adls_account = DataLakeServiceClient(
                account_url="https://{}.dfs.core.windows.net".format(self.__store_name),
                credential=credential
            )


    def _is_connected(self):
        return self._adls_account is not None

    def _disconnect(self):
        self._adls_account = None
        
        # File operations

    def _dir(self, path):
        return os.path.dirname(path)

    def _path_home(self):
        return os.path.expanduser("~")

    def _path_separator(self):
        return os.path.sep

    def _list(self, path, details=False, **kwargs):
        files = self._adls_account.ls(path, detail=details, invalidate_cache=True)
        if details:
            return files
        else:
            return [f['name'] for f in files]

    def _exists(self, path):
        return self._adls_account.exists(path)

    def _isdir(self, path):
        file_system_client = self._adls_account.get_file_system_client(file_system=self.__store_name)
        try:
            directory_client = file_system_client.get_directory_client(path)
            directory_client.get_directory_properties()
            return True
        except:
            return False


    def _isfile(self, path):
        return self._adls_account.isfile(path)

    def _mkdir(self, path, recursive=True, exist_ok=True, **kwargs):
        if recursive:
            self._adls_account.makedirs(path, exist_ok=exist_ok)
        else:
            self._adls_account.mkdir(path)

    def _remove(self, path, recursive=True, **kwargs):
        self._adls_account.rm(path, recursive=recursive)

    # Read/write operations

    def _open(self, path, mode='r', **kwargs):
        return self._adls_account.open(path, mode=mode)

    def _read(self, path, encoding='utf8', **kwargs):
        with self._open(path, mode='rb') as f:
            content = f.read()
        return content.decode(encoding)

    def _write(self, path, content, mode='w', encoding='utf8', **kwargs):
        if mode not in ['w', 'wb']:
            raise NotImplementedError("AzureDataLakeClient only supports writing in 'w' and 'wb' modes.")

        if mode == 'w':
            content = content.encode(encoding)

        with self._open(path, mode='wb') as f:
            f.write(content)
