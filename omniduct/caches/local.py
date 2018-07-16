import hashlib
import os
import pickle
import shutil
import sys

import six

from omniduct.utils.debug import logger
from omniduct.utils.storage import ensure_path_exists

from .base import Cache


class LocalCache(Cache):
    """
    `LocalCache` uses the local filesystem (at a nominated directory) to store
    the cache.

    Note: This cache will be replaced with a `FileSystemCache`, which is
    similar but based on the `FileSystemClient` API rather than directly accessing
    the local filesystem.
    """

    PROTOCOLS = ['local_cache']

    def _init(self, dir):
        """
        dir (str): The path to act as the parent directory for the cache.
        """
        self.dir = dir

    @property
    def dir(self):
        """
        str: The path to act as the parent directory for the cache.
        """
        return ensure_path_exists(self._dir)

    @dir.setter
    def dir(self, dir):
        self._dir = dir

    @classmethod
    def get_hash(cls, id_str):
        """
        Get a unique key for the given id_str by taking its sha1 hash.

        Parameters:
            id_str (str): The id_str to be hashed.

        Returns:
            str: The sha1 hash of the id_str.
        """
        if sys.version_info.major == 3 or sys.version_info.major == 2 and isinstance(id_str, unicode):  # noqa: F821
            id_str = id_str.encode('utf8')
        return hashlib.sha1(id_str).hexdigest()

    def get_path(self, id_duct, id_str, create=False):
        hash = self.get_hash(id_str)
        if isinstance(id_duct, six.string_types):
            id_duct = id_duct.split('.')
        path = os.path.join(os.path.join(self.dir, *id_duct), hash)
        if create:
            ensure_path_exists(os.path.dirname(path))
        return path

    # Duct Methods
    def _connect(self):
        pass

    def _is_connected(self):
        return True

    def _disconnect(self):
        pass

    # Cache implementations
    def clear(self, id_duct, id_str):
        try:
            os.remove(self.get_path(id_duct, id_str))
        except:
            pass

    def clear_all(self, id_duct=None):
        cache_path = self.dir if id_duct is None else os.path.dirname(self.get_path(id_duct, 'None'))
        shutil.rmtree(cache_path)

    def get(self, id_duct, id_str, deserializer=pickle.load):
        cache_path = self.get_path(id_duct, id_str)
        if not os.path.exists(cache_path):
            return None
        with open(cache_path, 'rb') as f:
            logger.debug("Loading local cache entry from '{}'.".format(cache_path))
            return deserializer(f)

    def has_key(self, id_duct, id_str):
        return os.path.exists(self.get_path(id_duct, id_str))

    def keys(self, id_dict):
        raise NotImplementedError()

    def set(self, id_duct, id_str, value, serializer=pickle.dump):
        cache_path = self.get_path(id_duct, id_str, create=True)
        with open(cache_path, 'wb') as f:
            return serializer(value, f)
