import datetime
import sys
from abc import abstractmethod

import six
import yaml
from decorator import decorator

from omniduct.duct import Duct
from omniduct.utils.config import config
from omniduct.utils.debug import logger
from omniduct.utils.decorators import function_args_as_kwargs
from omniduct.utils.docs import quirk_docs

from ._serializers import PickleSerializer

config.register('cache_fail_hard',
                description='Raise exception if cache fails to save.',
                default=False)


def cached_method(
        key,
        namespace=lambda self, kwargs: (
            self.cache_namespace or "{}.{}".format(self.__class__.__name__, self.name)
        ),
        cache=lambda self, kwargs: self.cache,
        use_cache=lambda self, kwargs: kwargs.pop('use_cache', True),
        renew=lambda self, kwargs: kwargs.pop('renew', False),
        serializer=lambda self, kwargs: PickleSerializer(),
        metadata=lambda self, kwargs: None
):
    @decorator
    def wrapped(method, self, *args, **kwargs):
        kwargs = function_args_as_kwargs(method, self, *args, **kwargs)
        kwargs.pop('self')

        _key = key(self, kwargs)
        _namespace = namespace(self, kwargs)
        _cache = cache(self, kwargs)
        _use_cache = use_cache(self, kwargs)
        _renew = renew(self, kwargs)
        _serializer = serializer(self, kwargs)
        _metadata = metadata(self, kwargs)

        if _cache is None or not _use_cache:
            return method(self, **kwargs)

        if _renew or not _cache.has_key(_key, namespace=_namespace):  # noqa: has_key is not of a dictionary here
            value = method(self, **kwargs)
            if value is None:
                logger.warning("Method value returned None. Not saving to cache.")
                return

            try:
                _cache.set(
                    _key,
                    value=value,
                    namespace=_namespace,
                    serializer=_serializer,
                    metadata=_metadata
                )
            except:
                logger.warning("Failed to save results to cache. If needed, please save them manually.")
                if config.cache_fail_hard:
                    six.reraise(*sys.exc_info())
        else:
            logger.caveat('Loaded from cache')

        # Return from cache every time, just in case serialization operation was
        # destructive (e.g. reading from cursors)
        return _cache.get(
            _key,
            namespace=_namespace,
            serializer=_serializer
        )
    return wrapped


class Cache(Duct):
    """
    `Cache` is an abstract subclass of `Duct` that provides a common
    API for all cache clients, which in turn will be subclasses of this
    class.
    """

    DUCT_TYPE = Duct.Type.CACHE

    @quirk_docs('_init', mro=True)
    def __init__(self, **kwargs):
        Duct.__init_with_kwargs__(self, kwargs)
        self._init(**kwargs)

    @abstractmethod
    def _init(self):
        pass

    # Data insertion and retrieval

    def set(self, key, value, namespace=None, serializer=None, metadata=None):
        """
        Set the value of a key.

        Parameters:
            key (str): The key for which `value` should be stored.
            value (*): The value to be stored.
            namespace (str, None): The namespace to be used.
            serializer (Serializer): The `Serializer` subclass to use for the
                serialisation of value into the cache. (default=PickleSerializer)
            metadata (dict, None): Additional metadata to be stored with the value
                in the cache. Values must be serializable via `yaml.safe_dump`.
        """
        self.connect()
        namespace, key = self._namespace(namespace), self._key(key)
        serializer = serializer or PickleSerializer()
        try:
            with self._get_stream_for_key(namespace, key, 'data{}'.format(serializer.file_extension), mode='wb', create=True) as fh:
                serializer.serialize(value, fh)
            self.set_metadata(key, metadata, namespace=namespace, replace=True)
        except:
            self.unset(key, namespace=namespace)
            six.reraise(*sys.exc_info())

    def set_metadata(self, key, metadata, namespace=None, replace=False):
        """
        Set the metadata associated with a stored key, creating the key if it
        is missing.

        Parameters:
            key (str): The key for which `value` should be stored.
            metadata (dict, None): Additional/override metadata to be stored
                for `key` in the cache. Values must be serializable via
                `yaml.safe_dump`.
            namespace (str, None): The namespace to be used.
            replace (bool): Whether the provided metadata should entirely
                replace any existing metadata, or just update it. (default=False)
        """
        self.connect()
        namespace, key = self._namespace(namespace), self._key(key)
        if replace:
            orig_metadata = {'created': datetime.datetime.utcnow()}
        else:
            orig_metadata = self.get_metadata(key, namespace=namespace)

        orig_metadata.update(metadata or {})

        with self._get_stream_for_key(namespace, key, 'metadata', mode='w', create=True) as fh:
            yaml.safe_dump(orig_metadata, fh, default_flow_style=False)

    def get(self, key, namespace=None, serializer=None):
        """
        Retrieve the value associated with the nominated key from the cache.

        Parameters:
            key (str): The key for which `value` should be retrieved.
            namespace (str, None): The namespace to be used.
            serializer (Serializer): The `Serializer` subclass to use for the
                deserialisation of value from the cache. (default=PickleSerializer)
        """
        self.connect()
        namespace, key = self._namespace(namespace), self._key(key)
        serializer = serializer or PickleSerializer()
        if not self._has_key(namespace, key):
            raise KeyError("{} (namespace: {})".format(key, namespace))
        try:
            with self._get_stream_for_key(namespace, key, 'data{}'.format(serializer.file_extension), mode='rb', create=False) as fh:
                return serializer.deserialize(fh)
        finally:
            self.set_metadata(key, namespace=namespace, metadata={'last_accessed': datetime.datetime.utcnow()})

    def get_metadata(self, key, namespace=None):
        """
        Retrieve metadata associated with the nominated key from the cache.

        Parameters:
            key (str): The key for which to extract metadata.
            namespace (str, None): The namespace to be used.
        """
        self.connect()
        namespace, key = self._namespace(namespace), self._key(key)
        if not self._has_key(namespace, key):
            raise KeyError("{} (namespace: {})".format(key, namespace))
        try:
            with self._get_stream_for_key(namespace, key, 'metadata', mode='r', create=False) as fh:
                return yaml.safe_load(fh)
        except:
            return {}

    def unset(self, key, namespace=None):
        """
        Remove the nominated key from the cache.

        Parameters:
            key (str): The key which should be unset.
            namespace (str, None): The namespace to be used.
        """
        self.connect()
        namespace, key = self._namespace(namespace), self._key(key)
        if not self._has_key(namespace, key):
            raise KeyError("{} (namespace: {})".format(key, namespace))
        self._remove_key(namespace, key)

    def unset_namespace(self, namespace=None):
        """
        Remove an entire namespace from the cache.

        Parameters:
            namespace (str, None): The namespace to be removed.
        """
        self.connect()
        namespace = self._namespace(namespace)
        if not self._has_namespace(namespace):
            raise KeyError("namespace: {}".format(namespace))
        self._remove_namespace(namespace)

    # Top-level descriptions

    @property
    def namespaces(self):
        "list <str,None>: A list of the namespaces stored in the cache."
        return self.connect()._get_namespaces()

    def has_namespace(self, namespace=None):
        """
        Check whether the cache has the nominated namespace.

        Parameters:
            namespace (str,None): The namespace for which to check for existence.
        """
        self.connect()
        namespace = self._namespace(namespace)
        return self._has_namespace(namespace)

    def keys(self, namespace=None):
        """
        Collect a list of all the keys present in the nominated namespaces.

        Parameters:
            namespace (str,None): The namespace from which to extract all of the
                keys.
        """
        self.connect()
        namespace = self._namespace(namespace)
        return self._get_keys(namespace)

    def has_key(self, key, namespace=None):
        """
        Check whether the cache as a nominated key.

        Parameters:
            key (str): The key for which to check existence.
            namespace (str,None): The namespace from which to extract all of the
                keys.
        """
        self.connect()
        namespace, key = self._namespace(namespace), self._key(key)
        return self._has_key(namespace, key)

    # Methods for subclasses to implement

    def _namespace(self, namespace):
        return namespace

    def _key(self, key):
        return key

    @abstractmethod
    def _get_namespaces(self):
        raise NotImplementedError

    def _has_namespace(self, namespace):
        return namespace in self._get_namespaces()

    @abstractmethod
    def _remove_namespace(self, namespace):
        raise NotImplementedError

    @abstractmethod
    def _get_keys(self, namespace):
        raise NotImplementedError

    def _has_key(self, namespace, key):
        return key in self._get_keys(namespace=namespace)

    @abstractmethod
    def _remove_key(self, namespace, key):
        raise NotImplementedError

    @abstractmethod
    def _get_stream_for_key(self, namespace, key, stream_name, mode, create):
        pass
