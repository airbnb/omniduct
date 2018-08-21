import datetime
from abc import abstractmethod

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
        namespace=lambda self, kwargs: "{}.{}".format(self.__class__.__name__, self.name),
        cache=lambda self, kwargs: self.cache,
        use_cache=lambda self, kwargs: kwargs.pop('use_cache', True),
        renew=lambda self, kwargs: kwargs.pop('renew', False),
        serializer=lambda self, kwargs: PickleSerializer,
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
            try:
                _cache.set(
                    _key,
                    value=value,
                    namespace=_namespace,
                    serializer=_serializer,
                    metadata=_metadata
                )
            except Exception:  # Remove any lingering (perhaps partial) cache files
                _cache.unset(
                    _key,
                    namespace=_namespace
                )
                logger.warning("Failed to save results to cache. If needed, please save them manually.")
                if config.cache_fail_hard:
                    raise
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
        """
        This is a shim __init__ function that passes all arguments onto
        `self._init`, which is implemented by subclasses. This allows subclasses
        to instantiate themselves with arbitrary parameters.
        """
        Duct.__init_with_kwargs__(self, kwargs)
        self._init(**kwargs)

    @abstractmethod
    def _init(self):
        pass

    # Data insertion and retrieval

    def set(self, key, value, namespace=None, serializer=PickleSerializer, expires=None, metadata=None):
        namespace, key = self._namespace(namespace), self._key(key)
        # try:
        self.set_metadata(key, metadata, namespace=namespace, replace=True)
        with self._get_stream_for_key(namespace, key, 'data{}'.format(serializer.file_extension), mode='wb', create=True) as fh:
            return serializer.serialize(value, fh)
        # except:
        #     self.unset(key, namespace=namespace)

    def set_metadata(self, key, metadata, namespace=None, replace=False):
        namespace, key = self._namespace(namespace), self._key(key)
        if replace:
            orig_metadata = {'created': datetime.datetime.utcnow()}
        else:
            orig_metadata = self.get_metadata(key, namespace=namespace)

        orig_metadata.update(metadata or {})

        with self._get_stream_for_key(namespace, key, 'metadata', mode='w', create=True) as fh:
            yaml.safe_dump(orig_metadata, fh, default_flow_style=False)

    def get(self, key, namespace=None, serializer=PickleSerializer):
        namespace, key = self._namespace(namespace), self._key(key)
        try:
            with self._get_stream_for_key(namespace, key, 'data{}'.format(serializer.file_extension), mode='rb', create=False) as fh:
                return serializer.deserialize(fh)
        finally:
            self.set_metadata(key, namespace=namespace, metadata={'last_accessed': datetime.datetime.utcnow()})

    def get_metadata(self, key, namespace=None):
        namespace, key = self._namespace(namespace), self._key(key)
        try:
            with self._get_stream_for_key(namespace, key, 'metadata', mode='r', create=True) as fh:
                return yaml.safe_load(fh)
        except:
            return {}

    def unset(self, key, namespace=None):
        namespace, key = self._namespace(namespace), self._key(key)
        self._remove_key(namespace, key)

    def unset_all(self, namespace):
        namespace = self._namespace(namespace)
        self._remove_namespace(namespace)

    # Top-level descriptions

    @property
    def namespaces(self):
        return self._get_namespaces()

    def has_namespace(self, namespace):
        namespace = self._namespace(namespace)
        return self._has_namespace(namespace)

    def keys(self, namespace=None):
        namespace = self._namespace(namespace)
        return self._get_keys(namespace)

    def has_key(self, key, namespace=None):
        namespace, key = self._namespace(namespace), self._key(key)
        return self._has_key(namespace, key)

    # Cache maintenance

    def cleanup(self):
        pass

    def get_resource_usage(self):
        pass

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
