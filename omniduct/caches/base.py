import inspect
import pickle
import sys
from abc import abstractmethod

import six
from decorator import decorator

from omniduct.duct import Duct

from ..utils.debug import logger


def cached_method(id_str,
                  cache=lambda self: self.cache,
                  id_duct=lambda self, kwargs: "{}.{}".format(self.__class__.__name__, self.name),
                  use_cache=lambda self, kwargs: kwargs.pop('use_cache', True),
                  renew=lambda self, kwargs: kwargs.pop('renew', False),
                  encoder=pickle.dumps,
                  decoder=pickle.loads):
    @decorator
    def wrapped(method, self, *args, **kwargs):
        if six.PY3 and not hasattr(sys, 'pypy_version_info'):
            arguments = inspect.signature(method).parameters.keys()
        else:
            arguments = inspect.getargspec(method).args
        kwargs.update(dict(zip(list(arguments)[1:], args)))

        _cache = cache(self)
        _use_cache = use_cache(self, kwargs)
        _renew = renew(self, kwargs)

        if _cache is None or not _use_cache:
            return method(self, **kwargs)

        _id_duct = id_duct(self, kwargs)
        _id_str = id_str(self, kwargs)

        if _renew or not _cache.has_key(_id_duct, _id_str):  # noqa: has_key is not of a dictionary here
            value = method(self, **kwargs)
            try:
                _cache.set(
                    id_duct=_id_duct,
                    id_str=_id_str,
                    value=value,
                    encoder=encoder
                )
            except Exception:  # Remove any lingering (perhaps partial) cache files
                _cache.clear(
                    id_duct=_id_duct,
                    id_str=_id_str
                )
                logger.warning("Failed to save results to cache. If needed, please save them manually.")
                # TODO: reraise exception if a qualifying debug flag is set.
            return value

        logger.caveat('Loaded from cache')

        return _cache.get(
            id_duct=_id_duct,
            id_str=_id_str,
            decoder=decoder
        )
    return wrapped


class Cache(Duct):

    DUCT_TYPE = Duct.Type.CACHE

    def __init__(self, *args, **kwargs):
        '''
        This is a shim __init__ function that passes all arguments onto
        `self._init`, which is implemented by subclasses. This allows subclasses
        to instantiate themselves with arbitrary parameters.
        '''
        Duct.__init_with_kwargs__(self, kwargs)
        self._init(*args, **kwargs)

    @abstractmethod
    def _init(self):
        pass

    @abstractmethod
    def clear(self, id_duct, id_str):
        pass

    @abstractmethod
    def clear_all(self, id_duct=None):
        pass

    @abstractmethod
    def get(self, id_duct, id_str, decoder=pickle.loads):
        pass

    @abstractmethod
    def has_key(self, id_duct, id_str):
        pass

    @abstractmethod
    def keys(self, id_duct):
        pass

    @abstractmethod
    def set(self, id_duct, id_str, value, encoder=pickle.dumps):
        pass
