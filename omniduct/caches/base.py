from __future__ import annotations

import datetime
import functools
from abc import abstractmethod
from collections.abc import Callable
from typing import IO, Any, cast

import dateutil
import pandas
import yaml
from decorator import decorator
from interface_meta import inherit_docs

from omniduct.duct import Duct
from omniduct.utils.config import config
from omniduct.utils.debug import logger
from omniduct.utils.decorators import function_args_as_kwargs, require_connection

from ._serializers import PickleSerializer, Serializer

config.register(
    "cache_fail_hard",
    description="Raise an exception if a cache fails to save (otherwise errors are logged and suppressed).",
    default=False,
)


def cached_method(
    key: Callable[[Any, dict[str, Any]], str],
    namespace: Callable[[Any, dict[str, Any]], str] = lambda self, kwargs: (
        self.cache_namespace or f"{self.__class__.__name__}.{self.name}"
    ),
    cache: Callable[[Any, dict[str, Any]], Cache | None] = lambda self, kwargs: (
        self.cache
    ),
    use_cache: Callable[[Any, dict[str, Any]], bool] = lambda self, kwargs: kwargs.pop(
        "use_cache", True
    ),
    renew: Callable[[Any, dict[str, Any]], bool] = lambda self, kwargs: kwargs.pop(
        "renew", False
    ),
    serializer: Callable[[Any, dict[str, Any]], Serializer] = lambda self, kwargs: (
        PickleSerializer()
    ),
    metadata: Callable[
        [Any, dict[str, Any]], dict[str, Any] | None
    ] = lambda self, kwargs: None,
) -> Callable:
    """
    Wrap a method of a `Duct` class and add caching capabilities.

    All arguments of this function are expected to be functions taking two
    arguments: a reference to current instance of the class (`self`) and a
    dictionary of arguments passed to the function (`kwargs`).

    Args:
        key: The key under which the value returned by the wrapped function
            should be stored.
        namespace: The namespace under which the key should be stored
            (default: `"<duct class name>.<duct instance name>"`).
        cache: The instance of cache via which to store the output of the
            wrapped function (default: `self.cache`).
        use_cache: Whether or not to use the caching functionality
            (default: `True`).
        renew: Whether to renew the stored cache, overriding if a value has
            already been stored (default: `False`).
        serializer: The `Serializer` subclass to use when storing the return
            object (default: `PickleSerializer`).
        metadata: A dictionary of additional metadata to be stored alongside
            the wrapped function's output (default: `None`).

    Returns:
        The (potentially cached) object returned when calling the wrapped
        function.

    Raises:
        Exception: If cache fails to store the output of the wrapped function,
            and the omniduct configuration key `cache_fail_hard` is `True`, then
            the underlying exceptions raised by the Cache instance will be
            reraised.
    """

    @decorator
    def wrapped(
        method: Callable[..., Any], self: Any, *args: Any, **kwargs: Any
    ) -> Any:
        kwargs = function_args_as_kwargs(method, self, *args, **kwargs)
        kwargs.pop("self")

        _key = key(self, kwargs)
        _namespace = namespace(self, kwargs)
        _cache = cache(self, kwargs)
        _use_cache = use_cache(self, kwargs)
        _renew = renew(self, kwargs)
        _serializer = serializer(self, kwargs)
        _metadata = metadata(self, kwargs)

        if _cache is None or not _use_cache:
            return method(self, **kwargs)

        if _cache.has_key(_key, namespace=_namespace) and not _renew:  # noqa  # has_key is not of a dictionary here
            try:
                return _cache.get(_key, namespace=_namespace, serializer=_serializer)
            except Exception as e:
                logger.warning(
                    "Failed to retrieve results from cache [%s]. Renewing the cache...",
                    e,
                )
                if config.cache_fail_hard:
                    raise
            finally:
                logger.caveat("Loaded from cache")

        # Renewing/creating cache
        value = method(self, **kwargs)
        if value is None:
            logger.warning("Method value returned None. Not saving to cache.")
            return None

        try:
            _cache.set(
                _key,
                value=value,
                namespace=_namespace,
                serializer=_serializer,
                metadata=_metadata,
            )
            # Return from cache every time, just in case serialization operation was
            # destructive (e.g. reading from cursors)
            return _cache.get(_key, namespace=_namespace, serializer=_serializer)
        except:
            logger.warning(
                "Failed to save results to cache. If needed, please save them manually."
            )
            if config.cache_fail_hard:
                raise
            return value  # As a last resort, return value object (which could be mutated by serialization).

    return cast(Callable[..., Any], wrapped)


class Cache(Duct):
    """
    An abstract class providing the common API for all cache clients.
    """

    DUCT_TYPE = Duct.Type.CACHE

    @inherit_docs("_init", mro=True)
    def __init__(self, **kwargs: Any) -> None:
        Duct.__init_with_kwargs__(self, kwargs)
        self._init(**kwargs)

    @abstractmethod
    def _init(self) -> None:
        pass

    # Data insertion and retrieval

    @require_connection
    def set(
        self,
        key: str,
        value: Any,
        namespace: str | None = None,
        serializer: Serializer | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Set the value of a key.

        Args:
            key: The key for which `value` should be stored.
            value: The value to be stored.
            namespace: The namespace to be used.
            serializer: The `Serializer` subclass to use for the serialisation
                of value into the cache. (default=PickleSerializer)
            metadata: Additional metadata to be stored with the value in the
                cache. Values must be serializable via `yaml.safe_dump`.
        """
        namespace, key = self._namespace(namespace), self._key(key)
        serializer = serializer or PickleSerializer()
        try:
            with self._get_stream_for_key(
                namespace,
                key,
                f"data{serializer.file_extension}",
                mode="wb",
                create=True,
            ) as fh:
                serializer.serialize(value, fh)
            self.set_metadata(key, metadata, namespace=namespace, replace=True)
        except:
            self.unset(key, namespace=namespace)
            raise

    @require_connection
    def set_metadata(
        self,
        key: str,
        metadata: dict[str, Any] | None,
        namespace: str | None = None,
        replace: bool = False,
    ) -> None:
        """
        Set the metadata associated with a stored key, creating the key if it
        is missing.

        Args:
            key: The key for which `value` should be stored.
            metadata: Additional/override metadata to be stored for `key` in
                the cache. Values must be serializable via `yaml.safe_dump`.
            namespace: The namespace to be used.
            replace: Whether the provided metadata should entirely replace any
                existing metadata, or just update it. (default=False)
        """
        namespace, key = self._namespace(namespace), self._key(key)
        if replace:
            orig_metadata = {"created": datetime.datetime.utcnow()}
        else:
            orig_metadata = self.get_metadata(key, namespace=namespace)

        orig_metadata.update(metadata or {})

        with self._get_stream_for_key(
            namespace, key, "metadata", mode="w", create=True
        ) as fh:
            yaml.safe_dump(orig_metadata, fh, default_flow_style=False)

    @require_connection
    def get(
        self,
        key: str,
        namespace: str | None = None,
        serializer: Serializer | None = None,
    ) -> Any:
        """
        Retrieve the value associated with the nominated key from the cache.

        Args:
            key: The key for which `value` should be retrieved.
            namespace: The namespace to be used.
            serializer: The `Serializer` subclass to use for the deserialisation
                of value from the cache. (default=PickleSerializer)

        Returns:
            The (appropriately deserialized) object stored in the cache.
        """
        namespace, key = self._namespace(namespace), self._key(key)
        serializer = serializer or PickleSerializer()
        if not self._has_key(namespace, key):
            raise KeyError(f"{key} (namespace: {namespace})")
        try:
            with self._get_stream_for_key(
                namespace,
                key,
                f"data{serializer.file_extension}",
                mode="rb",
                create=False,
            ) as fh:
                return serializer.deserialize(fh)
        finally:
            self.set_metadata(
                key,
                namespace=namespace,
                metadata={"last_accessed": datetime.datetime.utcnow()},
            )

    @require_connection
    def get_bytecount(self, key: str, namespace: str | None = None) -> int:
        """
        Retrieve the number of bytes used by a stored key.

        This bytecount may or may not include metadata storage, depending on
        the backend.

        Args:
            key: The key for which to extract the bytecount.
            namespace: The namespace to be used.

        Returns:
            The number of bytes used by the stored value associated with the
            nominated key and namespace.
        """
        namespace, key = self._namespace(namespace), self._key(key)
        if not self._has_key(namespace, key):
            raise KeyError(f"{key} (namespace: {namespace})")
        return self._get_bytecount_for_key(namespace, key)

    @require_connection
    def get_metadata(self, key: str, namespace: str | None = None) -> dict[str, Any]:
        """
        Retrieve metadata associated with the nominated key from the cache.

        Args:
            key: The key for which to extract metadata.
            namespace: The namespace to be used.

        Returns:
            The metadata associated with this namespace and key.
        """
        namespace, key = self._namespace(namespace), self._key(key)
        if not self._has_key(namespace, key):
            raise KeyError(f"{key} (namespace: {namespace})")
        try:
            with self._get_stream_for_key(
                namespace, key, "metadata", mode="r", create=False
            ) as fh:
                return cast(dict[str, Any], yaml.safe_load(fh))
        except:
            return {}

    @require_connection
    def unset(self, key: str, namespace: str | None = None) -> None:
        """
        Remove the nominated key from the cache.

        Args:
            key: The key which should be unset.
            namespace: The namespace to be used.
        """
        namespace, key = self._namespace(namespace), self._key(key)
        if not self._has_key(namespace, key):
            raise KeyError(f"{key} (namespace: {namespace})")
        self._remove_key(namespace, key)

    @require_connection
    def unset_namespace(self, namespace: str | None = None) -> None:
        """
        Remove an entire namespace from the cache.

        Args:
            namespace: The namespace to be removed.
        """
        namespace = self._namespace(namespace)
        if not self._has_namespace(namespace):
            raise KeyError(f"namespace: {namespace}")
        self._remove_namespace(namespace)

    # Top-level descriptions

    @property
    @require_connection
    def namespaces(self) -> list[str]:
        "A list of the namespaces stored in the cache."
        return self._get_namespaces()

    @require_connection
    def has_namespace(self, namespace: str | None = None) -> bool:
        """
        Check whether the cache has the nominated namespace.

        Args:
            namespace: The namespace for which to check for existence.

        Returns:
            Whether the cache has the nominated namespaces.
        """
        namespace = self._namespace(namespace)
        return self._has_namespace(namespace)

    @require_connection
    def keys(self, namespace: str | None = None) -> list[str]:
        """
        Collect a list of all the keys present in the nominated namespaces.

        Args:
            namespace: The namespace from which to extract all of the keys.

        Returns:
            The keys stored in the cache for the nominated namespace.
        """
        namespace = self._namespace(namespace)
        return self._get_keys(namespace)

    @require_connection
    def has_key(self, key: str, namespace: str | None = None) -> bool:
        """
        Check whether the cache as a nominated key.

        Args:
            key: The key for which to check existence.
            namespace: The namespace from which to extract all of the keys.

        Returns:
            Whether the cache has a value for the nominated namespace and key.
        """
        namespace, key = self._namespace(namespace), self._key(key)
        return self._has_key(namespace, key)

    def get_total_bytecount(self, namespaces: list[str] | None = None) -> int:
        """
        Retrieve the total number of bytes used by the cache.

        This method iterates over all (nominated) namespaces and the keys
        therein, summing the result of `.get_bytecount(...)` on each.

        Args:
            namespaces: The namespaces to which the bytecount should be
                restricted.

        Returns:
            The total number of bytes used by the nominated namespaces.
        """
        total_bytes = 0

        if namespaces is None:
            namespaces = self.namespaces

        for namespace in namespaces:
            for key in self.keys(namespace=namespace):
                total_bytes += self.get_bytecount(key, namespace=namespace)

        return total_bytes

    def describe(self, namespaces: list[str] | None = None) -> pandas.DataFrame:
        """
        Return a pandas DataFrame showing all keys and their metadata.

        Args:
            namespaces: The namespaces to which the summary should be
                restricted.

        Returns:
            A representation of keys in the cache. Will include at least the
            following columns: ['bytes', 'namespace', 'key', 'created',
            'last_accessed']. Any additional metadata for keys will be appended
            to these columns.
        """
        out = []

        if namespaces is None:
            namespaces = self.namespaces

        for namespace in namespaces:
            for key in self.keys(namespace=namespace):
                usage = {
                    "bytes": self.get_bytecount(key, namespace=namespace),
                    "namespace": namespace,
                    "key": key,
                    "created": None,
                    "last_accessed": None,
                }
                usage.update(self.get_metadata(key, namespace=namespace))
                out.append(usage)

        required_columns = ["bytes", "namespace", "key", "created", "last_accessed"]
        if out:
            df = pandas.DataFrame(out)
            order = required_columns + sorted(
                set(df.columns).difference(required_columns)
            )
            return df.sort_values("last_accessed", ascending=False).reset_index(  # type: ignore[no-any-return]
                drop=True
            )[order]

        return pandas.DataFrame(data=[], columns=required_columns)

    # Cache pruning

    def prune(
        self,
        namespaces: list[str] | None = None,
        max_age: int
        | datetime.timedelta
        | dateutil.relativedelta.relativedelta
        | datetime.date
        | datetime.datetime
        | None = None,
        max_bytes: int | None = None,
        total_bytes: int | None = None,
        total_count: int | None = None,
    ) -> None:
        """
        Remove keys from the cache in order to satisfy nominated constraints.

        Args:
            namespaces: The namespaces to consider for pruning.
            max_age: The number of days, a timedelta, or a relativedelta,
                indicating the maximum age of items in the cache (based on last
                accessed date). Deltas are expected to be positive.
            max_bytes: The maximum number of bytes for *each* key, allowing the
                pruning of larger keys.
            total_bytes: The total number of bytes for the entire cache. Keys
                will be removed from least recently accessed to most recently
                accessed until the constraint is satisfied. This constraint will
                be applied after max_age and max_bytes.
            total_count: The maximum number of items to keep in the cache. Keys
                will be removed from least recently accessed to most recently
                accessed until the constraint is satisfied. This constraint will
                be applied after max_age and max_bytes.
        """
        usage = self.describe(namespaces=namespaces)
        if (
            usage.shape[0] == 0
        ):  # Abort early if the cache is empty (and hence has no index, which would cause problems later on)
            return

        constraints = []

        # Unset keys according to per-key constraints
        if max_age is not None:
            if isinstance(max_age, int):
                max_age = datetime.timedelta(max_age)
            if isinstance(
                max_age, (datetime.timedelta, dateutil.relativedelta.relativedelta)
            ):
                max_age = datetime.datetime.now() - max_age
            if not isinstance(max_age, (datetime.datetime, datetime.date)):
                raise ValueError(
                    f"Invalid type specified for `max_age`: {repr(max_age)}"
                )
            constraints.append(usage.last_accessed < max_age)

        if max_bytes is not None:
            if not isinstance(max_bytes, int):
                raise ValueError(
                    f"Invalid type specified for `max_bytes`: {repr(max_bytes)}"
                )
            constraints.append(usage.bytes > max_bytes)

        if constraints:
            to_unset = usage[functools.reduce(lambda x, y: x | y, constraints)]
            for _, row in to_unset.iterrows():
                logger.info(
                    f"Unsetting key '{row.key}' (namespace: '{row.namespace}')..."
                )
                self.unset(row.key, namespace=row.namespace)

        # Unset keys according to global constraints
        if total_bytes is not None or total_count is not None:
            if total_bytes is not None and not isinstance(total_bytes, int):
                raise ValueError(
                    f"Invalid type specified for `total_bytes`: {repr(total_bytes)}"
                )
            if total_count is not None and not isinstance(total_count, int):
                raise ValueError(
                    f"Invalid type specified for `total_count`: {repr(total_bytes)}"
                )
            usage = self.describe(namespaces=namespaces).assign(
                cum_bytes=lambda x: x.bytes.cumsum()
            )

            unset_index = total_count if total_count is not None else len(usage)
            if total_bytes is not None:
                unset_index = min(
                    unset_index,
                    int(usage.cum_bytes.searchsorted(total_bytes, side="right")),
                )
            for _, row in usage.loc[unset_index:].iterrows():
                logger.info(
                    f"Unsetting key '{row.key}' (namespace: '{row.namespace}')..."
                )
                self.unset(row.key, namespace=row.namespace)

    # Methods for subclasses to implement

    def _namespace(self, namespace: str | None) -> str | None:
        return namespace

    def _key(self, key: str) -> str:
        return key

    @abstractmethod
    def _get_namespaces(self) -> list[str]:
        raise NotImplementedError

    def _has_namespace(self, namespace: str | None) -> bool:
        return namespace in self._get_namespaces()

    @abstractmethod
    def _remove_namespace(self, namespace: str | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def _get_keys(self, namespace: str | None) -> list[str]:
        raise NotImplementedError

    def _has_key(self, namespace: str | None, key: str) -> bool:
        return key in self._get_keys(namespace=namespace)

    @abstractmethod
    def _remove_key(self, namespace: str | None, key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def _get_bytecount_for_key(self, namespace: str | None, key: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def _get_stream_for_key(
        self,
        namespace: str | None,
        key: str,
        stream_name: str,
        mode: str,
        create: bool,
    ) -> IO[Any]:
        pass
