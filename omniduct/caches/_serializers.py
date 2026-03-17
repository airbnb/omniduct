from __future__ import annotations

import pickle
from typing import IO, Any, cast

import pandas


class Serializer:
    @property
    def file_extension(self) -> str:
        return ""

    def serialize(self, obj: Any, fh: IO[bytes]) -> None:
        raise NotImplementedError

    def deserialize(self, fh: IO[bytes]) -> Any:
        raise NotImplementedError


class BytesSerializer(Serializer):
    @property
    def file_extension(self) -> str:
        return ".bytes"

    def serialize(self, obj: bytes, fh: IO[bytes]) -> None:
        if not isinstance(obj, bytes):
            raise TypeError(
                "BytesSerializer requires incoming data be already encoded into a bytestring."
            )
        fh.write(obj)

    def deserialize(self, fh: IO[bytes]) -> bytes:
        return fh.read()


class PickleSerializer(Serializer):
    @property
    def file_extension(self) -> str:
        return ".pickle"

    def serialize(self, obj: Any, fh: IO[bytes]) -> None:
        pickle.dump(obj, fh)

    def deserialize(self, fh: IO[bytes]) -> Any:
        return pickle.load(fh)  # noqa: S301


class PandasSerializer(Serializer):
    @property
    def file_extension(self) -> str:
        return ".pandas"

    def serialize(self, obj: pandas.DataFrame, fh: IO[bytes]) -> None:
        obj.to_pickle(fh, compression=None)

    def deserialize(self, fh: IO[bytes]) -> pandas.DataFrame:
        return cast(pandas.DataFrame, pandas.read_pickle(fh, compression=None))  # noqa: S301
