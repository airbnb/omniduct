import pickle

import pandas


class Serializer:
    @property
    def file_extension(self):
        return ""

    def serialize(self, obj, fh):
        raise NotImplementedError

    def deserialize(self, fh):
        raise NotImplementedError


class BytesSerializer(Serializer):
    @property
    def file_extension(self):
        return ".bytes"

    def serialize(self, obj, fh):
        assert isinstance(
            obj, bytes
        ), "BytesSerializer requires incoming data be already encoded into a bytestring."
        fh.write(obj)

    def deserialize(self, fh):
        return fh.read()


class PickleSerializer(Serializer):
    @property
    def file_extension(self):
        return ".pickle"

    def serialize(self, obj, fh):
        return pickle.dump(obj, fh)

    def deserialize(self, fh):
        return pickle.load(fh)


class PandasSerializer(Serializer):
    @property
    def file_extension(self):
        return ".pandas"

    def serialize(self, obj, fh):
        return pandas.to_pickle(obj, fh, compression=None)

    def deserialize(self, fh):
        return pandas.read_pickle(fh, compression=None)
