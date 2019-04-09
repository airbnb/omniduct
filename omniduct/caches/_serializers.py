import pickle
from distutils.version import LooseVersion

import pandas


class Serializer(object):

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
        assert isinstance(obj, bytes), "BytesSerializer requires incoming data be already encoded into a bytestring."
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

    @classmethod
    def serialize(cls, formatted_data, fh):
        # compat: if pandas is old, to_pickle does not accept file handles
        if LooseVersion(pandas.__version__) <= LooseVersion('0.20.3'):
            fh.close()
            fh = fh.name
        return pandas.to_pickle(formatted_data, fh, compression=None)

    @classmethod
    def deserialize(cls, fh):
        return pandas.read_pickle(fh, compression=None)
