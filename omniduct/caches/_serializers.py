import pickle


class Serializer(object):

    def file_extension(self):
        return ".{}".format(self.__class__.__name__.lower())

    def serialize(self, obj, fh):
        raise NotImplementedError

    def deserialize(self, fh):
        raise NotImplementedError


class PickleSerializer(Serializer):

    def file_extension(self):
        return ".pickle"

    def serialize(self, obj, fh):
        return pickle.dump(obj, fh)

    def deserialize(self, fh):
        return pickle.load(fh)
