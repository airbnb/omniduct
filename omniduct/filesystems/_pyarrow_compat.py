from pyarrow.filesystem import FileSystem
from pyarrow.util import implements, _stringify_path


class OmniductFileSystem(FileSystem):
    """
    Wraps Omniduct filesystem implementations for use with PyArrow.
    """

    def __init__(self, fs):
        self.fs = fs

    @implements(FileSystem.isdir)
    def isdir(self, path):
        return self.fs.isdir(_stringify_path(path))

    @implements(FileSystem.isfile)
    def isfile(self, path):
        return self.fs.isfile(_stringify_path(path))

    @implements(FileSystem._isfilestore)
    def _isfilestore(self):
        return True

    @implements(FileSystem.delete)
    def delete(self, path, recursive=False):
        return self.fs.remove(_stringify_path(path), recursive=recursive)

    @implements(FileSystem.exists)
    def exists(self, path):
        return self.fs.exists(_stringify_path(path))

    @implements(FileSystem.mkdir)
    def mkdir(self, path, create_parents=True):
        return self.fs.mkdir(_stringify_path(path), recursive=create_parents)

    @implements(FileSystem.open)
    def open(self, path, mode='rb'):
        return self.fs.open(_stringify_path(path), mode=mode)

    @implements(FileSystem.ls)
    def ls(self, path, detail=False):
        path = _stringify_path(path)
        if detail:
            return self.showdir(path)
        return self.listdir(path)

    def walk(self, path):
        return self.fs.walk(_stringify_path(path))
