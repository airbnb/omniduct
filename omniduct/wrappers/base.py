from abc import abstractmethod
from omniduct.duct import Duct
from omniduct.utils.docs import quirk_docs


class WrapperClient(Duct):

    DUCT_TYPE = Duct.Type.OTHER

    @quirk_docs('_init', mro=True)
    def __init__(self, **kwargs):
        Duct.__init_with_kwargs__(self, kwargs, port=self.DEFAULT_PORT)
        self._init(**kwargs)

    @abstractmethod
    def _init(self):
        pass

    @property
    def wrapped_field(self):
        raise NotImplementedError

    def __getattr__(self, key):
        return getattr(object.__getattribute__(self, self.wrapped_field), key)
