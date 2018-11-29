import inspect
import os
import six
import textwrap
from abc import ABCMeta

import decorator

from omniduct.errors import DuctProtocolUnknown
from omniduct.utils.debug import logger


class ProtocolRegisteringABCMeta(ABCMeta):
    """
    This metaclass provides automatic registration of Duct subclasses so that
    they can be looked up by the protocols they support. Note that protocol
    mappings must be unique.
    """

    def __init__(cls, name, bases, dct):
        ABCMeta.__init__(cls, name, bases, dct)

        if not hasattr(cls, '_protocols'):
            cls._protocols = {}

        registry_keys = getattr(cls, 'PROTOCOLS', []) or []
        if registry_keys:
            for key in registry_keys:
                if key in cls._protocols and cls.__name__ != cls._protocols[key].__name__:
                    logger.info("Ignoring attempt by class `{}` to register key '{}', which is already registered for class `{}`.".format(cls.__name__, key, cls._protocols[key].__name__))
                else:
                    cls._protocols[key] = cls

    def _for_protocol(cls, key):
        if key not in cls._protocols:
            raise DuctProtocolUnknown("Missing `Duct` implementation for protocol: '{}'.".format(key))
        return cls._protocols[key]


class ProtocolRegisteringQuirkDocumentedABCMeta(ProtocolRegisteringABCMeta):
    """
    This metaclass adds the ability to automatically append quirk documentation
    to methods from a nominated method. For example, if the protocol specific
    implementation of `.connect()` is implemented in `._connect`, you can
    decorate the connect method with this decorator using
    `@quirk_docs('_connect')`, then the documentation from the `_connect`
    method will be appended to the `connect` docs under a heading "<cls> Quirks:".
    """

    def __init__(cls, name, bases, dct):
        super(ProtocolRegisteringQuirkDocumentedABCMeta, cls).__init__(name, bases, dct)

        # Allow method of avoiding appending of quirk docs in some environments (such as documentation)
        if os.environ.get('OMNIDUCT_DISABLE_QUIRKDOCS', None) is not None:
            return

        @decorator.decorator
        def wrapped(f, *args, **kw):
            return f(*args, **kw)

        mro = inspect.getmro(cls)
        mro = mro[:[klass.__name__ for klass in mro].index('Duct') + 1]

        # Handle module-level documentation
        module_docs = [cls.__doc__]
        for klass in mro:
            if klass != cls and hasattr(klass, '_{}__doc_attrs'.format(klass.__name__)):
                module_docs.append([
                    'Attributes inherited from {}:'.format(klass.__name__),
                    inspect.cleandoc(getattr(klass, '_{}__doc_attrs'.format(klass.__name__)))
                ])

        cls.__doc__ = cls.__doc_join(*module_docs)

        # Handle function/method-level documentation
        for name, member in inspect.getmembers(cls, predicate=inspect.isfunction):

            # Check if there is anything to do
            if (
                inspect.isabstract(member) or
                not (
                    getattr(member, '_quirks_method', None) or
                    getattr(member, '_quirks_mro', False)
                )
            ):
                continue

            local_member = name in cls.__dict__

            # Extract documentation from this member and the quirks member
            member_docs = getattr(member, '__doc_orig__', None) or getattr(member, '__doc__')
            mro_docs = quirk_docs = None
            mro_order = reversed(mro) if member._quirks_mro_reverse else mro
            if member._quirks_mro:
                mro_docs = cls.__doc_join(
                    *[
                        [
                            'Inherited via {}:'.format(klass.__name__),
                            getattr(getattr(klass, member.__name__), '__doc_orig__', None) or getattr(klass, member.__name__).__doc__
                        ]
                        for klass in mro_order if member.__name__ in klass.__dict__
                    ]
                )
            if member._quirks_method and member._quirks_method in cls.__dict__:
                quirk_member = getattr(cls, member._quirks_method, None)
                if quirk_member:
                    quirk_docs = getattr(quirk_member, '__doc_orig__', None) or getattr(quirk_member, '__doc__')

            if quirk_docs or mro_docs:
                # Overide method object with new object so we don't modify
                # underlying method that may be shared by multiple classes.
                setattr(cls, name, wrapped(member))
                member = getattr(cls, name)
                member.__doc__ = cls.__doc_join(
                    member_docs if (local_member or not mro_docs) else None,
                    mro_docs,
                    [
                        "{} Quirks:".format(cls.__name__),
                        quirk_docs
                    ]
                )

    @classmethod
    def __doc_join(cls, *docs, **kwargs):
        out = []
        for doc in docs:
            if doc in (None, ''):
                continue
            elif isinstance(doc, six.string_types):
                out.append(textwrap.dedent(doc).strip('\n'))
            elif isinstance(doc, (list, tuple)):
                if len(doc) < 2:
                    continue
                d = cls.__doc_join(*doc[1:])
                if d:
                    out.append(
                        '{header}\n{body}'.format(
                            header=doc[0].strip(),
                            body='    ' + d.replace('\n', '\n    ')  # textwrap.indent not available in python2
                        )
                    )
            else:
                raise ValueError("Unrecognised doc format: {}".format(type(doc)))
        return '\n\n'.join(out)
