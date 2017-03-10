from __future__ import print_function

import inspect
import logging
import sys
import time
import types

import progressbar
from decorator import decorate
from future.utils import raise_with_traceback

from .config import config

config.register('logging_level',
                description='Set the default logging level.',
                default=logging.INFO,
                onchange=lambda level: logger.setLevel(level, context='omniduct'))


class StatusLogger(object):
    '''
    StatusLogger is a wrapper around `logging.Logger` that allows for consistent
    treatment of logging messages. While not strictly required,
    it simplifies and abstracts the usage of the logging module within omniduct.
    It also adds support for displaying progress bars.

    Instances of StatusLogger are proxies for `logging.Logger` objects, and thus
    inherit the methods and properties of Logger. For example, to log a warning, use:

    >>> StatusLogger().warn('test')

    StatusLogger will automatically detect the context of the logged message, and
    include it in the log messages.
    '''

    def __init__(self):
        self.__scopes = []

        ch = LoggingHandler()
        formatter = logging.Formatter("%(levelname)s: %(name)s (%(funcName)s:%(lineno)s): %(message)s")
        ch.setFormatter(formatter)

        self.setLevel(config.logging_level, context='omniduct')
        omniductLogger = self.__get_logger_instance(context='omniduct')
        omniductLogger.addHandler(ch)
        omniductLogger.propagate = False

        self._progress_bar = None

    def _scope_enter(self, name, timed=False, extra=None):
        if config.logging_level < logging.INFO:
            print("\t" * len(self.__scopes) + "Entering scope: {}".format(name), file=sys.stderr)
        props = {'name': name}
        if timed:
            props['time'] = time.time()
        if extra is not None:
            props['extra'] = extra
        props['caveats'] = []
        self.__scopes.append(props)

    def _scope_exit(self, success=True):
        if self._progress_bar is not None:
            self.progress(100, complete=True)
        props = self.__scopes[-1]
        if 'time' in props:
            logger.info(
                "{} {} on {}.".format('Completed in' if success else 'Failed after', self.__get_time(time.time() - props['time']), time.strftime('%Y-%m-%d')) +
                (' CAVEATS: {}.'.format('; '.join(props['caveats'])) if props['caveats'] else '')
            )
        scope = self.__scopes.pop()
        if config.logging_level < logging.INFO:
            print("\t" * len(self.__scopes) + "Exited scope: {}".format(scope['name']), file=sys.stderr)
        elif 'has_logged' in scope:
            if len(self.current_scopes) == 0:
                print("\n", file=sys.stderr)
            else:
                self.current_scope_props['has_logged'] = self.current_scope_props.get('has_logged') or props['has_logged']

    def __get_time(self, seconds):
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)

        if h > 0:
            return "{:.0f} hours and {:.0f} minutes".format(h, m)
        if m > 0:
            return "{:.0f} minutes and {:.0f} seconds".format(m, s)
        return "{:.2f} seconds".format(s)

    def caveat(self, caveat):
        self.current_scope_props['caveats'].append(caveat)

    @property
    def current_scopes(self):
        '''
        The current logger scopes. This is not designed to work with multiple
        threads.
        '''
        return [scope['name'] for scope in self.__scopes]

    @property
    def current_scope_props(self):
        ''''
        The properties for the most nested scope.
        '''
        return self.__scopes[-1]

    def __get_progress_bar(self):
        if self._progress_bar is None:
            if config.logging_level >= logging.INFO:
                prefix = ": ".join(self.current_scopes) + ": "
            else:
                prefix = "\t" * len(self.current_scopes)
            self._progress_bar = progressbar.ProgressBar(widgets=[prefix, progressbar.widgets.Bar()],
                                                         redirect_stderr=True,
                                                         redirect_stdout=True).start()

        return self._progress_bar

    def progress(self, progress, complete=False):
        '''
        Set the current progress to `progress`, and if not already showing, display
        a progress bar. If `complete` evaluates to True, then finish displaying the progress.
        '''
        if config.logging_level <= logging.INFO:
            self.__get_progress_bar().update(progress)
            if complete:
                self.__get_progress_bar().finish()
                self._progress_bar = None

    # Logging emulation

    def __get_logger_instance(self, context=None):
        '''
        Get a `logger.Logger` instance for the provided context; inferring the
        context from the runtime stack if the provided context is `None`.
        '''
        if context is None:
            try:
                caller = inspect.stack()[2]
                context = inspect.getmodule(caller.frame).__name__
            except:
                context = 'omniduct'
        return logging.getLogger(context)

    def __getattr__(self, name):
        '''
        Return the attributes of the wrapped `logging.Logger` instance rather
        than this one (unless the property actually exists in StatusLogger).
        '''
        return getattr(self.__get_logger_instance(), name)

    def setLevel(self, level, context=None):
        '''
        Add a keyword argument `context` to the standard `setLevel` method, in
        order to allow for fine-grained logging levels, while retaining the
        simplicity of a single "logger" instance.
        '''
        self.__get_logger_instance(context).setLevel(level)


class LoggingHandler(logging.Handler):
    '''
    An implementation of Logging.Handler to render the logging methods shown in Omniduct and derivatives.
    '''

    def __init__(self, level=logging.NOTSET):
        logging.Handler.__init__(self, level=level)
        self.setFormatter(logging.Formatter("%(levelname)s: %(name)s (%(funcName)s:%(lineno)s): %(message)s"))

    def format_simple(self, record):
        return "{}: {}".format(record.levelname, record.getMessage())

    def handle(self, record):
        if config.logging_level < logging.INFO:  # Print everything verbosely
            prefix = '\t' * len(logger.current_scopes)
            self._overwrite(prefix + self.format(record),
                            overwritable=False,
                            truncate=False)
        else:
            prefix = ""
            if len(logger.current_scopes) > 0:
                prefix = ": ".join(logger.current_scopes) + ": "
                logger.current_scope_props['has_logged'] = True
            important = (record.levelno >= logging.WARNING or
                         logger._progress_bar is not None or
                         len(logger.current_scopes) == 0)
            self._overwrite(prefix + self.format_simple(record),
                            overwritable=not important,
                            truncate=not important
                            )

        sys.stderr.flush()

    def _overwrite(self, text, overwritable=True, truncate=True, file=sys.stderr):
        w, h = progressbar.utils.get_terminal_size()
        file.write('\r' + ' ' * w + '\r')  # Clear current line
        if overwritable:
            text.replace('\n', ' ')
        if truncate:
            if len(text) > w:
                text = text[:w - 3] + '...'
        if not overwritable:
            text += '\n'
        file.write(text)


def logging_scope(name, *wargs, **wkwargs):
    '''
    A decorator to add the decorated function as a new logging scope, with name `name`.
    All additional arguments are passed to `StatusLogger._scope_enter`. Current
    supported keyword arguments are "timed", in which case when the scope closes,
    the duration of the call is shown.
    '''
    def track(func, *args, **kwargs):
        logger._scope_enter(name, *wargs, **wkwargs)
        success = True
        try:
            f = func(*args, **kwargs)
            return f
        except Exception as e:
            success = False
            raise_with_traceback(e)
        finally:
            logger._scope_exit(success)
    return (lambda func: decorate(func, track))


# HACK: remove newline after progress bars by patching progressbar2 library
def new_finish(self, *args, **kwargs):  # pragma: no cover
    progressbar.bar.ProgressBarMixinBase.finish(self, *args, **kwargs)


progressbar.bar.DefaultFdMixin.finish = new_finish

logger = StatusLogger()
