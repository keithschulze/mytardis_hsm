# -*- coding: utf-8 -*-

"""Utilities module for mytardis_hsm"""

from abc import ABCMeta, abstractmethod
from importlib import import_module


class Try(object):
    """Quasi-coproduct (Sum) type that represents a computation that could fail
    with an exception. Try encapsulates these 2 outcomes via it's subclasses:
    Success and Failure

    Examples
    --------
    One way to use Try is via its attempt method. Attempt should be called with
    a function that could possibly fail. If the function succeeds, the result
    will be wrapped in a Success instance. Conversely if the function raise an
    Exception, the exception will be wrapped in a Failure instance.

    >>> from mytardis_hsm.utils import Try
    >>> def greeting(name):
    ...     if name == "Joe":
    ...             raise Exception("I don't greet Joe's")
    ...     else:
    ...             return "Hello, " + name


    >>> result = Try.attempt(greeting, "Tracy")
    >>> result.get_or_raise()
    'Hello, Tracy'

    >>> hello_joe = Try.attempt(greeting, "Joe")
    >>> hello_joe.get_or_raise()
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "mytardis_hsm/utils.py", line 92, in attempt
        return Success(func(*args, **kwargs))
      File "<stdin>", line 3, in greeting
    Exception: I don't greet Joe's
    """
    __metaclass__ = ABCMeta

    @staticmethod
    def attempt(func, *args, **kwargs):
        """Effectively, run a func and it's arguments inside a try/except
        block and wraps the result or exception in a `Success` or `Failure`,
        respectively::

            try:
                return(Success(func(*args, **kwargs)))
            except Exception as e:
                return Failure(e)

        Parameters
        ----------
        func: function
            Arbitrary function
        args: tuple
            Positional arguments to apply to func
        kwargs: dict
            Keyword arguments to apply to func

        Return
        ------
        Success or Failure
            If func completes successfully, the result is wrapped in a
            `Success` instance. If the func fails with an Exception, the
            Exception is wrapped in a `Failure` instance.
        """
        try:
            return Success(func(*args, **kwargs))
        except Exception as exc:
            return Failure(exc)

    @classmethod
    def raise_error(cls, err):
        return Failure(err)

    def match(self, handle_success, handle_failure):
        pass

    @abstractmethod
    def map(self, func):
        """Apply a function, `func`, to the result of a successful computation.

        .. note::

            `map` will only apply `func` to the result of a successful
            computation. It will not apply it to an exception. If you want to
            handle an exception, use `handle_error`.


        Parameters
        ----------
        func: function
            Single argument function that accepts a value currently stored in
            this Try.


        Returns
        -------
        `Success` or `Failure`
            Result of applying `func` to value in `Success`. Result is
            rewrapped in `Success`. A `Failure` is simply returned.
        """
        pass

    @abstractmethod
    def get_or_raise(self):
        """Get the result of a successful computation or reraise the exception
        if the computation failed"""
        pass

    @abstractmethod
    def handle_error(self, func):
        """Handle any error, potentially recovering from it, by mapping it to
        an Success(value) with function, `func`.

        Parameters
        ----------
        func: function
            Single argument function that accepts a particular Error/Exception
            and converts is to a value.


        Returns
        -------
        Success(value)
            Result of applying `func` to the exception from a failed
            computation and then wrapping it in Success
        """
        pass


class Success(Try):
    """Represents the successful case of a computation that could fail"""
    def __init__(self, a):
        self.success = a

    def map(self, func):
        return Try.attempt(func, (self.success,))

    def get_or_raise(self):
        return self.success

    def handle_error(self, func):
        return self


class Failure(Try):
    """Represents the failed case of a computation that could fail with
    an Exception. In this case, the instances value hold the exception
    was raised during failure"""
    def __init__(self, exc):
        self.failure = exc

    def __nonzero__(self):
        return False

    def map(self, func):
        return Failure(self.failure)

    def get_or_raise(self):
        raise self.failure

    def handle_error(self, func):
        return Try.attempt(func, (self.failure,))


def create_instance(cls, *args, **kwargs):
    """Safely create an instance of a class using the string
    for its module and class name.

    Parameters
    ----------
    cls: str
        Classes module and name i.e., 'os.PathLike'

    Returns
    -------
    instance
        Instance of cls parameters

    Raises
    ------
    ValueError
        Raised if module isn't specified
    ImportError
        Raised if module can't be imported
    AttributeError
        Raised Class can't be found in module
    """
    try:
        dot = cls.rindex('.')
        module, classname = cls[:dot], cls[dot + 1:]
        mod = import_module(module)
        class_ = getattr(mod, classname)
    except ValueError:
        raise ValueError('%s doesn\'t have a valid module' % cls)
    except ImportError as e:
        raise ImportError('Error importing %s: "%s"' %
                          (module, e))
    except AttributeError:
        raise AttributeError(
            'Module "%s" does not define a "%s" class' %
            (module, classname))

    instance = class_(*args, **kwargs)
    return instance
