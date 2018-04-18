# -*- coding: utf-8 -*-
"""HSM module. Utilities for detecting whethers files have
been HSM'd"""
from __future__ import unicode_literals

import os
from abc import ABCMeta, abstractmethod
from multiprocessing import cpu_count  # , Pool
from multiprocessing.pool import AsyncResult
from multiprocessing.dummy import Pool
from .utils import Try, Success


def _stat_os(path):
    """Use os.stat to calculate size and block number

    Parameters
    ----------
    path : str
        Path to file to calculate size and block number for

    Returns
    -------
    tuple
        tuple of size and block number i.e., (size, blocks)
    """
    stats = os.stat(path)
    return stats.st_size, stats.st_blocks


def _stat_subprocess(path):
    """Use subprocess to call underlying stat command. Uses
    regexp to isolate size and block number.

    Parameters
    ----------
    path : str
        Path to file to calculate size and block number for

    Returns
    -------
    tuple
        tuple of size and block number i.e., (size, blocks)

    Raises
    ------
    IOError
        If unable to detect size or blocks successfully using subprocess
    """
    import re
    import subprocess

    try:
        proc = subprocess.Popen(
            ['stat', path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, _ = proc.communicate()

        for line in stdout.splitlines():
            match = re.search(r"^.*Size: (\d+).*Blocks: (\d+).*$", line)
            if match:
                size = int(match.groups()[0])
                blocks = int(match.groups()[1])
                return size, blocks
    except Exception as exc:
        raise IOError("Unable to detect size or blocks for %s:\n%s"
                      % (path, exc))


def online(path, min_file_size):
    """Detects whether a file is online or offline (on tape).

    Basically this function checks the size and block number.
    If size > 0 and block number == 0, this is typically a sign that
    files are on tape.

    We attempt to os.stat to determine file size and block number;
    however, this doesn't work on all unix-like systems, so if it fails
    we attempt to determine them using a subprocess proc call to stat
    which we aim to match using a regexp.

    Notes
    -----
    We set a minimum size since very small files can be stored
    in the inode and hence have a 0 blksize.

    Parameters
    ----------
    path : str
        Path to the file for which we want to determine size and and block
        number.
    min_file_size : int, optional
        minimum size of files that could be stored in
        in the inode.

    Returns
    -------
    bool
        specifies whether the file in online i.e., not on tape.
    """
    try:
        size, blocks = _stat_os(path)
    except AttributeError:
        size, blocks = _stat_subprocess(path)

    if size > min_file_size and blocks == 0:
        return False

    return True


def _try_online(dfo, min_file_size):
    return Try.attempt(dfo.storage_box.options.get(key="location").value)\
              .map(lambda l: os.path.join(l, dfo.uri))\
              .map(lambda path: online(path, min_file_size))


def _read_first_byte(path):
    """Reads first byte from a file underlying a DataFileObject

    Parameters
    ----------
    path: str
        Path to file from which to read first byte

    Returns
    _______
    success: bool
        True if read of first byte completes successfully
    """

    with open(path, "rb") as rdr:
        rdr.read(1)

        return True


def _try_read_first_byte(dfo):
    """Attempt to read first byte of a file underlying a DataFileObject.

    Result of read attempt wrapped in a Try.

    Parameters
    ----------
    dfo: DataFileObject
        DFO path to read first byte from

    Returns
    _______
    path: Try
        Either Success(DFO) if read succeeded or Failure(err) if
        read failed with an exception.

    See Also
    --------
    mytardis_hsm.utils.Try : Coproduct type that represent either
        a successful computation or an exception.
    """
    def _check_result(result):
        if result:
            return dfo
        else:
            raise IOError("Could not read %s" % dfo)

    return Try.attempt(dfo.storage_box.options.get(key="location").value)\
              .map(lambda l: os.path.join(l, dfo.uri))\
              .map(_read_first_byte)\
              .map(_check_result)
    # Try.attempt(_read_first_byte, dfo)


class HSMAsyncResult(object):
    """Alias for multiprocessing.pool.AsyncResult"""

    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, timeout=None):
        """Return result when it arrives. Will block until result is available
        or raise ``multiprocessing.TimeOutError`` if `timeout` is specified and
        result is not available before timeout

        Parameters
        ----------
        timeout: Int, optional
            Number of seconds to wait before abandoning computation.

        Returns
        -------
        result: Try
            Output of computation wrapped in `Try` instance, where `Success`
            wraps the result of a successful computation, while `Failure`
            wraps the exception is the computation failed.

        Raises
        ------
        multiprocessing.TimeOutError
            If compuation is not complete before the specified time.

        See Also
        --------
        mytardis_hsm.utils.Try : Quasi-coproduct type that represent either
            a successful computation or an exception.
        """


class AbstractHSMChecker(object):
    """Interface for HSM Checkers"""

    __metaclass__ = ABCMeta

    @abstractmethod
    def online(self, dfo):
        """Check whether a DFO is online or has been HSM'd to disk.

        Parameters
        ----------
        dfo: DataFileObject
            DFO to check online status of
        callback: function
            Single argument function that will be called on with the result of
            the check. Function should accept a Try, where the result will be
            wrapped in Success() if the check successfully completes or Failure
            if it raised an exception.
        """
        pass


class AbstractHSMRetriever(object):
    """Abstract class for HSM File Retrievers"""

    __metaclass__ = ABCMeta

    @abstractmethod
    def retrieve(self, dfo, callback=None):
        """Retrieve a file underlying a DataFileObject from tape.

        Parameters
        ----------
        dfo: DataFileObject
            Path of file to retrieve from tape
        callback: function
            Callback that will be called for successful retrievals in
            asynchronous implementations. Should be a single argument function
            that accepts the DataFileObject that is retrieved from tape.
        """
        pass

    @abstractmethod
    def retrieve_batch(self, dfos, callback=None):
        """Retrieve files underlying a batch of DataFileObjects from tape.

        Parameters
        ----------
        dfos: [DataFileObject]
            List of DataFileObjects to retrieve from tape
        callback: function
            Callback that will be called for successful retrievals in
            asynchronous implementations. Should be a single argument function
            that accepts the list of DataFileObjects that are retrieved from
            tape.
        """
        pass


class HSMDummy(AbstractHSMChecker, AbstractHSMRetriever):

    def online(self, dfo, callback=None):
        return callback(Success(True))

    def retrieve(self, dfo, callback=None):
        return callback(Success(dfo))

    def retrieve_batch(self, dfos, callback=None):
        return callback(Success(dfos))


class HSMManager(AbstractHSMRetriever, AbstractHSMChecker):
    """Basic implementation of :py:class:`mytardis_hsm.hsm.AbstractFileRetriever`.
    This just attempts to read the first byte from each file.

    Attributes
    ----------
    pool: multiprocessing.Pool, optional
        Multiprocessing pool to use. Could be process or thread pool.
        Default is a multiprocessing.Pool with number of processes equal
        to the number of CPUs.
    min_file_size: int, optional
        Minimum size of file to perform check for. Should be equal to the
        maximum size of files that could be stored in the inode. Default is
        350 bytes.

    See Also
    --------
    mytardis_hsm.hsm.AbstractHSMRetriever : HSMRetriever interface that this
        class implements

    multiprocessing.Pool : Python process pool

    multiprocessing.dummy.Pool : Thread pool
    """
    _instance = None

    def __new__(cls, pool=Pool(processes=cpu_count()), min_file_size=350):
        if HSMManager._instance is None:
            HSMManager._instance = object.__new__(cls)
            HSMManager._instance.pool = pool
            HSMManager._instance.min_file_size = min_file_size

        return HSMManager._instance

    def online(self, dfo, callback):
        """Asynchronously check status of file

        Result will be wrapped in a `Try`.

        Parameters
        ----------
        dfo: DataFileObject
            DFO to check status of
        callback: function
            Single argument function that will passed the result of this async
            computation. Callback should expect the result to be a `Try`, where
            a successful computation will be represented by a `Success`
            intance, while a failed computation will hold by wrapped in a
            `Failure` instance. Callback should avoid avoid long-running
            computations as this will block the `_result_handler` thread until
            the computation completes.

        Notes
        -----
        In all cases, the computation is run in a `Try` context and the result
        is wrapped in either a `Success` if the computation succeeded or a
        `Failure` if the computation failed with an exception.

        See Also
        --------
        mytardis_hsm.utils.Try : Quasi-coproduct type that represent either
            a successful computation or an exception.
        """
        self.pool.apply_async(
            _try_online,
            (dfo, self.min_file_size),
            callback=callback
        )

    def retrieve(self, dfo, callback):
        """Asynchronously recall a file from tape

        Result will be wrapped in a `Try`.

        Parameters
        ----------
        dfo: DataFileObject
            DataFileObject of file to retrieve
        callback: function
            Single argument function that will passed the result of this async
            computation. Callback should expect the result to be a `Try`, where
            a successful computation will be represented by a `Success`
            intance, while a failed computation will hold by wrapped in a
            `Failure` instance. Callback should avoid avoid long-running
            computations as this will block the `_result_handler` thread until
            the computation completes.

        Notes
        -----
        In all cases, the computation is run in a `Try` context and the result
        is wrapped in either a `Success` if the computation succeeded or a
        `Failure` if the computation failed with an exception.

        See Also
        --------
        mytardis_hsm.utils.Try : Quasi-coproduct type that represent either
            a successful computation or an exception.
        """
        self.pool.apply_async(
            _try_read_first_byte,
            (dfo,),
            callback=callback
        )

    def retrieve_batch(self, dfos, callback=None):
        """Asynchronously recall a batch of DataFileObjects from tape

        Results will be wrapped in a `Try`.

        Parameters
        ----------
        dfos: [DataFileObject]
            List of DFOs to retrieve
        callback: function
            Single argument function that will passed the result of this async
            computation. Callback should expect the result to be a `Try`, where
            a successful computation will be represented by a `Success`
            intance, while a failed computation will hold by wrapped in a
            `Failure` instance. Callback should avoid avoid long-running
            computations as this will block the `_result_handler` thread until
            the computation completes.

        Notes
        -----
        In all cases, the computation is run in a `Try` context and the result
        is wrapped in either a `Success` if the computation succeeded or a
        `Failure` if the computation failed with an exception.

        See Also
        --------
        mytardis_hsm.utils.Try : Quasi-coproduct type that represent either
            a successful computation or an exception.
        """
        self.pool.map_async(
            _try_read_first_byte,
            (dfos,),
            callback=callback
        )
