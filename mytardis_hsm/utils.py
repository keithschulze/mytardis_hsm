# -*- coding: utf-8 -*-

"""Utilities module for mytardis_hsm"""

from celery.five import monotonic
from django.core.cache import caches


class DatafileLock(object):
    """Helper class to manage the acquisition and release of locks for
    working with a datafile.

    Attributes
    ----------
    datafile: Datafile
        Datafile for which to acquire a lock over
    oid: str
        Unique id for the object acquiring lock
    cache_name: str, optional
        Name of cache holding the locks
    expires: int, optional
        Number of seconds for which the lock is valid. Default is 5 min.

    Examples
    --------
    The main way to use this class if using the Python *with* syntax:

    >>> with DatafileLock(datafile, "oid_from_somewhere") as lock:
    ...     if lock:
    ...         # do something interesting
    ...         print "I successfully got the lock"

    """

    def __init__(self, datafile, oid, cache_name="default",
                 expires=300):
        self.lockid = DatafileLock.generate_lockid(datafile.id)
        self.oid = oid
        self.cache = caches[cache_name]
        self.expires = expires
        self.expires_at = monotonic() + expires - 3

    def __enter__(self):
        return self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()

    @staticmethod
    def generate_lockid(datafile_id):
        """Return a lock id for a datafile"""
        return "mt-hsm-lock-%d" % datafile_id

    def acquire(self):
        """Acquire lock for a datafile to prevent filters from running
        mutliple times on the same datafile in quick succession.

        Returns
        -------
        locked: boolean
            Boolean representing whether datafile is locked
        """
        return self.cache.add(self.lockid, self.oid, self.expires)

    def release(self):
        """ Release lock on datafile."""
        if monotonic() < self.expires_at:
            self.cache.delete(self.lockid)
