# -*- coding: utf-8 -*-

"""mytardis_hsm package for checking HSM status of MyTardis
models"""

import logging
import os

from celery.five import monotonic
from django.core.cache import caches
from tardis.tardis_portal.models import (
    DataFile,
    Dataset,
    DatafileParameter,
    DatafileParameterSet,
    ParameterName,
    Schema,
    StorageBoxOption
)
from .hsm import HSMDummy
from .models import HSMConfig, MultipleHSMConfigError


LOGGER = logging.getLogger(__name__)


DEFAULT_HSM_CLASSES = [
    "tardis.tardis_portal.storage.MyTardisLocalFileSystemStorage",
    "django.core.files.storage.FileSystemStorage"
]
"""Default supported values for `django_storage_class` in StorageBox

In order to prevent checking online status of unsupported (i.e., non-
filesystem) storage types, we check the `django_storage_class` of a
StorageBox. The default supported value are defined by this constant.
If you need to add or override these defaults, you can supply a different
list of support storage class as the HSM_STORAGE_CLASSES field in settings.py
"""

HSM_DATAFILE_NAMESPACE = "http://tardis.edu.au/schemas/hsm/datafile/1"
"""Schema Namespace for HSM Datafile metadata
"""

HSM_DATASET_NAMESPACE = "http://tardis.edu.au/schemas/hsm/dataset/1"
"""Schema Namespace for HSM Dataset metadata
"""


class DataFileNotVerified(Exception):
    """Exception raied when an operation is attempted on an
    unverified DataFile"""
    pass


class DataFileObjectNotVerified(Exception):
    """Exception raied when an operation is attempted on an
    unverified DataFile"""
    pass


class StorageClassNotSupportedError(Exception):
    """Exception raised when a storage class is not supported"""
    pass


class OnlineParamExistsError(Exception):
    """Exception raised when there is an attempt to create a
    Parameter of ParameterName online for a datafile where this
    parameter already exists
    """
    pass


def _get_checker(storage_box):
    """Get a online HSM status checker for given storage box.

    Parameters
    ----------
    storage_box: StorageBox
        StorageBox to get HSM online checker for

    Returns
    -------
    checker: AbstractHSMChecker
        Instance able to check online status of files in `storage_box`
    """
    sbs = HSMConfig.objects.filter(storage_box=storage_box)

    if sbs.count() == 0:
        return HSMDummy()
    elif sbs.count() == 1:
        return sbs.first().create_status_checker()
    else:
        raise MultipleHSMConfigError(
            "StorageBox %s has mutliple HSMConfig configs." % storage_box)


def dfo_online(dfo, callback):
    """Checks whether the underlying file of a DataFileObject is online

    Parameters
    ----------
    dfo: DataFileObject
        DataFileObject to check online status of
    callback : Function
        Single argument function that will passed the result of this async
        computation. Callback should expect the result to be a `Try`, where
        a successful computation will be represented by a `Success`
        intance, while a failed computation will hold by wrapped in a
        `Failure` instance. Callback should avoid avoid long-running
        computations as this will block the `_result_handler` thread until
        the computation completes.

    Raises
    ------
    DataFileObjectNotVerified
        If dfo is unverified
    """
    if dfo.verified:
        try:
            checker = _get_checker(dfo.storage_box)
            checker.online(dfo, callback)
        except StorageBoxOption.DoesNotExist as exc:
            LOGGER.error(" storagebox id %s does not exist: %s",
                         dfo.storage_box.id, exc)
    else:
        raise DataFileObjectNotVerified(
            "Cannot check online status of unverified DataFileObject: %s"
            % dfo.id)


def df_online(datafile, callback):
    """Checks whether the primary/preferred download DataFileObject backing
    a DataFile is online.

    Parameters
    ----------
    datafile: DataFile
        Tuple with DataFile record and Schema to check the online status of
    callback : Function
        Single argument function that will passed the result of this async
        computation. Callback should expect the result to be a `Try`, where
        a successful computation will be represented by a `Success`
        intance, while a failed computation will hold by wrapped in a
        `Failure` instance. Callback should avoid avoid long-running
        computations as this will block the `_result_handler` thread until
        the computation completes.

    Returns
    -------
    bool
        Status for whether datafile is online.

    Raises
    ------
    StorageClassNotSupportedError
        If the `django_storage_class` for the StorageBox of the input
        DataFileObject is not supported
    """
    if datafile.verified:
        return dfo_online(datafile.get_preferred_dfo(), callback)
    else:
        raise DataFileNotVerified(
            "Cannot check online status of unverified DataFile: %s"
            % datafile.id)


def datafile_online(datafile):
    """Checks whether 'online' ParameterName for a datafile is True

    Parameters
    ----------
    datafile: DataFile
        DataFile to check online metadata for

    Returns
    -------
    bool
        Online/offline status of datafile

    Raises
    ------
    DataFileParameter.DoesNotExist
        If a Parameter for with ParameterName 'online' does not exist for
        this datafile

    Notes
    -----
    While this is superficially similar to df_online, they are
    strictly different. This method simply returns the value of the
    Parameter with ParamerName 'online' for this datafile. On the other
    hand, `df_online` actually checks whether the file on disk for the
    preferred DFO has > 0 blocks.

    """
    schema = Schema.objects.get(namespace=HSM_DATAFILE_NAMESPACE)
    param_name = ParameterName.objects.get(schema=schema,
                                           name="online")

    param_set = DatafileParameterSet.objects.get(schema=schema,
                                                 datafile=datafile)

    dfp = DatafileParameter.objects.get(parameterset=param_set,
                                        name=param_name)

    return dfp.string_value == "True"


def dataset_online(dataset):
    """Checks whether all files in a dataset are online.

    A dataset is online if all the DataFiles in the dataset are marked
    with a metadata Parameter (ParameterName=online) where online = True.

    Parameters
    ----------
    dataset: Dataset
        MyTardis Dataset record to check online status of.

    Returns
    -------
    bool
        Status for the dataset
    """
    dfs = DataFile.objects.filter(dataset=dataset)

    return all(datafile_online(df) for df in dfs)


def experiment_online(experiment):
    """Checks whether all files in an experiment are online.

    An experiment is online if all the DataFiles in every dataset are marked
    with a metadata Parameter (ParameterName=online) where online = True.

    Parameters
    ----------
    experiment: Experiment
        MyTardis Experiment record to check online status of.

    Returns
    -------
    bool
        Status for the experiment
    """
    dfs = Dataset.objects.filter(experiments=experiment)

    return all(dataset_online(df) for df in dfs)


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
