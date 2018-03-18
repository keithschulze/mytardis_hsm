# -*- coding: utf-8 -*-

"""mytardis_hsm package for checking HSM status of MyTardis
models"""

import logging
import os
import hsm
from django.conf import settings
from tardis.tardis_portal.models import StorageBoxOption


LOGGER = logging.getLogger(__name__)


"""Default supported values for `django_storage_class` in StorageBox

In order to prevent checking online status of unsupported (i.e., non-
filesystem) storage types, we check the `django_storage_class` of a
StorageBox. The default supported value are defined by this constant.
If you need to add or override these defaults, you can supply a different
list of support storage class as the HSM_STORAGE_CLASSES field in settings.py
"""
DEFAULT_HSM_CLASSES = [
    "tardis.tardis_portal.storage.MyTardisLocalFileSystemStorage",
    "django.core.files.storage.FileSystemStorage"
]

HSM_SCHEMA_NAMESPACE = "http://tardis.edu.au/hsm/1"


class DataFileNotVerified(Exception):
    """Exception raied when an operation is attempted on an
    unverified DataFile"""


class DataFileObjectNotVerified(Exception):
    """Exception raied when an operation is attempted on an
    unverified DataFile"""


class StorageClassNotSupportedError(Exception):
    """Exception raised when a storage class is not supported"""
    pass


def dfo_online(dfo, min_file_size=350):
    """Checks whether the underlying file of a DataFileObject is online

    Parameters
    ----------
    dfo : DataFileObject
        The DataFileObject for which to check the status
    min_file_size : int, optional
        minimum size of files that could be stored in
        in the inode.

    Returns
    -------
    bool
        Status for whether dfo is online.

    Raises
    ------
    DataFileObjectNotVerified
        If dfo is unverified
    StorageClassNotSupportedError
        If the `django_storage_class` for the StorageBox of the input
        DataFileObject is not supported
    """
    if dfo.verified:
        storage_classes = getattr(settings,
                                  "HSM_STORAGE_CLASSES",
                                  DEFAULT_HSM_CLASSES)
        if dfo.storage_box.django_storage_class in storage_classes:
            try:
                location = dfo.storage_box.options.get(key="location").value
                filepath = os.path.join(location, dfo.uri)
                return hsm.online(filepath, min_file_size)
            except StorageBoxOption.DoesNotExist:
                LOGGER.debug("DataFileObject with id %s doesn't have a file"
                             "system path/location", dfo.id)
        else:
            msg = (
                "You have tried to check the online/offline status of a\n"
                "DataFileObject with data in a StorageBox with an\n"
                "unsupported `django_storage_class`. The supported \n"
                "`django_storage_class` are declared by the\n"
                "`django.conf.settings.HSM_STORAGE_CLASSES` variable.\n"
                "By default this is set by the DEFAULT_HSM_CLASSES\n"
                "variable above. If the storage class you are using supports\n"
                "file path based access and the StorageBox has a\n"
                "StorageBoxOption with a `location` key, then you should\n"
                "declare it in your settings.py using the HSM_STORAGE_CLASSES"
                "\nkey.\n\n"
                "For example, to add another storage class:\n"
                "    HSM_STORAGE_CLASSES = settings.HSM_STORAGE_CLASSES \\\n"
                "                          + ['another.storage.Class']\n\n"
                "or to override storage classes:\n"
                "    HSM_STORAGE_CLASSES = ['another.storage.Class']"
            )
            raise StorageClassNotSupportedError(msg)
    else:
        raise DataFileObjectNotVerified(
            "Cannot check online status of unverified DataFileObject: %s"
            % dfo.id)


def df_online(datafile, min_file_size=350):
    """Checks whether the primary/preferred download DataFileObject backing
    a DataFile is online.

    Parameters
    ----------
    df: DataFile
        The DataFile record to check the online status of
    min_file_size : int, optional
        minimum size of files that could be stored in
        in the inode.

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
        return dfo_online(datafile.get_preferred_dfo(), min_file_size)
    else:
        raise DataFileNotVerified(
            "Cannot check online status of unverified DataFile: %s"
            % datafile.id)
