# -*- coding: utf-8 -*-

from mytardis_hsm.mytardis_hsm import (
    DEFAULT_HSM_CLASSES
)

# Minimum file size to check online/offline status for.
# Files smaller than this are usually retained in inode.

HSM_MIN_FILE_SIZE = 500

# List of Django storage classes for which to check online/
# offline status of files. Basically, this can be any storage_box
# with a location attribute that points to a file on a filesystem.

HSM_STORAGE_CLASSES = DEFAULT_HSM_CLASSES
