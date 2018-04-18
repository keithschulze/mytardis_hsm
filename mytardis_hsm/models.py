# -*- coding: utf-8 -*-8

"""MyTardisHSM models"""

from django.conf import settings
from django.db import models
from tardis.tardis_portal.models import StorageBox
from .utils import create_instance


class MultipleHSMConfigError(Exception):
    """Raised when there are multiple HSMConfig for a single storage box"""
    pass


class HSMConfig(models.Model):
    """HSM Configuration Model

    HSM configurations are done per storage box. Checking file status and
    retrieving files are also
    """

    NONE = "mytardis_hsm.hsm.HSMDummy"
    FILESYSTEM = "mytardis_hsm.hsm.HSMManager"
    DEFAULT_HSM_INTERFACES = (
        (NONE, 'None'),
        (FILESYSTEM, 'File-system based')
    )

    storage_box = models.OneToOneField(StorageBox, related_name="hsm_config")
    status_checker = models.CharField(
        choices=getattr(settings, "HSM_INTERFACES", DEFAULT_HSM_INTERFACES),
        default=NONE,
        max_length=200
    )
    retriever = models.CharField(
        choices=getattr(settings, "HSM_INTERFACES", DEFAULT_HSM_INTERFACES),
        default=NONE,
        max_length=200
    )

    def create_status_checker(self, *args, **kwargs):
        """Create an instance of the selected status_checker class."""
        return create_instance(self.status_checker, *args, **kwargs)

    def create_retriever(self, *args, **kwargs):
        """Create an instance of the selected retriever class."""
        return create_instance(self.retriever, *args, **kwargs)
