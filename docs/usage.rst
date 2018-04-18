=====
Usage
=====

Configuration
-------------
Detection of the online/offline status of files, DataFiles and DataFileObjects
depends on whether the file is in a backend system that supports automated
archival to tape i.e., a backend storage with a HSM system. Furthermore, the
tools provided by `mytardis_hsm` use the Python `os` tools to detect the status
of files. Hence, `mytardis_hsm` only supports filesystem based storage
backends, not things like object storage. The way `mytardis_hsm` enforces this
is via the `django_storage_class` attribute of the StorageBox. By default,
`mytardis_hsm` supports the following `django_storage_class`'s::

  - django.core.files.storage.FileSystemStorage
  - tardis.tardis_portal.storage.MyTardisLocalFileSystem

This is defined by `DEFAULT_HSM_CLASSES` in the `mytardis_hsm.mytardis_hsm`. It
is, however, configurable by setting `HSM_STORAGE_CLASSES` in `settings.py`.
`HSM_STORAGE_CLASSES` should be a list of strings for the Django storage
classes you wish to support. For example, to add another storage class::

    HSM_STORAGE_CLASSES = settings.HSM_STORAGE_CLASSES + ['another.storage.Class']

Or to override storage classes::

    HSM_STORAGE_CLASSES = ['another.storage.Class']

Note that these storage classes must be file based and have a StorageBoxOption
with a ``location`` key that has the location in the filesystem as its value.

To use mytardis_hsm in a project::

    import mytardis_hsm
