============
mytardis_hsm
============


.. https://img.shields.io/pypi/v/mytardis_hsm.svg
        :target: https://pypi.python.org/pypi/mytardis_hsm

.. image:: https://img.shields.io/travis/keithschulze/mytardis_hsm.svg
        :target: https://travis-ci.org/keithschulze/mytardis_hsm

.. image:: https://readthedocs.org/projects/mytardis-hsm/badge/?version=latest
        :target: https://mytardis-hsm.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. https://pyup.io/repos/github/keithschulze/mytardis_hsm/shield.svg
     :target: https://pyup.io/repos/github/keithschulze/mytardis_hsm/
     :alt: Updates

===========
MyTardisHSM
===========

MyTardis App for monitoring online/offline status of DataFile/DataFileObjects in environments where backend storage is managed by an Hierachical Storage Management (HSM) system.

Why?
----

When the backend storage system is managed by an HSM, you have little
visibility into when a file might be archived to tape. This has implications
for serving files via the web in that users can encounter terrible performance
while trying to download files that are only on tape-based archive storage. It
also has big implications for general MyTardis operations like verification and
file movements. This App aims to provide tools to detect files that have been
archived.


* Free software: BSD license
* Documentation: https://mytardis-hsm.readthedocs.io.



Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

