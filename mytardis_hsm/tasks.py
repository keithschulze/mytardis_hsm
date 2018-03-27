# -*- coding: utf-8 -*-

"""mytardis_hsm tasks module which house celery tasks for assessing
HSM status of Datafiles in MyTardis"""

import logging

from celery.task import task
from django.conf import settings
from tardis.tardis_portal.models import (DatafileParameter,
                                         DatafileParameterSet,
                                         ParameterName,
                                         Schema)
from .mytardis_hsm import (df_online, HSM_SCHEMA_NAMESPACE)
from .utils import DatafileLock


LOGGER = logging.getLogger(__name__)


@task(name="mytardis_hsm.create_df_status")
def create_df_status(datafile, schema_name, min_file_size):
    """Post-save celery task that checks online status of new file and create
    HSM metadata to track online status.

    Parameters
    ----------
    datafile: DataFile
        datafile to check and create online/offline status
        metadata for
    schema_name: Schema
        name of Schema which describes ParameterNames
    min_file_size : int
        minimum size of files to check HSM status of. This
        param is simply passed on to df_online.
    """
    if datafile.verified:
        with DatafileLock(datafile, "datafile-%s" % datafile.id) as lock:
            if lock:
                schema = Schema.objects.get(namespace=schema_name)
                if DatafileParameterSet.objects.filter(
                        schema=schema, datafile=datafile).exists():
                    LOGGER.debug(
                        """HSM DatafileParameterSet already exists for: %s""",
                        datafile.id
                    )
                    return

                ps = DatafileParameterSet(schema=schema, datafile=datafile)
                ps.save()

                param_name = ParameterName.objects.get(
                    schema=schema,
                    name="online"
                )

                dfp = DatafileParameter(parameterset=ps, name=param_name)
                dfp.string_value = str(df_online(datafile, min_file_size))
                dfp.save()

    else:
        LOGGER.warning(
            """Cannot determine online/offline status for datafile %s "
            "is not verified""",
            datafile.id
        )


@task(name="mytardis_hsm.update_df_status")
def update_df_status():
    """Celery task that checks for a change in HSM status for
    all online (verified) DataFiles.

    Notes
    -----
    Minimum size of files to check HSM status of is read from
    settings otherwise default is 500 bytes.
    """
    param_name = ParameterName.objects.get(
        schema__namespace=HSM_SCHEMA_NAMESPACE,
        name="online")

    online_params = DatafileParameter.objects.filter(
        parameterset__schema__namespace=HSM_SCHEMA_NAMESPACE,
        name=param_name,
        string_value="True"
    ).select_related('parameterset__datafile')

    min_file_size = getattr(settings, "HSM_MIN_FILE_SIZE", 500)
    for param in online_params:
        df = param.parameterset.datafile

        if df.verified:
            if not df_online(df, min_file_size):
                param.string_value = "False"
                param.save()
