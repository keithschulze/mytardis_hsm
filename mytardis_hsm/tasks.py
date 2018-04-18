# -*- coding: utf-8 -*-

"""mytardis_hsm tasks module which house celery tasks for assessing
HSM status of Datafiles in MyTardis"""

import logging

from celery.task import task
from tardis.tardis_portal.models import (
    DatafileParameter,
    DatafileParameterSet,
    ParameterName,
    Schema
)
from .mytardis_hsm import (
    DatafileLock,
    DataFileNotVerified,
    DataFileObjectNotVerified,
    df_online
)


LOGGER = logging.getLogger(__name__)


def _create_online_handler(datafile, schema):
    """Creates callback to handle result of checking online status for a
    DatafileObject

    Parameters
    ----------
    datafile: DataFile
        DataFile to create online status for
    schema: Schema
        Schema that description online ParameterName

    Returns
    -------
    func: function
        Single argument function that accepts the result of checking online
        status of datafile. Result will be a boolean wrapped in a Try, where
        a successful check will be wrapped in a `Success`, while a check that
        failed will be the exception wrapped in a `Failure`.

    Raises
    ------
    IOError
        If online check couldn't find file or couldn't compute size or number
        of blocks
    """
    def _handler(result):
        try:
            output = result.get_or_raise()
            with DatafileLock(datafile, "datafile-%s" % datafile.id) as lock:
                if lock:
                    ps = DatafileParameterSet(schema=schema,
                                              datafile=datafile)
                    ps.save()

                    param_name = ParameterName.objects.get(
                        schema=schema,
                        name="online"
                    )

                    dfp = DatafileParameter(parameterset=ps,
                                            name=param_name)
                    dfp.string_value = str(output)
                    dfp.save()
        except IOError as ioerr:
            LOGGER.error("IOError for DF: %s\n%s", datafile.id, ioerr)

    return _handler


@task(name="mytardis_hsm.create_df_status")
def create_df_status(datafile, schema_name):
    """Post-save celery task that checks online status of new file and create
    HSM metadata to track online status.

    Parameters
    ----------
    datafile: DataFile
        datafile to check and create online/offline status
        metadata for
    schema_name: Schema
        name of Schema which describes ParameterNames

    Returns
    -------
    None
    """
    if datafile.verified:
        try:
            schema = Schema.objects.get(namespace=schema_name)
            if DatafileParameterSet.objects.filter(
                    schema=schema, datafile=datafile).exists():
                LOGGER.debug(
                    "HSM DatafileParameterSet already exists for: %s",
                    datafile.id
                )
                return

            df_online(
                datafile,
                callback=_create_online_handler(datafile, schema)
            )
        except Schema.DoesNotExist as err:
            LOGGER.error("Schema with namespace: %s does not exist.\n%s",
                         schema_name, err)
        except DataFileNotVerified as exc:
            LOGGER.debug(exc)
        except DataFileObjectNotVerified as exc:
            LOGGER.debug(exc)
    else:
        LOGGER.debug(
            "Not determing online/offline status for datafile %s "
            "because it is not verified",
            datafile.id
        )


def _update_online_handler(param):
    """Callback to handle updating online status for Datafile

    Parameters
    ----------
    param: DatafileParameter
        DatafileParameter to update


    Returns
    -------
    None

    """
    def _handler(result):
        import pdb;pdb.set_trace()
        try:
            param.string_value = result.get_or_raise()
            param.save()
        except IOError as ioerr:
            LOGGER.error(ioerr)

    return _handler


@task(name="mytardis_hsm.update_df_status")
def update_df_status(schema_namespace):
    """Celery task that checks for a change in HSM status for
    all online (verified) DataFiles.

    Returns
    -------
    None

    Notes
    -----
    Minimum size of files to check HSM status of is read from
    settings otherwise default is 500 bytes.
    """
    try:
        schema = Schema.objects.get(namespace=schema_namespace)
        param_name = ParameterName.objects.get(
            schema=schema,
            name="online"
        )

        online_params = DatafileParameter.objects.filter(
            parameterset__schema=schema,
            name=param_name
            # string_value="True"
        ).select_related('parameterset__datafile')

        for param in online_params:
            df = param.parameterset.datafile

            if df.verified:
                df_online(df, callback=_update_online_handler(param))
    except DataFileNotVerified as exc:
        LOGGER.warning(exc)
    except DataFileObjectNotVerified as exc:
        LOGGER.warning(exc)
    except Schema.DoesNotExist as exc:
        LOGGER.error("Schema with namespace %s does not exist:\n%s",
                     schema_namespace, exc)
    except ParameterName.DoesNotExist as exc:
        LOGGER.error("ParameterName 'online' does not exist in Schema %s:\n%s",
                     schema_namespace, exc)
