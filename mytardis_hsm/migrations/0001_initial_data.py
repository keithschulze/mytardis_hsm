# -*- coding: utf-8 -*-

"""Django migration to provide initial schema data for the
mytardis_hsm app."""

from django.conf import settings
from django.db import migrations
from mytardis_hsm.mytardis_hsm import (
    HSM_DATAFILE_NAMESPACE,
    HSM_DATASET_NAMESPACE,
    df_online
)
from tardis.tardis_portal.models import (Schema, ParameterName, Dataset,
                                         DataFile, DatafileParameter,
                                         DatasetParameterSet,
                                         DatafileParameterSet)


def forward_func(apps, schema_editor):
    """Create HSM Schema and online ParameterName"""
    db_alias = schema_editor.connection.alias
    df_schema = Schema.objects\
        .using(db_alias)\
        .create(namespace=HSM_DATAFILE_NAMESPACE,
                name="Datafile HSM Schema",
                hidden=True,
                type=3,
                immutable=True)

    ds_schema = Schema.objects\
        .using(db_alias)\
        .create(namespace=HSM_DATASET_NAMESPACE,
                name="Dataset HSM Schema",
                hidden=True,
                type=2,
                immutable=True)

    param_name = ParameterName.objects\
        .using(db_alias)\
        .create(
            name="online",
            full_name="Is Online",
            schema=df_schema)

    min_file_size = getattr(settings, "HSM_MIN_FILE_SIZE", 500)
    for ds in Dataset.objects.using(db_alias).all():
        DatasetParameterSet.objects\
            .using(db_alias)\
            .create(schema=ds_schema, dataset=ds)

    for df in DataFile.objects.using(db_alias).all():
        if df.verified:
            dfps = DatafileParameterSet.objects\
                .using(db_alias)\
                .create(schema=df_schema, datafile=df)

            dp = DatafileParameter.objects\
                .using(db_alias)\
                .create(parameterset=dfps, name=param_name)
            dp.string_value = str(df_online(df, min_file_size))
            dp.save()


def reverse_func(apps, schema_editor):
    """Remove HSM Schema and online ParameterName"""
    db_alias = schema_editor.connection.alias
    df_schema = Schema.objects.using(db_alias)\
        .get(namespace=HSM_DATAFILE_NAMESPACE)

    ds_schema = Schema.objects.using(db_alias)\
        .get(namespace=HSM_DATASET_NAMESPACE)

    param_names = ParameterName.objects.using(db_alias)\
        .filter(schema=df_schema)

    for pn in param_names:
        DatafileParameter.objects\
            .using(db_alias)\
            .filter(name=pn).delete()
        pn.delete()

    DatafileParameterSet.objects\
        .using(db_alias)\
        .filter(schema=df_schema).delete()
    DatasetParameterSet.objects\
        .using(db_alias)\
        .filter(schema=ds_schema).delete()

    df_schema.delete()
    ds_schema.delete()


class Migration(migrations.Migration):
    """HSM Schema and online ParameterName migrations"""
    dependencies = [
        ("tardis_portal", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(forward_func, reverse_func),
    ]
