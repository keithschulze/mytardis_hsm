# -*- coding: utf-8 -*-

"""Django migration to provide initial schema data for the
mytardis_hsm app."""

from django.conf import settings
from django.db import migrations
from mytardis_hsm.mytardis_hsm import HSM_SCHEMA_NAMESPACE, df_online


def forward_func(apps, schema_editor):
    """Create HSM Schema and online ParameterName"""
    Dataset = apps.get_model("tardis_portal", "Dataset")
    DataFile = apps.get_model("tardis_portal", "DataFile")
    Schema = apps.get_model("tardis_portal", "Schema")
    ParameterName = apps.get_model("tardis_portal", "ParameterName")
    DatasetParameterSet = apps.get_model("tardis_portal",
                                         "DatasetParameterSet")
    DatafileParameterSet = apps.get_model("tardis_portal",
                                          "DatafileParameterSet")
    DatafileParameter = apps.get_model("tardis_portal",
                                       "DatafileParameter")

    db_alias = schema_editor.connection.alias
    schema = Schema.objects\
        .using(db_alias)\
        .create(namespace=HSM_SCHEMA_NAMESPACE,
                name="MyTardis HSM Schema",
                hidden=True,
                type=3,
                immutable=True)

    param_name = ParameterName.objects\
        .using(db_alias)\
        .create(
            name="online",
            full_name="Is Online",
            schema=schema)

    min_file_size = getattr(settings, "HSM_MIN_FILE_SIZE", 500)
    for ds in Dataset.objects.using(db_alias).all():
        DatasetParameterSet.objects\
            .using(db_alias)\
            .create(schema=schema, dataset=ds)
        # ParameterSetManager(schema=schema, parentObject=ds)

    for df in DataFile.objects.using(db_alias).all():
        if df.verified:
            dfps = DatafileParameterSet.objects\
                .using(db_alias)\
                .create(schema=schema, datafile=df)

            dp = DatafileParameter.objects\
                .using(db_alias)\
                .create(parameterset=dfps, name=param_name)
            dp.string_value = str(df_online(df, min_file_size))
            dp.save()


def reverse_func(apps, schema_editor):
    """Remove HSM Schema and online ParameterName"""
    Schema = apps.get_model("tardis_portal", "Schema")
    ParameterName = apps.get_model("tardis_portal", "ParameterName")
    DatasetParameterSet = apps.get_model("tardis_portal",
                                         "DatasetParameterSet")
    DatafileParameterSet = apps.get_model("tardis_portal",
                                          "DatafileParameterSet")
    DatafileParameter = apps.get_model("tardis_portal",
                                       "DatafileParameter")
    db_alias = schema_editor.connection.alias
    schema = Schema.objects.using(db_alias)\
        .get(namespace=HSM_SCHEMA_NAMESPACE)

    param_names = ParameterName.objects.using(db_alias)\
        .filter(schema=schema)

    for pn in param_names:
        DatafileParameter.objects\
            .using(db_alias)\
            .filter(name=pn).delete()
        pn.delete()

    DatafileParameterSet.objects\
        .using(db_alias)\
        .filter(schema=schema).delete()
    DatasetParameterSet.objects\
        .using(db_alias)\
        .filter(schema=schema).delete()
    schema.delete()


class Migration(migrations.Migration):
    """HSM Schema and online ParameterName migrations"""
    dependencies = [
        ("tardis_portal", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(forward_func, reverse_func),
    ]
