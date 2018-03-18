# -*- coding: utf-8 -*-

"""Test mytardis_hsm celery tasks"""

try:
    import unittest.mock
except ImportError:
    import mock

import os
import tempfile

from datetime import datetime
from django.conf import settings
from django.contrib.auth.models import User
from django.test import TestCase
from mytardis_hsm.mytardis_hsm import (DEFAULT_HSM_CLASSES,
                                       HSM_SCHEMA_NAMESPACE)
from mytardis_hsm.tasks import update_df_status
from mytardis_hsm.utils import DatafileLock
from tardis.tardis_portal.models import (Experiment, Dataset,
    Facility, Group, Instrument, DataFileObject, StorageBox,
    StorageBoxAttribute, StorageBoxOption, DataFile, DatafileParameter,
    Schema, ParameterName, DatafileParameterSet)
from tests.test_hsm import Stats


class MyTardisHSMTasksTestCase(TestCase):
    """Tests for mytardis_hsm.tasks"""

    def setUp(self):
        """Setup test fixtures if needed."""
        settings.REQUIRE_DATAFILE_SIZES = False
        settings.REQUIRE_DATAFILE_CHECKSUMS = False
        self.user = User.objects.create_user("doctor", '',
                                             "pwd")

        self.exp = Experiment(title="Wonderful",
                              institution_name="Monash University",
                              created_by=self.user)
        self.exp.save()

        group = Group(name="Group1")
        group.save()

        facility = Facility(name="Test Facility",
                            manager_group=group)
        facility.save()

        inst = Instrument(name="Test Instrument1",
                          facility=facility)
        inst.save()

        self.dataset = Dataset(description="Dataset1",
                               instrument=inst)
        self.dataset.save()

        storage_classes = getattr(settings,
                                  "HSM_STORAGE_CLASSES",
                                  DEFAULT_HSM_CLASSES)
        self.sbox1 = StorageBox(name="SBOX1",
                                django_storage_class=storage_classes[0],
                                status='online', max_size=256)
        self.sbox1.save()
        sbox1_attr = StorageBoxAttribute(storage_box=self.sbox1,
                                         key='type',
                                         value=StorageBox.DISK)
        sbox1_attr.save()
        sbox1_loc_opt = StorageBoxOption(storage_box=self.sbox1,
                                         key="location",
                                         value=tempfile.gettempdir())
        sbox1_loc_opt.save()

        self.sbox2 = StorageBox(
            name="SBOX2",
            django_storage_class="any.non.disk.StorageSystem",
            status='offline', max_size=256)
        self.sbox2.save()
        sbox2_attr = StorageBoxAttribute(storage_box=self.sbox2,
                                         key='type',
                                         value=StorageBox.TAPE)
        sbox2_attr.save()

    @mock.patch("os.stat")
    def test_000_update_df_status_offline(self, mock_stat):
        """update_df_status should check the online status of
        preferred DFOs for all previously online datafiles and
        update online Parameter to 'False' for any offline files."""
        df1 = DataFile(dataset=self.dataset,
                       filename="test_df.jpg")
        df1.save()
        dfo1 = DataFileObject(datafile=df1,
                              storage_box=self.sbox1,
                              uri="stream/test.jpg",
                              verified=True)
        dfo1.save()
        df1.verify()

        schema = Schema.objects.get(namespace=HSM_SCHEMA_NAMESPACE)
        ps = DatafileParameterSet(schema=schema, datafile=df1)
        ps.save()

        param_name = ParameterName.objects.get(schema=schema, name="online")
        param = DatafileParameter(parameterset=ps, name=param_name)
        param.string_value = True
        param.save()

        mock_stat.return_value = Stats(st_size=10000,
                                       st_blocks=0,
                                       st_mtime=datetime.now())
        update_df_status(min_file_size=500)

        params = DatafileParameter.objects.filter(
            parameterset__schema__namespace=HSM_SCHEMA_NAMESPACE,
            parameterset__datafile=df1)

        self.assertEquals(params.count(), 1)
        self.assertEquals(params[0].string_value, "False")

    @mock.patch("os.stat")
    def test_001_update_df_status_online(self, mock_stat):
        """update_df_status should check the online status of
        preferred DFOs for all previously online datafiles and
        leave the online Parameter as 'True' for any online files."""
        df1 = DataFile(dataset=self.dataset,
                       filename="test_df.jpg")
        df1.save()
        dfo1 = DataFileObject(datafile=df1,
                              storage_box=self.sbox1,
                              uri="stream/test.jpg",
                              verified=True)
        dfo1.save()
        df1.verify()

        schema = Schema.objects.get(namespace=HSM_SCHEMA_NAMESPACE)
        ps = DatafileParameterSet(schema=schema, datafile=df1)
        ps.save()

        param_name = ParameterName.objects.get(schema=schema, name="online")
        param = DatafileParameter(parameterset=ps, name=param_name)
        param.string_value = True
        param.save()

        mock_stat.return_value = Stats(st_size=10000,
                                       st_blocks=100,
                                       st_mtime=datetime.now())
        update_df_status(min_file_size=500)

        params = DatafileParameter.objects.filter(
            parameterset__schema__namespace=HSM_SCHEMA_NAMESPACE,
            parameterset__datafile=df1)

        self.assertEquals(params.count(), 1)
        self.assertEquals(params[0].string_value, "True")

    @mock.patch('mytardis_hsm.mytardis_hsm.df_online')
    @mock.patch("os.stat")
    def test_002_update_df_status_skip_unverified(self, mock_stat, df_online):
        """update_df_status should skip files that are unverified"""
        df2 = DataFile(dataset=self.dataset,
                       filename="test_df2.jpg")
        df2.save()
        dfo2 = DataFileObject(datafile=df2,
                              storage_box=self.sbox1,
                              uri="stream/test_df2.jpg")
        dfo2.save()

        schema = Schema.objects.get(namespace=HSM_SCHEMA_NAMESPACE)
        ps2 = DatafileParameterSet(schema=schema, datafile=df2)
        ps2.save()

        param_name = ParameterName.objects.get(schema=schema, name="online")
        param2 = DatafileParameter(parameterset=ps2, name=param_name)
        param2.string_value = True
        param2.save()

        mock_stat.return_value = Stats(st_size=10000,
                                       st_blocks=100,
                                       st_mtime=datetime.now())
        update_df_status(500)
        df_online.assert_not_called()

    @mock.patch('mytardis_hsm.tasks.df_online', autopec=True)
    @mock.patch("os.stat")
    def test_003_update_df_status_skip_offline(self, mock_stat, mock_df_online):
        """update_df_status should skip any files that have previously
        marked as offline."""
        df2 = DataFile(dataset=self.dataset,
                       filename="test_df2.jpg")
        df2.save()
        dfo2 = DataFileObject(datafile=df2,
                              storage_box=self.sbox1,
                              uri="stream/test_df2.jpg",
                              verified=True)
        dfo2.save()
        df2.verify()

        schema = Schema.objects.get(namespace=HSM_SCHEMA_NAMESPACE)
        ps2 = DatafileParameterSet(schema=schema, datafile=df2)
        ps2.save()

        param_name = ParameterName.objects.get(schema=schema, name="online")
        param2 = DatafileParameter(parameterset=ps2, name=param_name)
        param2.string_value = False
        param2.save()

        mock_stat.return_value = Stats(st_size=10000,
                                       st_blocks=100,
                                       st_mtime=datetime.now())
        update_df_status(500)

        # assert that the df_online method wasn't called
        self.assertEquals(mock_df_online.call_count, 0)

    @mock.patch("os.stat")
    def test_004_create_df_status(self, mock_stat):
        """When a new datafile record is verified, metadata for it's
        online/offline status should be created and populated with the
        current online status"""

        with self.modify_settings(
            # MIDDLEWARE_CLASSES={
            #     'append': 'tardis.tardis_portal.filters.FilterInitMiddleware'
            # },
            FILTER_MIDDLEWARE={
                'append': [("tardis.tardis_portal.filters",
                           "FilterInitMiddleware")]
            },
            POST_SAVE_FILTERS={
                'append': [('mytardis_hsm.filters.make_filter',
                            ["HSMFilter",
                             "http://tardis.edu.au/hsm/1"])]
            }
        ):
            mock_stat.return_value = Stats(st_size=10000,
                                           st_blocks=100,
                                           st_mtime=datetime.now())

            temp = tempfile.NamedTemporaryFile(dir=tempfile.gettempdir())
            temp_name = os.path.basename(temp.name)
            df2 = DataFile(dataset=self.dataset,
                           filename=temp_name)
            df2.save()
            dfo2 = DataFileObject(datafile=df2,
                                  storage_box=self.sbox1,
                                  uri=temp_name)
            dfo2.save()
            df2.verify()

            param_name = ParameterName.objects.get(
                schema__namespace=HSM_SCHEMA_NAMESPACE,
                name="online")

            paramset = DatafileParameterSet.objects.get(
                schema__namespace=HSM_SCHEMA_NAMESPACE,
                datafile=df2)

            param = DatafileParameter.objects.get(
                parameterset=paramset,
                name=param_name
            )

            self.assertEquals(param.string_value, "True")
            temp.close()
