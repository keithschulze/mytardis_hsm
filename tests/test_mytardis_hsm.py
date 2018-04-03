# -*- coding: utf-8 -*-

"""Tests for `mytardis_hsm` package."""
try:
    import unittest.mock
except ImportError:
    import mock

from datetime import datetime
from django.conf import settings
from django.contrib.auth.models import User
from django.test import TestCase
from mytardis_hsm.mytardis_hsm import (dfo_online, df_online,
                                       DEFAULT_HSM_CLASSES,
                                       StorageClassNotSupportedError,
                                       DataFileNotVerified,
                                       DataFileObjectNotVerified)
from tardis.tardis_portal.models import (Experiment, Dataset, Facility, Group,
                                         Instrument, DataFileObject,
                                         StorageBox, StorageBoxAttribute,
                                         StorageBoxOption, DataFile, Schema)
from tests.test_hsm import Stats


class TestMytardisHSM(TestCase):
    """Tests for `mytardis_hsm` package."""

    def setUp(self):
        """Setup test fixtures if needed."""
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

        self.inst = Instrument(name="Test Instrument1",
                               facility=facility)
        self.inst.save()

        self.dataset = Dataset(description="Dataset1",
                               instrument=self.inst)
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
                                         value="/dummy/path")
        sbox1_loc_opt.save()

        self.sbox2 = StorageBox(
            name="SBOX2",
            django_storage_class="any.non.disk.StorageSystem",
            status='offline',
            max_size=256)
        self.sbox2.save()
        sbox2_attr = StorageBoxAttribute(storage_box=self.sbox2,
                                         key='type',
                                         value=StorageBox.TAPE)
        sbox2_attr.save()

        self.df1 = DataFile(dataset=self.dataset,
                            filename="test_df.jpg")
        self.df1.save()
        self.dfo1 = DataFileObject(datafile=self.df1,
                                   storage_box=self.sbox1,
                                   uri="stream/test.jpg",
                                   verified=True)
        self.dfo1.save()
        self.df1.verify()

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_number_of_users(self):
        """Checks the number of users in the database"""
        user = User.objects.all().count()
        self.assertEqual(user, 1)

    def test_001_number_of_experiments(self):
        """Creates an experiments, saves it and then checks the
        number of experiments in the database is equal to 1.
        """
        exps = Experiment.objects.all()
        self.assertEqual(exps.count(), 1)

    def test_003_number_of_datasets(self):
        """Checks that number of experiments is equal to 1
        """
        self.assertEqual(Dataset.objects.all().count(), 1)

    @mock.patch("os.stat")
    def test_003_dfo_online(self, mock_stat):
        """HSM.online should return True when a DFOs underlying file
        has > 0 blocks"""
        mock_stat.return_value = Stats(st_size=10000,
                                       st_blocks=100,
                                       st_mtime=datetime.now())
        self.assertTrue(dfo_online(self.dfo1))

    @mock.patch("os.stat")
    def test_004_dfo_offline(self, mock_stat):
        """HSM.online should return False when a DFOs underlying file
        is > 350 bytes and 0 blocks"""
        mock_stat.return_value = Stats(st_size=10000,
                                       st_blocks=0,
                                       st_mtime=datetime.now())
        self.assertFalse(dfo_online(self.dfo1))

    def test_005_dfo_non_disk(self):
        """Files in StorageBoxes with a django_storage_class other than
        those specified in settings should not be processed"""
        dfo2 = DataFileObject(datafile=self.df1,
                              storage_box=self.sbox2,
                              uri="stream/test.jpg",
                              verified=True)
        self.assertRaises(StorageClassNotSupportedError, dfo_online, dfo2)

        with self.settings(
            HSM_STORAGE_CLASSES=["random.storage.CLASS"]
        ):
            self.assertRaises(
                StorageClassNotSupportedError,
                dfo_online,
                self.dfo1
            )

    def test_006_hsm_schema(self):
        """HSM schema should be installed"""
        schemas = Schema.objects\
            .filter(namespace="http://tardis.edu.au/schemas/hsm/datafile/1")\
            .count()
        self.assertEqual(schemas, 1)

    def test_007_dfo_unverified(self):
        """df_online and dfo_online should raise Exception for an unverfied DataFile or
        DataFileObject, respectively"""
        df2 = DataFile(dataset=self.dataset,
                       filename="test_df.jpg")
        df2.save()
        self.assertRaises(DataFileNotVerified, df_online, df2)

        dfo2 = DataFileObject(datafile=df2,
                              storage_box=self.sbox1,
                              uri="stream/test.jpg",
                              verified=False)
        dfo2.save()

        self.assertRaises(DataFileObjectNotVerified, dfo_online, dfo2)
