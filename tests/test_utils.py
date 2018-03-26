# -*- coding: utf-8 -*-

"""Test for the utils module"""

import time

from django.conf import settings
from django.contrib.auth.models import User
from django.test import TestCase
from mytardis_hsm.mytardis_hsm import DEFAULT_HSM_CLASSES
from mytardis_hsm.utils import DatafileLock
from tardis.tardis_portal.models import (Experiment, Dataset, Facility, Group,
                                         Instrument, DataFileObject,
                                         StorageBox, StorageBoxAttribute,
                                         StorageBoxOption, DataFile)


class MyTardisHSMUtilsTestCase(TestCase):
    """Test cases for the MyTardisHSM utils module"""

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
                                         value="/dummy/path")
        sbox1_loc_opt.save()

        self.sbox2 = StorageBox(
            name="SBOX2",
            django_storage_class="any.non.disk.StorageSystem",
            status='offline', max_size=256
        )
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
                                   uri="stream/test.jpg")
        self.dfo1.save()

    def tearDown(self):
        """Remove stuff"""

    def test_000_lock_datafile(self):
        """We should be able to lock a datafile to prevent concurrent access"""
        with DatafileLock(self.df1, "dummy_oid1") as lock1:
            if lock1:
                with DatafileLock(self.df1, "dummy_oid2") as lock2:
                    self.assertTrue(lock1)
                    self.assertFalse(lock2)

    def test_001_datafile_lock_expiry(self):
        """A datafile lock should not release until the expiry has been
        reached"""
        with DatafileLock(self.df1, "dummy_oid1", expires=2) as lock1:
            self.assertTrue(lock1)

        # If we retry lock right away, lock acquisition should fail because
        # expiry hasn't been reached
        with DatafileLock(self.df1, "dummy_oid1_1") as lock1:
            self.assertFalse(lock1)

        # wait 2s for lock to release
        time.sleep(2)

        # If we retry acquiring the lock now it should succeed
        with DatafileLock(self.df1, "dummy_oid1_2") as lock1:
            self.assertTrue(lock1)
