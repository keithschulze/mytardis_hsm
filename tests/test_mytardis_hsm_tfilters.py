# -*- coding: utf-8 -*-

"""Test HSM App post-save filter related code for MyTardis"""

try:
    import unittest.mock
except ImportError:
    import mock

import os
import tempfile

from datetime import datetime
from django.conf import settings
from django.contrib.auth.models import User
from django.test import (TestCase, modify_settings)
from mytardis_hsm.models import HSMConfig
from mytardis_hsm.mytardis_hsm import (DEFAULT_HSM_CLASSES,
                                       HSM_DATAFILE_NAMESPACE,
                                       OnlineParamExistsError,
                                       dataset_online,
                                       experiment_online)
from mytardis_hsm.tasks import create_df_status
from tardis.tardis_portal.models import (Experiment, Dataset, Facility, Group,
                                         Instrument, DataFileObject,
                                         StorageBox, StorageBoxAttribute,
                                         StorageBoxOption, DataFile,
                                         DatafileParameter, ParameterName,
                                         DatafileParameterSet)
from tests.test_hsm import Stats


@modify_settings(
    FILTER_MIDDLEWARE={
        'append': [("tardis.tardis_portal.filters",
                    "FilterInitMiddleware")]
    },
    POST_SAVE_FILTERS={
        'append': [('mytardis_hsm.filters.make_filter',
                    ["HSMFilter",
                     "http://tardis.edu.au/schemas/hsm/datafile/1"])]
    }
)
class MyTardisHSMTFiltersTestCase(TestCase):
    """Testing MyTardis HSM App filters"""

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

        self.dataset.experiments.add(self.exp)

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

        sbox1_hsm_config = HSMConfig(
            storage_box=self.sbox1,
            status_checker=HSMConfig.FILESYSTEM
        )
        sbox1_hsm_config.save()


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
    def test_001_create_df_status(self, mock_stat):
        """When a new datafile record is verified, metadata for it's
        online/offline status should be created and populated with the
        current online status"""
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
            schema__namespace=HSM_DATAFILE_NAMESPACE,
            name="online")

        paramset = DatafileParameterSet.objects.get(
            schema__namespace=HSM_DATAFILE_NAMESPACE,
            datafile=df2)

        param = DatafileParameter.objects.get(
            parameterset=paramset,
            name=param_name
        )

        self.assertEquals(param.string_value, "True")
        temp.close()

    @mock.patch("os.stat")
    def test_002_no_duplicate_params(self, mock_stat):
        """Datafile should only ever have one online param"""
        mock_stat.return_value = Stats(st_size=10000,
                                       st_blocks=100,
                                       st_mtime=datetime.now())

        df1 = DataFile(dataset=self.dataset,
                       filename="test_df.jpg")
        df1.save()
        dfo1 = DataFileObject(datafile=df1,
                              storage_box=self.sbox1,
                              uri="stream/test.jpg",
                              verified=True)
        dfo1.save()
        df1.verify()

        param_name = ParameterName.objects.get(
            schema__namespace=HSM_DATAFILE_NAMESPACE,
            name="online")

        paramset = DatafileParameterSet.objects.get(
            schema__namespace=HSM_DATAFILE_NAMESPACE,
            datafile=df1)

        params = DatafileParameter.objects.filter(
            parameterset=paramset,
            name=param_name
        )

        self.assertEquals(params.count(), 1)

        self.assertRaises(OnlineParamExistsError,
                          create_df_status(df1, HSM_DATAFILE_NAMESPACE))

        params = DatafileParameter.objects.filter(
            parameterset=paramset,
            name=param_name
        )

        self.assertEquals(params.count(), 1)

    @mock.patch("os.stat")
    def test_003_offline_dataset(self, mock_stat):
        """A dataset should be offline if any datafiles are offline"""
        mock_stat.return_value = Stats(st_size=10000,
                                       st_blocks=0,
                                       st_mtime=datetime.now())
        ds = Dataset(description="Dataset2",
                     instrument=self.inst)
        ds.save()

        df2 = DataFile(dataset=ds,
                       filename="test_file.jpg")
        df2.save()
        dfo2 = DataFileObject(datafile=df2,
                              storage_box=self.sbox1,
                              uri=df2.filename)
        dfo2.save()
        df2.verify()

        self.assertFalse(dataset_online(ds))


    @mock.patch("os.stat")
    def test_004_offline_experiment(self, mock_stat):
        """An experiment should be offline if any datafiles are offline"""
        mock_stat.return_value = Stats(st_size=10000,
                                       st_blocks=0,
                                       st_mtime=datetime.now())
        ds = Dataset(description="Dataset2",
                     instrument=self.inst)
        ds.save()
        ds.experiments.add(self.exp)

        df2 = DataFile(dataset=ds,
                       filename="test_file.jpg")
        df2.save()
        dfo2 = DataFileObject(datafile=df2,
                              storage_box=self.sbox1,
                              uri=df2.filename)
        dfo2.save()
        df2.verify()

        self.assertFalse(experiment_online(self.exp))
