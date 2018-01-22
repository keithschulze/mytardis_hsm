# -*- coding: utf-8 -*-

"""Tests for `mytardis_hsm` package."""
from django.contrib.auth.models import User
from django.test import TestCase
from mytardis_hsm import mytardis_hsm
from tardis.tardis_portal.models import (Experiment, Dataset,
    Facility, Group, Instrument)


class TestMytardisHSM(TestCase):
    """Tests for `mytardis_hsm` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        self.user = User.objects.create_user("doctor", '',
                                             "pwd")

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_number_of_users(self):
        """Checks the number of users in the database"""
        u = User.objects.all().count()
        self.assertEqual(u, 1)

    def test_001_number_of_experiments(self):
        """Creates an experiments, saves it and then checks the
        number of experiments in the database is equal to 1.
        """
        exp = Experiment(title="Wonderful",
                         institution_name="Monash University",
                         created_by = self.user)
        exp.save()
        qExp = Experiment.objects.all()
        self.assertEqual(qExp.count(), 1)

    def test_003_online_should_return_false_when_a_file_has_zero_blocks(self):
        """Calling online should return False for a DFO id where the underlying
        file reports block size of 0 in a stat call.
        """
        exp = Experiment(title="Wonderful",
                         institution_name="Monash University",
                         created_by = self.user)
        exp.save()

        group = Group(name="Group1")
        group.save()

        facility = Facility(name="Test Facility",
                            manager_group=group)
        facility.save()

        inst = Instrument(name="Test Instrument1",
                          facility=facility)
        inst.save()

        dataset = Dataset(description="Dataset1",
                          instrument=inst)
        dataset.save()
