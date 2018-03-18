# -*- coding: utf-8 -*-

"""Tests for `mytardis_hsm` package."""
from __future__ import unicode_literals
import unittest
try:
    from unittest import mock
except ImportError:
    import mock

from collections import namedtuple
from datetime import datetime

from mytardis_hsm.hsm import online


Stats = namedtuple('Stats', ['st_size', 'st_blocks', 'st_mtime'])


class TestHSM(unittest.TestCase):
    """Tests for HSM detection utils"""

    def setUp(self):
        """Do some setup"""
        self.file_path = "/path/to/a/fictional/file"

    @mock.patch("os.stat")
    def test_000_online_file(self, mock_stat):
        """online should return True for files with more than 0 blocks"""
        mock_stat.return_value = Stats(st_size=1000000, st_blocks=100, st_mtime=datetime.now())

        self.assertTrue(online(self.file_path))

    @mock.patch("os.stat")
    def test_001_offline_file(self, mock_stat):
        """online should return False for files with 0 blocks"""
        mock_stat.return_value = Stats(st_size=1000000, st_blocks=0, st_mtime=datetime.now())

        self.assertFalse(online(self.file_path))

    @mock.patch("os.stat")
    def test_002_small_file(self, mock_stat):
        """Small files are stored in the inode and hence have 0 blocks,
        but should nevertheless be reported as online"""
        mock_stat.return_value = Stats(st_size=20, st_blocks=0, st_mtime=datetime.now())

        self.assertTrue(online(self.file_path))
