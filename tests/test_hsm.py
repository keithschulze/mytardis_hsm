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
from multiprocessing.pool import AsyncResult
from multiprocessing.dummy import Pool

from mytardis_hsm.hsm import HSMManager, online


Stats = namedtuple('Stats', ['st_size', 'st_blocks', 'st_mtime'])


class TestHSM(unittest.TestCase):
    """Tests for HSM detection utils"""

    def setUp(self):
        """Do some setup"""
        self.file_path = "/path/to/a/fictional/file"

    @mock.patch("os.stat")
    def test_000_online_file(self, mock_stat):
        """online should return True for files with more than 0 blocks"""
        mock_stat.return_value = Stats(st_size=1000000, st_blocks=100,
                                       st_mtime=datetime.now())

        self.assertTrue(online(self.file_path, 350))

    @mock.patch("os.stat")
    def test_001_offline_file(self, mock_stat):
        """online should return False for files with 0 blocks"""
        mock_stat.return_value = Stats(st_size=1000000, st_blocks=0,
                                       st_mtime=datetime.now())

        self.assertFalse(online(self.file_path, 350))

    @mock.patch("os.stat")
    def test_002_small_file(self, mock_stat):
        """Small files are stored in the inode and hence have 0 blocks,
        but should nevertheless be reported as online"""
        mock_stat.return_value = Stats(st_size=20, st_blocks=0,
                                       st_mtime=datetime.now())

        self.assertTrue(online(self.file_path, 350))

    def test_003_hsm_manager_retrieve(self):
        """SimpleFileRetriever.retrieve should return Success(path) if it
        successfully retrieves a file by reading the first byte"""
        with mock.patch(
            "mytardis_hsm.hsm.open",
            mock.mock_open(read_data="test")
        ):
            retriever = HSMManager(Pool(2))
            retriever.retrieve(
                self.file_path,
                callback=lambda success: self.assertTrue(
                    success.get_or_raise()
                )
            )

    def test_003_hsm_manager_retrieve_batch(self):
        """SimpleFileRetriever.retrieve_batch should return [Success(path)] if
        it successfully retrieves a batch of files by reading the firt byte of
        each."""
        with mock.patch(
            "mytardis_hsm.hsm.open",
            mock.mock_open(read_data="test")
        ):
            retriever = HSMManager(Pool(2))
            paths = [self.file_path,
                     self.file_path,
                     self.file_path,
                     self.file_path]
            retriever.retrieve_batch(
                paths,
                callback=lambda suc: map(lambda r: r.map(self.assertTrue),
                                         suc)
            )
