# -*- coding: utf-8 -*-

"""Tests for `mytardis_hsm` package."""
from __future__ import unicode_literals
import os
import unittest

from tempfile import NamedTemporaryFile

from mytardis_hsm.hsm import online


class TestHSM(unittest.TestCase):
    """Tests for HSM detection utils"""
    def setUp(self):
        # Create a file with zero blocks
        with NamedTemporaryFile(delete=False) as tf:
            tf.seek(1048576-1)
            tf.write(b"\0")
            self.tmp_path = tf.name

    def tearDown(self):
        os.remove(self.tmp_path)

    def test_000_online_file(self):
        """online should return True for files with block_size > 0"""
        self.assertTrue(online(self.tmp_path))