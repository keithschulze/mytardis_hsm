# -*- coding: utf-8 -*-
"""HSM module. Utilities for detecting whethers files have
been HSM'd"""
from __future__ import unicode_literals


def _stat_os(path):
    """Use os.stat to calculate size and block number

    Parameters
    ----------
    path : str
        Path to file to calculate size and block number for

    Returns
    -------
    tuple
        tuple of size and block number i.e., (size, blocks)
    """
    import os
    stats = os.stat(path)
    return stats.st_size, stats.st_blocks


def _stat_subprocess(path):
    """Use subprocess to call underlying stat command. Uses
    regexp to isolate size and block number.

    Parameters
    ----------
    path : str
        Path to file to calculate size and block number for

    Returns
    -------
    tuple
        tuple of size and block number i.e., (size, blocks)

    Raises
    ------
    Exception
        If unable to detect size or blocks successfully using subprocess
    """
    import re
    import subprocess

    proc = subprocess.Popen(
        ['stat', path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, _ = proc.communicate()

    for line in stdout.splitlines():
        match = re.search(r"^.*Size: (\d+).*Blocks: (\d+).*$", line)
        if match:
            size = int(match.groups()[0])
            blocks = int(match.groups()[1])
            return size, blocks

    raise Exception("Unable to detect size or blocks for %s\n" % path)


def online(path, min_file_size=350):
    """Detects whether a file is online or offline (on tape).

    Basically this function checks the size and block number.
    If size > 0 and block number == 0, this is typically a sign that
    files are on tape.

    We attempt to os.stat to determine file size and block number;
    however, this doesn't work on all unix-like systems, so if it fails
    we attempt to determine them using a subprocess proc call to stat
    which we aim to match using a regexp.

    Notes
    -----
    We set a minimum size since very small files can be stored
    in the inode and hence have a 0 blksize.

    Parameters
    ----------
    path : str
        Path to the file for which we want to determine size and and block
        number.
    min_file_size : int, optional
        minimum size of files that could be stored in
        in the inode.

    Returns
    -------
    bool
        specifies whether the file in online i.e., not on tape.
    """
    try:
        size, blocks = _stat_os(path)
    except AttributeError:
        size, blocks = _stat_subprocess(path)

    if size > min_file_size and blocks == 0:
        return False
    else:
        return True
