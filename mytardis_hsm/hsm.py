# -*- coding: utf-8 -*-
"""HSM module. Utilities for detecting whethers files have
been HSM'd"""


def _stat_os(path):
    """Use os.stat to calculate size and block number
    @param path path to file to calculate size and block number of
    @return: tuple containing size and block number
    """
    import os
    stats = os.stat(path)
    return stats.st_size, stats.st_blocks


def _stat_subprocess(path):
    """Use subprocess to call underlying stat command. Uses
    regexp to isolate size and block number."""
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

    raise Exception("Unable to detect st_blksize for %s\n" % path)


def online(path, min_file_size=30):
    """Detects whether a file is online or offline (HSM'd) to
    tape. Basically this function checks the size and block number.
    If size > 0 and block number == 0, this is typically a sign that
    files are on tape.

    We attempt to os.stat to determine file size and block number;
    however, this doesn't work on all *nix systems, so if it fails
    we attempt to determine them using a subprocess proc call to stat
    which we aim to match using a regexp.

    Note: We set a minimum size since very small files can be stored
    in the inode and hence have a 0 blksize.

    @param path path to the file for which we want to determine size and
        and block number.
    @param min_file_size minimum size of files that could be stored in
        in the inode.
    @return boolean that specifies whether the file in online i.e., not
        on tape.
    """
    try:
        size, blocks = _stat_os(path)
    except AttributeError:
        try:
            size, blocks = _stat_subprocess(path)
        except Exception as exception:
            raise exception

    if size > min_file_size and blocks == 0:
        return False
    else:
        return True
