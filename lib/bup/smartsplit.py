from bup.helpers import *

import tarfile
from cStringIO import StringIO

def _round_bytes(n, tgt):
    rem = n % tgt
    if rem == 0:
        return n
    else:
        return n + tgt - rem

class TakeStream(object):
    def __init__(self, input, size):
        self._input = input
        self._size = size

    def read(self, n):
        if self._size == 0:
            return ''
        if n > self._size:
            n = self._size
        block = self._input.read(n)
        self._size -= len(block)
        return block

def _tar_blocks(f):
    block = f.read(512)
    while block:
        if len(block) < 512:
            log('WARNING: block too short\n')
            yield (block, None)
        else:
            try:
                info = tarfile.TarInfo.frombuf(block)
                nbytes = info.size
                to_read = _round_bytes(nbytes, 512)
                yield (block, TakeStream(f, to_read))
            except tarfile.EOFHeaderError:
                yield (block, None)
        block = f.read(512)

def split_tar(files):
    """Split a file like a tar file"""
    for f in files:
        for (header, body) in _tar_blocks(f):
            yield StringIO(header)
            if body is not None:
                yield body
