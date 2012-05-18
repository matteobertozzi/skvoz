#!/usr/bin/env python
#
# Copyright (c) 2012, Matteo Bertozzi
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of the <organization> nor the
#     names of its contributors may be used to endorse or promote products
#     derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from base64 import urlsafe_b64encode as name_encode
from base64 import urlsafe_b64decode as name_decode
from tempfile import mkstemp
from gzip import GzipFile
from bz2 import BZ2File
from heapq import merge
from uuid import uuid1

from skvoz.util.dateutil import msec_to_timestamp

import threading
import os
import re

# /key/latest        <- Currently in progress
# /key/uid.cts       <- Currently in consolidation
# /key/uid.build     <- Currently in consolidation writing
# /key/sts-dts-uid   <- Archived file
RX_CONSOLIDATED = re.compile('^([0-9]+\\.[0-9]+\\.[a-z0-9]+)$')
RX_NAME = re.compile('^(latest)$|^([a-z0-9]+).cts$|^([0-9]+\\.[0-9]+\\.[a-z0-9]+)$')

SORT_FILE_PREFIX = 'ts_sort_'

def _read_raw_fd(fd):
    fd.seek(0)
    line = fd.readline()
    while line:
        yield line.strip().split(' ', 1)
        line = fd.readline()

def _read_raw_file(path):
    for file_cls in (GzipFile, BZ2File, open):
        fd = file_cls(path)
        try:
            for line in fd:
                yield line.strip().split(' ', 1)
            break
        except IOError:
            pass
        finally:
            fd.close()

def _read_file(path):
    for file_cls in (GzipFile, BZ2File, open):
        fd = file_cls(path)
        try:
            for line in fd:
                ts, data = line.strip().split(' ', 1)
                yield msec_to_timestamp(int(ts)), data
            break
        except IOError:
            pass
        finally:
            fd.close()

def _slice_tsfile(iterable, threshold):
    while True:
        chunk = []
        chunk_add = chunk.append
        try:
            size = 0
            while size < threshold:
                tsline = tuple(next(iterable))
                chunk_add(tsline)
                timestamp, data = tsline
                size += len(timestamp) + len(data) + 1
        except StopIteration:
            break
        finally:
            yield chunk

def sort(path, threshold, tmpdir=None):
    tmpfiles = []

    # Split and Sort
    if os.stat(path).st_size > threshold:
        tslines = _read_raw_file(path)
        for chunk in _slice_tsfile(tslines, threshold):
            chunk.sort()

            # Write temp file
            fd, fdpath = mkstemp(prefix=SORT_FILE_PREFIX, dir=tmpdir)
            fd = os.fdopen(fd, 'w+t')
            for line in chunk:
                fd.write('%s %s\n' % line)
            tmpfiles.append((fd, fdpath))

        # Merge Temp Files
        for ts, data in merge(*tuple(_read_raw_fd(fd) for fd, _ in tmpfiles)):
            yield msec_to_timestamp(int(ts)), data

        for fd, fd_path in tmpfiles:
            fd.close()
            os.unlink(fd_path)
    else:
        tslines = list(_read_raw_file(path))
        tslines.sort()
        for ts, data in tslines:
            yield msec_to_timestamp(int(ts)), data

def _consolidate(path, uid):
    THRESHOLD = 24 << 20

    dirpath, _ = os.path.split(os.path.abspath(path))
    cpath = os.path.join(dirpath, '%s.build' % uid)
    fd = GzipFile(cpath, 'wb')
    try:
        min_timestamp = None
        max_timestamp = None
        for timestamp, data in sort(path, THRESHOLD, dirpath):
            if min_timestamp is None:
                min_timestamp = timestamp
            max_timestamp = timestamp
            fd.write('%s %s\n' % (timestamp, data))
    except:
        fd.close()
        try:
            os.unlink(cpath)
        except:
            # TODO: USE LOG
            print 'tsfile.consolidate(): Failed to remove %s' % cpath
    else:
        fd.close()
        min_timestamp = int(min_timestamp)
        max_timestamp = int(max_timestamp)
        try:
            cname = '%d.%d.%s' % (min_timestamp, max_timestamp - min_timestamp, uid)
            os.rename(cpath, os.path.join(dirpath, cname))
            os.unlink(path)
        except:
            # TODO: USE LOG
            print 'tsfile.consolidate(): Failed to rename %s' % cpath

def consolidate(path):
    dirpath, name = os.path.split(os.path.abspath(path))
    assert name == 'latest', name

    uid = uuid1().hex
    tsc_path = os.path.join(dirpath, '%s.tsc' % uid)
    os.rename(path, tsc_path)

    t = threading.Thread(target=_consolidate, args=(tsc_path, uid))
    t.start()
    return t

def is_consolidated(name):
    return RX_CONSOLIDATED.match(name) is not None

def read_file(path, consolidated=False):
    """
    Read a file line by line returning the timestamp and the rest of the line:
        for ts, data in read_files(path):
            ...
    """
    if consolidated:
        return _read_file(path)

    data = list(_read_file(path))
    data.sort()
    return data

def read_files(files, data_path=None):
    """
    Read all specified files and sort them by timestamp,
    returning the timestamp and the rest of the line:
        for ts, data in read_files((path0, path1, ...)):
            ...
    """
    readers = []
    for f in files:
        if isinstance(f, tuple):
            path, consolidated = f
            if data_path is not None:
                path = os.path.join(data_path, path)
            readers.append(read_file(path, consolidated))
        else:
            readers.append(read_file(f))

    for ts, data in merge(*readers):
        yield ts, data

def read(data_path, key):
    return read_files(data_path, find_files(data_path, key))

def find_files(data_path, key):
    """
    Returns all the files that match the specified key.
        for name, consolidated in find_files(dirpath, key):
            ...
    """
    for name in os.listdir(os.path.join(data_path, key)):
        r = RX_NAME.match(name)
        if r is not None:
            yield os.path.join(key, name), r.groups()[2] is not None

def filter_files_by_time(files, start_time, end_time):
    for name, consolidated in files:
        if consolidated:
            st, dt, uid = name.split('.')
            st = int(st)
            dt = int(dt)
            et = st + dt
            if start_time > et or end_time < st:
                continue
        yield name, consolidated

def find_keys(data_path, kpattern):
    """
    Returns all the keys that matches the specified pattern
    """
    rx = re.compile(kpattern)
    for ekey in os.listdir(data_path):
        try:
            key = name_decode(ekey)
        except TypeError:
            # Not TS File...
            key = ekey

        if rx.match(key) is not None:
            yield ekey

class Writer(object):
    DEFAULT_NAME = 'latest'
    THRESHOLD = 16 << 20
    OPEN_MODE = 'a'

    def __init__(self, key, path, name=None):
        key_path = os.path.join(path, name_encode(key))
        if not os.path.exists(key_path): os.makedirs(key_path)
        if name is None: name = self.DEFAULT_NAME
        self.fd = open(os.path.join(key_path, name), self.OPEN_MODE)

    def write(self, data):
        self.fd.write(data)

        if self.fd.tell() > self.THRESHOLD:
            path = self.fd.name
            self.fd.flush()
            self.fd.close()
            consolidate(path)
            self.fd = open(path, self.OPEN_MODE)

    def close(self):
        self.fd.flush()

        do_consolidate = (self.fd.tell() > self.THRESHOLD)

        path = self.fd.name
        self.fd.close()
        self.fd = None

        if do_consolidate:
            consolidate(path)

if __name__ == '__main__':
    from time import time
    import sys

    if len(sys.argv) < 2:
        print 'usage: consolidate <filename>'
    else:
        st = time()
        t = consolidate(sys.argv[1])
        t.join()
        et = time()
        print '[T] Consolidated in %.3fsec' % (et - st)
