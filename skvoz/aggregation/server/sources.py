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

from glob import glob

from skvoz.util.dateutil import date_to_timestamp
from skvoz.util import tsfile

import os

class AggregatorSource(object):
    def read_files(self, files):
        raise NotImplementedError

    def files_from_keys(self, keys):
        raise NotImplementedError

    def filter_files_by_time(self, files, start_time, end_time):
        return files

class AggregatorFile(AggregatorSource):
    def read_files(self, files):
        return tsfile.read_files(files)

    def files_from_keys(self, keys):
        for key, files in keys.iteritems():
            files = sum([glob(f) for f in files], [])
            yield key, tuple(f for f in files if os.path.exists(f))

class AggregatorTsFile(AggregatorSource):
    def __init__(self, data_dir):
        self.data_dir = data_dir

    def read_files(self, files):
        return tsfile.read_files(files, self.data_dir)

    def files_from_keys(self, keys):
        for key, tskeys in keys.iteritems():
            files = []
            for tskpattern in tskeys:
                for tskey in tsfile.find_keys(self.data_dir, tskpattern):
                    files.extend(tsfile.find_files(self.data_dir, tskey))
            yield key, files

    def filter_files_by_time(self, files, start_time, end_time):
        return tsfile.filter_files_by_time(files, date_to_timestamp(start_time), date_to_timestamp(end_time))

