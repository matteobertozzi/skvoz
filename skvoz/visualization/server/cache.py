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

from time import time
import zlib

class CacheStore(object):
    MIN_COMPRESSION_SIZE = 128
    MAX_CACHE_SIZE = 64 << 20
    NULL_ITEM = (None, None)
    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(CacheStore, cls).__new__(cls)
            cls.data = {}
            cls.size = 0
        return cls.__instance

    def get(self, key):
        timestamp, value = self.data.get(key, self.NULL_ITEM)
        if time() > timestamp:
            self.flush()
            raise KeyError

        if isinstance(value, basestring):
            return zlib.decompress(value)
        return value

    def add(self, key, value, expire):
        if isinstance(value, basestring):
            value = zlib.compress(value)
            self.size += len(value)
        self.flush()
        self.data[key] = (time() + expire, value)

    def flush(self):
        if self.size <= self.MAX_CACHE_SIZE:
            return

        sdata = self._sort_by_timestamp()
        removed = 0

        now = time()
        for key, (timestamp, value) in sdata:
            if now >= timestamp:
                self.size -= len(value)
                del self.data[key]
                removed += 1
            else:
                break

        if self.size > self.MAX_CACHE_SIZE:
            for i in xrange(removed, len(sdata)):
                if self.size > self.MAX_CACHE_SIZE:
                    del self.data[sdata[i][0]]
                    removed += 1
                else:
                    break

        return removed

    def clear(self):
        self.data.clear()
        self.size = 0

    def _sort_by_timestamp(self):
        return sorted(self.data.iteritems(), key=lambda x: x[1][0])

    @classmethod
    def cache(cls, expire=5):
        def _fcache(func):
            def _fwrap(*args, **kwargs):
                ckey = (func, args, tuple(kwargs.iteritems()))
                cache = cls()
                try:
                    value = cache.get(ckey)
                except KeyError:
                    print ckey, 'not in cache'
                    value = func(*args, **kwargs)
                    cache.add(ckey, value, expire)
                return value
            return _fwrap
        return _fcache
