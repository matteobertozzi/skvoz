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

from Queue import Queue

from skvoz.collection.server.cache import TimedFdCache
from skvoz.collection.server.sink import CollectSinks
from skvoz.util.debug import debug_time
from skvoz.util import tsfile

import logging
import socket

class CollectQueue(object):
    LOG = logging.getLogger('collector-queue')

    WAIT_TIMEOUT = 1

    def __init__(self, data_dir, sink_conf):
        self.data_dir = data_dir
        self.running = False

        self.tfcache = TimedFdCache(self.WAIT_TIMEOUT)
        self.sinks = CollectSinks(sink_conf)
        self.queue = Queue()

    def run(self):
        self.running = True
        while self.running:
            self._process_data(self.WAIT_TIMEOUT)

        while not self.queue.empty():
            self._process_data(1)

        self.tfcache.close()

    def stop(self):
        self.running = False
        print 'Queue Killed', self.queue.qsize()

    def put(self, data, data_dir=None):
        self.queue.put(data)

    def _process_data(self, timeout):
        try:
             key, timestamp, value = self.queue.get(True, timeout)
        except:
            self.tfcache.flush()
        else:
            self._store_data(key, timestamp, value)
            self._sink_store_data(key, timestamp, value)

    @debug_time
    def _store_data(self, key, timestamp, value):
        try:
            fd = self.tfcache.open(key, tsfile.Writer, self.data_dir)
            fd.write('%s %s\n' % (timestamp, value))
        except Exception, e:
            self.LOG.warn('WAL failure: %s' % e)

    @debug_time
    def _sink_store_data(self, key, timestamp, value):
        # Entry point to handle data in different way
        data = '%s %s %s\n' % (timestamp, key, value)
        for sink in self.sinks:
            if not sink.match(key):
                continue

            try:
                if sink.is_socket:
                    self.tfcache.open_socket(sink.address).sendall(data)
                else:
                    self.tfcache.open_file(sink.address, 'ab').write(data)
            except socket.error, e:
                self.LOG.warn("Sink connection failure '%s': %s" % (sink.name, e))
            except Exception, e:
                self.LOG.warn("Delivery failure on sink '%s': %s" % (sink.name, e))
