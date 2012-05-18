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

from SocketServer import ThreadingUnixStreamServer, ThreadingTCPServer
from SocketServer import StreamRequestHandler

from skvoz.util.debug import debug_time, request_session_time
from skvoz.collection.server.queue import CollectQueue
from skvoz.util.service import AbstractService
from skvoz.util.dateutil import timestamp

import threading
import logging

class CollectRequestHandler(StreamRequestHandler):
    LOG = logging.getLogger('collector-service')

    @request_session_time
    def handle(self):
        try:
            cq = self.server.queue
            while cq.running:
                request = self.rfile.readline()
                if not request:
                    break

                self.handle_request(cq, request.strip())
        except Exception, e:
            self.LOG.warn('handle() failure %s' % e)

    @debug_time
    def handle_request(self, queue, request):
        try:
            key, ts, value = request.split(' ', 2)
            if ts == '-': ts = timestamp()
            queue.put((key, ts, value))
        except Exception, e:
            self.LOG.warn('handle_request() failure: %s' % e)
            self.LOG.warn('request: %s' % request)

class CollectorUnixServer(ThreadingUnixStreamServer):
    def __init__(self, address, handler, cqueue):
        self.queue = cqueue
        ThreadingUnixStreamServer.__init__(self, address, handler)

class CollectorTcpServer(ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, address, handler, cqueue):
        self.queue = cqueue
        ThreadingTCPServer.__init__(self, address, handler)

class CollectorService(AbstractService):
    CLS_REQUEST_HANDLER = CollectRequestHandler
    CLS_UNIX_SERVER = CollectorUnixServer
    CLS_TCP_SERVER = CollectorTcpServer

    def run(self, address, data_dir, sink_conf):
        collect_queue = CollectQueue(data_dir, sink_conf)
        super(CollectorService, self).run(address, collect_queue)

    def _starting(self, collect_queue):
        threading.Thread(target=collect_queue.run).start()

    def _stopping(self, collect_queue):
        collect_queue.stop()
