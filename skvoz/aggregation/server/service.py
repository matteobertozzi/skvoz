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

from json import dumps as json_dumps

from skvoz.aggregation.server import sources
from skvoz.aggregation.server import engine

from skvoz.util.http import HttpRequestHandler, UnixHttpServer, TcpHttpServer
from skvoz.util.service import AbstractService

class AggregatorRequestHandler(HttpRequestHandler):
    @HttpRequestHandler.match("/query$", commands='POST')
    def tdql_query(self):
        request = dict(self._post_data())
        query = request['query']

        for result in engine.execute_query(self.server.engine, query):
            self.wfile.write(json_dumps(result) + '\n')

def _create_engine(data_dir):
    e = engine.AggregatorEngine()
    e.add_source('file', sources.AggregatorFile())
    if data_dir:
        e.add_source('tsfile', sources.AggregatorTsFile(data_dir))
    return e

class AggregatorUnixServer(UnixHttpServer):
    def __init__(self, address, request_handler, data_dir):
        self.engine = _create_engine(data_dir)
        UnixHttpServer.__init__(self, address, request_handler)

class AggregatorTcpServer(TcpHttpServer):
    def __init__(self, address, request_handler, data_dir):
        self.engine = _create_engine(data_dir)
        TcpHttpServer.__init__(self, address, request_handler)

class AggregationService(AbstractService):
    CLS_REQUEST_HANDLER = AggregatorRequestHandler
    CLS_UNIX_SERVER = AggregatorUnixServer
    CLS_TCP_SERVER = AggregatorTcpServer
