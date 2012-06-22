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

from skvoz.visualization.server import charts
from skvoz.visualization.server import cgi

from skvoz.util.http import HttpRequestHandler, UnixHttpServer, TcpHttpServer
from skvoz.util.service import AbstractService
from skvoz.util.http import http_readlines
from skvoz.util.data import DataTable

from json import loads as json_loads
from base64 import b64encode

import skvoz

import os
import re

def fetch_data_table(address, query):
    table = DataTable()
    for line in http_readlines(address, '/query', {'query': b64encode(query)}):
        data = json_loads(line)

        if not isinstance(data, dict):
            _data = {}
            keys, gdata = data
            if '__ts__' in keys:
                _data['__ts__'] = keys.pop('__ts__')
            keys = ''.join(keys)
            for kdata in gdata:
                for k, v in kdata.iteritems():
                    _data['%s%s' % (keys, k)] = v
            data = _data

        data.pop('__key__', None)
        print data
        table.addRow(data)
    return table

def _load_and_replace_vars(path, query_vars):
    fd = file(path)
    try:
        data = fd.read()
        for key, value in query_vars:
            data = data.replace('${%s}' % key, value)
        return data
    finally:
        fd.close()

def chart_from_config(path, query_vars):
    conf = json_loads(_load_and_replace_vars(path, query_vars))

    table = fetch_data_table(conf['aggregator'], conf['query'])

    chart_renderer = conf.get('renderer')
    if chart_renderer == 'google':
        chart_renderer = charts.GoogleChart
    else:
        chart_renderer = charts.HighChart

    chart_type = conf.get('type')
    if chart_type == 'column':
        chart_type = chart_renderer.CHART_TYPE_COLUMN
    else:
        chart_type = chart_renderer.CHART_TYPE_LINE

    chart = chart_renderer.fromDataTable(conf['name'], chart_type, table, xaxis='__ts__')
    chart.setTitle(conf.get('title'))
    chart.setSubTitle(conf.get('subtitle'))

    return '{"name": "%s", "type": "%s", "chart": %s}' % (chart.name, chart_renderer.NAME, chart.toData())

class VisualizatorRequestHandler(HttpRequestHandler):
    RAW_FILE_EXT = ('htm', 'html', 'css' 'js', 'txt', 'png', 'jpg')
    INDEX_EXT = ('htm', 'html', 'py', 'txt')

    @HttpRequestHandler.match('/skvoz/chart/(.+)')
    def graph(self, name):
        path = os.path.join(self.server.graphs_dir, name)
        if os.path.exists(path):
            json_chart = chart_from_config(path, self.query)
            self.send_headers(200, 'text/plain')
            self.wfile.write(json_chart)
        else:
            self.handle_not_found()

    @HttpRequestHandler.match('/(.*)')
    def page(self, name):
        path = self._find_path(name)
        if path is None:
            self.handle_not_found()
        else:
            self._load_page(path)

    def _find_path(self, name):
        path = os.path.join(self.server.pages_dir, name)
        if os.path.exists(path):
            rpath = self._find_file(path, True)
            if rpath is not None: return rpath

        path = self._find_file(path, False)
        if path is None:
            path = os.path.join(skvoz.resource_path(), name)
            path = self._find_file(path, os.path.exists(path))
        return path

    def _find_file(self, path, exists):
        if os.path.isdir(path):
            # Search for 'index*' file
            names = []
            nprio = len(self.INDEX_EXT)
            rx = re.compile('^index(\\..+|)$')
            for f in os.listdir(path):
                if not rx.match(f):
                    continue

                _, ext = os.path.splitext(f)
                try:
                    prio = self.INDEX_EXT.index(ext[1:])
                except ValueError:
                    prio = nprio
                names.append((prio, f))

            if len(names) == 0:
                return None

            names.sort()
            return os.path.join(path, names[0][1])

        if not exists:
            for ext in self.INDEX_EXT:
                f = '%s.%s' % (path, ext)
                if os.path.exists(f):
                    return f
            return None

        return path

    def _load_page(self, path):
        _, ext = os.path.splitext(path)
        if ext[1:] in self.RAW_FILE_EXT or not cgi.is_executable(path):
            self.send_file(path)
        else:
            self.send_headers(200, None)
            try:
                env = dict(self._post_data() + self.query)
                cgi.execute(path, self.rfile, self.wfile, env=env)
            except Exception, e:
                self.log_message("CGI '%s' Execution failed: %s", (path, e))


class VisualizatorUnixServer(UnixHttpServer):
    def __init__(self, address, request_handler, pages_dir, graphs_dir):
        self.graphs_dir = graphs_dir
        self.pages_dir = pages_dir
        UnixHttpServer.__init__(self, address, request_handler)

class VisualizatorTcpServer(TcpHttpServer):
    def __init__(self, address, request_handler, pages_dir, graphs_dir):
        self.graphs_dir = graphs_dir
        self.pages_dir = pages_dir
        TcpHttpServer.__init__(self, address, request_handler)

class VisualizationService(AbstractService):
    CLS_REQUEST_HANDLER = VisualizatorRequestHandler
    CLS_UNIX_SERVER = VisualizatorUnixServer
    CLS_TCP_SERVER = VisualizatorTcpServer
