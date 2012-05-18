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

from SocketServer import ThreadingTCPServer, ThreadingUnixStreamServer
from BaseHTTPServer import BaseHTTPRequestHandler
from httplib import HTTPConnection
from urlparse import parse_qsl

import socket
import urllib
import cgi
import re
import os

MIME_TABLE = {
    'js':   'application/x-javascript',
    'css':  'text/css',
    'png':  'image/png',
    'jpg':  'image/jpeg',
    'txt':  'text/plain',
    'htm':  'text/html',
    'html': 'text/html',
}

def _url_collapse_path(path):
    path_parts = path.split('/')
    head_parts = []
    for part in path_parts[:-1]:
        if part == '..':
            head_parts.pop()
        elif part and part != '.':
            head_parts.append( part )
    if path_parts:
        tail_part = path_parts.pop()
        if tail_part:
            if tail_part == '..':
                head_parts.pop()
                tail_part = ''
            elif tail_part == '.':
                tail_part = ''
    else:
        tail_part = ''
    return ('/' + '/'.join(head_parts), tail_part)

class HttpMatchRequest(object):
    def __init__(self, uri_pattern, commands):
        if not commands:
            self.commands = None
        else:
            if isinstance(commands, basestring):
                commands = [commands]
            self.commands = set(c.upper() for c in commands)
        self.uri_re = re.compile(uri_pattern)

    def __call__(self, uri, command):
        if self.commands is not None and command not in self.commands:
            return None
        m = self.uri_re.match(uri)
        if m is not None:
            return m.groups()
        return None

class HttpRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.query = []
        self._static_rules = self._fetch_static_rules()
        BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def send_headers(self, code, content_type, headers=None):
        if content_type:
            self.send_response(code)
            self.send_header('Content-Type', content_type)
        else:
            self.send_response(code, "Script output follows")
        if headers is not None:
            for key, value in headers.iteritems():
                self.send_header(key, value)
        self.end_headers()

    def send_file(self, filename):
        if os.path.exists(filename):
            self.send_headers(200, self.guess_mime(filename))
            fd = open(filename)
            try:
                while True:
                    data = fd.read(8192)
                    if not data:
                        break
                    self.wfile.write(data)
            finally:
                fd.close()
        else:
            self.log_message("File %s not found", filename)
            self.handle_not_found()

    def guess_mime(self, filename):
        _, ext = os.path.splitext(filename)
        return MIME_TABLE.get(ext[1:], 'application/octet-stream')

    def __getattr__(self, name):
        if name.startswith('do_'):
            return lambda: self._dispatch(name[3:].upper())
        raise AttributeError(name)

    def _dispatch(self, command):
        directory, name = _url_collapse_path(self.path)
        query_index = name.find('?')
        if query_index >= 0:
            self.query = parse_qsl(name[query_index+1:])
            name = name[:query_index]
        else:
            self.query = []

        self.path = os.path.join(directory, name)
        for match, handle_func in self._rules():
            result = match(self.path, command)
            if result is not None:
                try:
                    handle_func(*result)
                except Exception, e:
                    self.handle_failure(e)
                finally:
                    break
        else:
            self.handle_not_found()

    def handle_failure(self, exception):
        self.send_error(500, "Internal Server Error")

    def handle_not_found(self):
        self.send_error(404, "File not found")

    def _post_data(self, maxlen=None):
        #'application/x-www-form-urlencoded':
        #curl -d "param1=value1&param2=value2" http://localhost:8080/

        # multipart/form-data
        #curl -F "fileupload=@test.txt" http://localhost:8080/

        if self.command.upper() != 'POST':
            return []

        ctype, pdict = cgi.parse_header(self.headers.typeheader or self.headers.type)
        if ctype == 'multipart/form-data':
            data = cgi.parse_multipart(self.rfile, pdict)
        elif ctype == 'application/x-www-form-urlencoded':
            clength = int(self.headers.getheader('content-length', 0))
            if maxlen is not None and clength > maxlen:
                raise ValueError, 'Maximum content length exceeded'
            d = self.rfile.read(clength)
            data = parse_qsl(d)
        else:
            data = []

        return data

    def _load_file(self, filename):
        fd = open(filename)
        try:
            data = fd.read()
        finally:
            fd.close()
        return data

    def _fetch_static_rules(self):
        return [(rule, method) for method in (getattr(self, attr)
                               for attr in dir(self))
                               for rule in getattr(method, "_rules", [])]

    def _rules(self):
        for rule in self._static_rules:
            yield rule

    @classmethod
    def match(cls, uri_pattern, commands=None):
        rule = HttpMatchRequest(uri_pattern, commands)
        def _wrap(method):
            method._rules = getattr(method, '_rules', []) + [rule]
            return method
        return _wrap

class TcpHttpServer(ThreadingTCPServer):
    allow_reuse_address = True

class UnixHttpServer(ThreadingUnixStreamServer):
    allow_reuse_address = True

class UnixHTTPConnection(HTTPConnection):
    """
    Represents one transaction with an HTTP server over unix socket.
    """
    def __init__(self, path):
        HTTPConnection.__init__(self, 'localhost')
        self.path = path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.path)
        self.sock = sock

def http_open(address, path, data=None):
    """
    Wrapper around HTTPConnection and UnixHTTPConnection that allows to
    to make a get or post http request based on data.

        address = '/var/lib/hsrv.sock'
        address = ('kernel.org', 8080)
        data = {'a': 10, 'b': 20}  # used for POST requests

        http, response = http_open(address, '/index.html', data)
        try:
            response.getheader('content-type')
            response.status
            response.read()
        finally:
            http.close()
    """
    if isinstance(address, basestring):
        http = UnixHTTPConnection(address)
    else:
        http = HTTPConnection(*address)

    if data:
        headers = {'Content-type': 'application/x-www-form-urlencoded'}
        http.request('POST', path, urllib.urlencode(data), headers)
    else:
        http.request('GET', path)
    return http, http.getresponse()

def http_readlines(address, path, data=None, chunk_size=8192):
    """
    Helper to read line by line an Http Response:
        for line in http_readlines(('localhost', 8080), '/index.html'):
            ...
    """
    http, response = http_open(address, path, data)
    try:
        data = response.read(chunk_size)
        while data:
            index = 0
            while True:
                cindex = data.find('\n', index)
                if cindex < 0:
                    data = data[index:]
                    break
                yield data[index:cindex]
                index = cindex + 1
            data += response.read(chunk_size)
    finally:
        http.close()
