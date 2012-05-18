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

from skvoz.util.network import sock_connect

class TimedFdCache(object):
    def __init__(self, timeout):
        self.timeout = timeout
        self.flush_time = 0
        self.fds = {}

    def open_file(self, filename, mode):
        return self.open(filename, open, mode)

    def open_socket(self, address):
        return self.open(address, sock_connect)

    def open(self, key, open_func, *args, **kwargs):
        finfo = self.fds.get(key)
        if finfo is None:
            fd = open_func(key, *args, **kwargs)
        else:
            fd = finfo[0]
        self.fds[key] = (fd, time())

        if (time() - self.flush_time) > self.timeout:
            self.flush()

        return fd

    def close(self):
        for fd, _ in self.fds.itervalues():
            if hasattr(fd, 'flush'):
                fd.flush()
            fd.close()
        self.fds.clear()

    def flush(self):
        for key, (fd, last_update) in self.fds.items():
            if (time() - last_update) > self.timeout:
                del self.fds[key]
                if hasattr(fd, 'flush'):
                    fd.flush()
                fd.close()
        self.flush_time = time()
