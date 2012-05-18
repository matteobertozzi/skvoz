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

import logging
import socket

def sock_address(address):
    r = address.split(':')
    if len(r) == 2:
        host, port = r
        try:
            return host, int(port)
        except ValueError:
            pass
    return address

def sock_address_type(address):
    address = sock_address(address)
    if isinstance(address, basestring):
        return socket.AF_UNIX, address
    assert address is not None, address
    return socket.AF_INET, tuple(address)

def sock_connect(addresses, timeout=None):
    for sock_type, address in addresses:
        try:
            sock = socket.socket(sock_type, socket.SOCK_STREAM)
            if timeout is not None: sock.settimeout(timeout)
            sock.connect(address)
        except: pass
        else: return sock
    return None

class StatsUploader(object):
    LOG = logging.getLogger('stats-uploader')

    AGGREGATE_THRESHOLD = 10
    AGGREGATE_TIMEOUT = 10    # TODO
    AGGREGATE_MAX = 100
    TIMEOUT = 5

    def __init__(self, addresses):
        if addresses is None:
            self.addresses = []
        else:
            self.addresses = [sock_address_type(addr) for addr in addresses]

        self.atime = None
        self.sock = None
        self.data = []

    def push(self, key, value, aggregate=True):
        self.pushts(int(time() * 1000), key, value, aggregate)

    def pushts(self, msec_timestamp, key, value, aggregate=True):
        if ' ' in key:
            raise Exception("Key '%s' cannot contain spaces" % key)

        self.data.append('%s %d %s\n' % (key, msec_timestamp, value))
        if not aggregate or len(self.data) > self.AGGREGATE_THRESHOLD:
            self.flush()

        # sorry I've to throw away something... (TODO: LOG)
        if len(self.data) > self.AGGREGATE_MAX:
            self.LOG.warn('throwing away stats current buffer %d > %d!' % (len(self.data), self.AGGREGATE_MAX))
            self.data.pop(0)

    def flush(self):
        if self.sock is None:
            self.sock = sock_connect(self.addresses, self.TIMEOUT)
            if self.sock is None:
                return
        try:
            self.sock.sendall(''.join(self.data))
        except:
            self.sock = None
        else:
            self.data = []

class StatEvent(object):
    def __init__(self, key, stat_uploader):
        self.key = key
        self.uploader = stat_uploader

    def flush(self):
        self.uploader.flush()

    def push(self, value, aggregate=True):
        self.uploader.push(self.key, value, aggregate)

    def pushts(self, timestamp, value, aggregate=True):
        self.uploader.pushts(timestamp, self.key, value, aggregate)

class StatCounter(StatEvent):
    def __init__(self, key, stat_uploader):
        super(StatCounter, self).__init__(key, stat_uploader)
        self.value = 0

    def set(self, value=0, aggregate=True):
        self.value = value
        self.push(self.value)

    def inc(self, aggregate=True):
        self.add(1, aggregate)

    def dec(self, aggregate=True):
        self.add(-1, aggregate)

    def add(self, value, aggregate=True):
        self.value += value
        self.push(self.value, aggregate)

    def sub(self, value, aggregate=True):
        self.add(-value, aggregate)
