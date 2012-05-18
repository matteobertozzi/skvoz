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

from skvoz.util.config import ListConfig

import logging
import re

class CollectSink(object):
    """
    Describe a collect sink object and the way to communicate with it.
    """
    def __init__(self, name, key, channel, address):
        self.key = key
        self.rkey = re.compile(key)
        self.name = name
        self.channel = channel
        self.address = address
        self.is_socket = channel in ('tcp', 'unix')

    def match(self, key):
        return self.rkey.match(key)

    @classmethod
    def load(cls, data):
        # {'name': 'test', 'channel': 'tcp', 'address': '127.0.0.1:8080', 'key': '[a-z]+'}
        name = data['name']
        key = data.get('key', '')
        channel = data['channel']
        address = data['address']
        if channel == 'tcp':
            host, port = address.split(':')
            address = (host, int(port))
        elif not channel in ('file', 'unix'):
            raise Exception("Sink has invalid channel '%s'" % channel)

        return cls(name, key, channel, address)

class CollectSinks(object):
    """
    Sinks loader. Everytime you ask for sinks reloads
    the conf file to pick up the new added.
    """
    LOG = logging.getLogger('collector-sinks')

    RELOAD_TIMEOUT = 30

    def __init__(self, sink_conf):
        self.sink_conf = sink_conf
        self._cksum = None
        self._sinks = []
        self._atime = 0

    def __iter__(self):
        return iter(self._reload_conf())

    def _reload_conf(self):
        if not self.sink_conf or (time() - self._atime) <= self.RELOAD_TIMEOUT:
            return self._sinks

        try:
            cksum = ListConfig.checksum(self.sink_conf)
            if cksum == self._cksum:
                return self._sinks

            conf, cksum = ListConfig.fetch(self.sink_conf, True)
            sinks = [CollectSink.load(sink) for sink in conf]
        except Exception, e:
            self.LOG.warn('Failed to reload sinks conf: %s' % e)
        else:
            self._sinks = sinks
            self._cksum = cksum

        return self._sinks
