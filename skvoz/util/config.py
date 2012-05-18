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
from json import loads as json_loads
from getpass import getuser
from hashlib import sha1

import os

def _conf_directories():
    yield os.getcwd()
    yield '/home/%s' % getuser()
    yield '/Users/%s' % getuser()
    yield '/etc/skvoz'

class _JSonConfig(object):
    CHECKSUM_CLASS = sha1
    ROOT_TYPE = None

    def load(self, path):
        fd = open(path)
        try:
            data = fd.read()
            cksum = self.CHECKSUM_CLASS(data).digest()

            self.data = json_loads(data)
            if self.ROOT_TYPE is not None and not isinstance(self.data, self.ROOT_TYPE):
                raise Exception("Invalid json data expected '%s' received '%s'" %
                                (self.ROOT_TYPE.__name__, self.data.__class__.__name__))
        finally:
            fd.close()
        return cksum

    def save(self, path):
        fd = open(path, 'w')
        try:
            fd.write(json_dumps(self.data))
        finally:
            fd.close()

    @classmethod
    def fetch(cls, name, checksum=False):
        path = cls.filepath(name)
        config = cls()
        if path is not None:
            cksum = config.load(path)
        else:
            cksum = None
        if checksum:
            return config, cksum
        return config

    @classmethod
    def checksum(cls, name, blksize=65536):
        path = cls.filepath(name)
        if path is None:
            return None

        fd = open(path)
        h = cls.CHECKSUM_CLASS()
        try:
            data = fd.read(blksize)
            while data:
                h.update(data)
                data = fd.read(blksize)
        finally:
            fd.close()
        return h.digest()

    @staticmethod
    def filepath(name):
        if os.path.exists(name):
            return name

        name = os.basename(name) if os.sep in name else name
        for confdir in _conf_directories():
            path = os.path.join(confdir, name)
            if os.path.exists(path):
                return path

        return None


class Config(_JSonConfig):
    ROOT_TYPE = dict

    def __init__(self):
        self.data = {}

    def __getitem__(self, key):
        self.data[key]

    def get(self, key, default=None):
        conf, key = self._walk(key, False)
        return conf.get(key, default)

    def get_address(self, key, default=None):
        address = self.get(key)
        if address is None:
            return default

        if address['type'] == 'tcp':
            address = (address.get('host', ''), address['port'])
        elif address['type'] == 'unix':
            address = address['path']
        else:
            raise Exception("Invalid Address Type '%s'" % address['type'])
        return address

    def put(self, key, value):
        conf, key = self._walk(key, True)
        conf[key] = value

    def _walk(self, key, do_add):
        parts = key.split('/')
        conf = self.data
        for part in parts[:-1]:
            if not part:
                continue
            if part in conf:
                conf[part] = {}
            conf = conf[part]
        return conf, parts[-1]

class ListConfig(_JSonConfig):
    ROOT_TYPE = list

    def __init__(self):
        self.data = []

    def __iter__(self):
        return iter(self.data)

    def get(self, index):
        return self.data[index]

    def add(self, value):
        self.data.append(value)

    def replace(self, index, value):
        self.data[index] = value
