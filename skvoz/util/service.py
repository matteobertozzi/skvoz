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

import signal
import grp
import pwd
import sys
import os

class AbstractService(object):
    CLS_REQUEST_HANDLER = None
    CLS_UNIX_SERVER = None
    CLS_TCP_SERVER = None

    def __init__(self):
        self.server = None

        if not __debug__:
            self._setup_signals()

    def set_user(self, user):
        if isinstance(user, basestring):
            _, _, user, gid, _, _, _ = pwd.getpwnam(user)

        if user is not None:
            self._drop_group_privileges()
            os.setuid(user)

    def set_group(self, group):
        if isinstance(group, basestring):
            _, _, group, _ = grp.getgrnam(group)

        if group is not None:
            self._drop_group_privileges()
            os.setgid(group)

    def set_umask(self, umask):
        os.umask(umask)

    def run(self, address, *args, **kwargs):
        if self.CLS_REQUEST_HANDLER is None:
            raise Exception("Missing Request Handler Class for '%s'!" % self.__class__.__name__)

        if isinstance(address, basestring):
            if self.CLS_UNIX_SERVER is None:
                raise Exception("Missing Unix Server Class for '%s'!" % self.__class__.__name__)

            self.server = self.CLS_UNIX_SERVER(address, self.CLS_REQUEST_HANDLER, *args, **kwargs)
        else:
            if self.CLS_TCP_SERVER is None:
                raise Exception("Missing TCP Server Class for '%s'!" % self.__class__.__name__)

            self.server = self.CLS_TCP_SERVER(address, self.CLS_REQUEST_HANDLER, *args, **kwargs)

        self.log_message("%s starting on %s\n" % (self.__class__.__name__, address))
        try:
            self._starting(*args, **kwargs)
            self.server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.log_message("%s %s is stopping\n" % (self.__class__.__name__, address))

            self._stopping(*args, **kwargs)
            self.server.shutdown()
            if isinstance(address, basestring) and os.path.exists(address):
                os.unlink(address)

    def shutdown(self, *args, **kwargs):
        if self.server is None:
            return

        self.server.shutdown()

    def reload(self):
        pass

    def _starting(self, *args):
        pass

    def _stopping(self, *args):
        pass

    def _sighup(self, signo, frame):
        self.reload()

    def _sigint(self, signo, frame):
        self.shutdown()

    def _sigterm(self, signo, frame):
        self.shutdown()

    def _setup_signals(self):
        signal.signal(signal.SIGHUP, self._sighup)
        signal.signal(signal.SIGINT, self._sigint)
        signal.signal(signal.SIGTERM, self._sigterm)

    def _drop_group_privileges(self):
        try:
            os.setgroups([])
        except OSError, exc:
            print 'Failed to remove group privileges: %s' % exc

    def log_message(self, message):
        sys.stderr.write(message)
        sys.stderr.flush()
