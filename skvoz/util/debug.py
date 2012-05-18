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

from skvoz.util.dateutil import human_elapsed_time

from time import time

DEBUG_TIME_WARN_THRESHOLD = 0.05

def request_session_time(func, *args, **kwargs):
    def _wrap(self, *args, **kwargs):
        st = time()
        try:
            func(self, *args, **kwargs)
        finally:
            et = time()
            print '[SESSION] %s elapsed in %s' % (self.client_address or id(self), human_elapsed_time(et - st))
    return _wrap

def debug_time(func, *args, **kwargs):
    if __debug__:
        def _wrap(*args, **kwargs):
            st = time()
            try:
                func(*args, **kwargs)
            finally:
                total = time() - st
                if total >= DEBUG_TIME_WARN_THRESHOLD:
                    print '[DEBUG-TIME] %s(): %s' % (func.__name__, human_elapsed_time(total))
        return _wrap
    return func

