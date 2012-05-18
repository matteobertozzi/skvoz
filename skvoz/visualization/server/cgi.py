#!/usr/bin/env python
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

import select
import os

nobody = None
def nobody_uid():
    """Internal routine to get nobody's uid"""
    global nobody
    if nobody:
        return nobody
    try:
        import pwd
    except ImportError:
        return -1
    try:
        nobody = pwd.getpwnam('nobody')[2]
    except KeyError:
        nobody = 1 + max(x[2] for x in pwd.getpwall())
    return nobody

def is_executable(path):
    """Test for executable file."""
    return os.access(path, os.X_OK)

def execute(path, rfile, wfile, args=[], env={}):
    if not hasattr(os, 'fork'):
        return 1

    # Values must be string
    for k, v in env.iteritems(): env[k] = str(v)

    nobody = nobody_uid()
    wfile.flush() # Always flush before forking
    pid = os.fork()
    if pid != 0:
        # Parent
        pid, sts = os.waitpid(pid, 0)
        # throw away additional data [see bug #427345]
        while select.select([rfile], [], [], 0)[0]:
            if not rfile.read(1):
                break
        return sts

    # Child
    try:
        try:
            os.setuid(nobody)
        except os.error:
            pass
        fdnull = open(os.devnull, 'a+')
        os.dup2(rfile.fileno(), 0)
        os.dup2(wfile.fileno(), 1)
        if not __debug__:
            os.dup2(fdnull.fileno(), 2)
        os.execve(path, [os.path.basename(path)] + args, env)
    except:
        os._exit(127)
