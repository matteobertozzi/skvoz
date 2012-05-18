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

import sys

try:
    from argparse import ArgumentParser
except ImportError:
    from optparse import OptionParser

    class ArgumentGroup(object):
        def __init__(self, group):
            self.group = group

        def add_argument(self, *args, **kwargs):
            required = kwargs.pop('required', False)
            o = self.group.add_option(*args, **kwargs)
            o.required = required
            return o

    class ArgumentParser(OptionParser):
        def add_argument(self, *args, **kwargs):
            required = kwargs.pop('required', False)
            o = self.add_option(*args, **kwargs)
            o.required = required
            return o

        def add_argument_group(self, *args, **kwargs):
            return ArgumentGroup(self.add_option_group(*args, **kwargs))

        def parse_args(self):
            try:
                required = [o for o in self.option_list if getattr(o, 'required', False)]
                for g in self.option_groups:
                    required.extend([o for o in g.option_list if getattr(o, 'required', False)])

                options, args = OptionParser.parse_args(self)
                for ro in required:
                    if getattr(options, ro.dest, None) is None:
                        print >> sys.stderr, "Required option '%s' %s" % (ro, ro.help)
                        sys.exit(1)
            except Exception, e:
                print >> sys.stderr, e
            return options

def to_address(arg):
    r = arg.split(':')
    if len(r) == 2:
        host, port = r
        try:
            return host, int(port)
        except ValueError:
            pass
    return arg
