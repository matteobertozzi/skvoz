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

from skvoz.aggregation.tdql.tokenizer import TOKEN_NUMBER, TOKEN_STRING
from skvoz.aggregation.tdql.rpn import rpn_evaluate

class _Function(object):
    def __init__(self):
        self.reset()

    def __call__(self, items):
        self.apply(items)
        return self.result()

    def reset(self):
        raise NotImplementedError

    def result(self):
        raise NotImplementedError

    def apply(self, items):
        raise NotImplementedError

    @staticmethod
    def parse_number(value):
        if not isinstance(value, basestring):
            return value
        value = value.strip()
        try:
            value = int(value)
        except ValueError:
            value = float(value)
        return value

class Executor(_Function):
    def __init__(self, functions, rpn):
        self.functions = dict((k, f()) for k, f in functions.iteritems())
        self.fresult = None
        self.rpn = rpn

    def reset(self):
        for f in self.functions.itervalues():
            f.reset()

    def result(self):
        if len(self.fresult) == 1:
            return self.fresult[0][1]
        return [r[1] for r in self.fresult]

    def apply(self, items):
        self.fresult = rpn_evaluate(self.rpn, dict(self.functions, **items))

class MinFunction(_Function):
    def reset(self):
        self.value = None

    def result(self):
        return TOKEN_NUMBER, self.value

    def apply(self, value):
        value = self.parse_number(value)
        self.value = value if self.value is None else min(self.value, value)

class MaxFunction(_Function):
    def reset(self):
        self.value = None

    def result(self):
        return TOKEN_NUMBER, self.value

    def apply(self, value):
        value = self.parse_number(value)
        self.value = value if self.value is None else max(self.value, value)

class SumFunction(_Function):
    def reset(self):
        self.total = 0

    def result(self):
        return TOKEN_NUMBER, self.total

    def apply(self, value):
        self.total += self.parse_number(value)

class SubFunction(_Function):
    def reset(self):
        self.total = 0

    def result(self):
        return TOKEN_NUMBER, self.total

    def apply(self, value):
        self.total -= self.parse_number(value)

class AvgFunction(_Function):
    def reset(self):
        self.total = 0
        self.count = 0

    def result(self):
        return TOKEN_NUMBER, self.total / self.count

    def apply(self, value):
        self.total += self.parse_number(value)
        self.count += 1

class CountFunction(_Function):
    def reset(self):
        self.count = 0

    def result(self):
        return TOKEN_NUMBER, self.count

    def apply(self, value):
        self.count += 1

class ListFunction(_Function):
    def reset(self):
        self.data = []

    def result(self):
        return TOKEN_STRING, '[%s]' % ', '.join(self.data)

    def apply(self, value):
        self.data.append(value)

class SetFunction(_Function):
    def reset(self):
        self.data = set()

    def result(self):
        return TOKEN_STRING, '[%s]' % ', '.join(self.data)

    def apply(self, value):
        self.data.add(value)
