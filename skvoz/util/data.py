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

from itertools import izip

import re

def string_to_type(sdata):
    if not isinstance(sdata, basestring):
        return sdata

    for t in (int, float):
        try:
            return t(sdata)
        except ValueError:
            pass

    ldata = sdata.lower()
    if ldata == 'true': return True
    if ldata == 'false': return False

    return sdata

class DataSplitter(object):
    """
    Split ['a', 'b', 'c'] On [':', ',']
    Split ['a', 'b'] On ['\w+]
    """
    def __init__(self, varnames, delimiters=None):
        self.varnames = varnames
        self.delimiters = delimiters

        if delimiters is None:
            self._rxsplit = None
        elif len(delimiters) == 1:
            self._rxsplit = re.compile(delimiters[0])
        else:
            pattern = '|'.join(map(re.escape, delimiters))
            self._rxsplit = re.compile(pattern)

    def __call__(self, data):
        varnames = self.varnames
        rxsplit = self._rxsplit

        if rxsplit is None:
            splits = data.split(' ', len(varnames) - 1)
        else:
            splits = rxsplit.split(data, len(varnames) - 1)

        if len(splits) != len(varnames):
            raise Exception("Number of splits %r don't match with vars %r" % (splits, varnames))

        results = {}
        for key, value in izip(varnames, splits):
            results[key] = string_to_type(value)
        return results

class DataTable(object):
    def __init__(self):
        self.columns = []
        self.rows = []

    def addColumn(self, name):
        if len(self.rows) > 0:
            raise Exception("You've already added rows!")
        self.columns.append(name)

    def addColumns(self, *args):
        for column in args:
            self.addColumn(column)

    def addRow(self, row):
        if isinstance(row, dict):
            if len(self.columns) == 0:
                self.columns.extend(sorted(row.keys()))

            irow = [None] * len(self.columns)
            for col, value in row.iteritems():
                index = self.columns.index(col)
                if index >= 0:
                    irow[index] = value
            row = irow
        self.rows.append(row)

    def addRows(self, rows):
        for row in rows:
            self.addRow(row)

    def clearRows(self):
        self.rows = []

    def columnRows(self, column):
        if isinstance(column, basestring):
            column = self.columns.index(column)

        for row in self.rows:
            yield row[column]

    def show(self):
        lencols = [max(len(str(row)) for row in self.columnRows(i))
                                     for i in xrange(len(self.columns))]
        lencols = [max(clen, len(c)) for clen, c in zip(lencols, self.columns)]

        hfrmt = ' {0:^{w}} '
        rfrmt = ' {0:<{w}} '

        separator = '+%s+' % '+'.join(['-' * (width + 2) for width in lencols])

        print separator
        print '|%s|' % '|'.join([hfrmt.format(c, w=w) for c, w in zip(self.columns, lencols)])
        print separator
        for row in self.rows:
            print '|%s|' % '|'.join([rfrmt.format(c, w=w) for c, w in zip(row, lencols)])
        print separator
