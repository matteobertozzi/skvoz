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

from collections import defaultdict
from heapq import merge

from skvoz.aggregation.util import timestamps
from skvoz.aggregation import tdql
from skvoz.util.data import DataSplitter

def _filter_bypass(tsdata):
    return tsdata

def _group_bypass(tsdata):
    for ts, data in tsdata:
        yield ts, ((ts, data),)

class AggregationContext(object):
    def __init__(self):
        self.data_split = None
        self.time_period = None
        self.group_period = None
        self.data_filters = []
        self.functions = {}
        self._cfunctions = 0

    def functions_reset(self):
        self._cfunctions = 0
        for _, func in self.functions.iteritems():
            func.reset()

    def functions_apply(self, items):
        self._cfunctions += 1
        for func in self.functions.itervalues():
            func.apply(items)

    def functions_results(self):
        if self._cfunctions == 0:
            return None

        results = {}
        for key, func in self.functions.iteritems():
            results[key] = func.result()
        return results

    def filter_row(self, items):
        for func in self.data_filters:
            if func(items):
                return True
        return False

    def results_keys(self):
        if len(self.functions) > 0:
            return self.functions.keys()
        if self.data_split is not None:
            return self.data_split.varnames
        return None

class AggregatorEngine(object):
    """
    for key, data in engine.fetch(context, source, keys):
        for ts, value in data:
            ...
    """
    def __init__(self):
        self.sources = {}

    def add_source(self, name, source):
        self.sources[name] = source

    def fetch(self, context, source_name, keys, group_by_key=False):
        source = self.sources.get(source_name)
        if source is None:
            raise Exception("Invalid Source '%s'!" % source_name)

        if group_by_key:
            return self._fetch_grouped(context, source, keys)
        return self._fetch_ungrouped(context, source, keys)

    def _fetch_grouped(self, context, source, keys):
        data = []
        groups = []
        for group, files in source.files_from_keys(keys):
            groups.append(group)
            data.append(self.fetch_files(context, source, files, group))

        values = []
        last_ts = None
        for ts, v, key in merge(*data):
            if ts != last_ts and last_ts is not None:
                yield last_ts, values
                values = []
            values.append((key, v)) # TODO: CHANGE MERGE POLICY
            last_ts = ts
        if len(values) > 0:
            yield last_ts, values

    def _fetch_ungrouped(self, context, source, keys):
        files = sum([list(ks) for _, ks in source.files_from_keys(keys)], [])
        return self.fetch_files(context, source, files)

    def fetch_files(self, context, source, files, group=None):
        # Initialize Time Period Filter
        if context.time_period:
            files = source.filter_files_by_time(files, *context.time_period)
            ff = lambda ts, p=context.time_period: timestamps.filter_by_interval(ts, *p)
        else:
            ff = _filter_bypass

        # Initialize Group Period Filter
        gf = context.group_period or _group_bypass

        # Read Data with Filters
        if group is None:
            for ts, values in gf(ff(source.read_files(files))):
                for v in self._fetch_values(context, values):
                    yield ts, v
        else:
            for ts, values in gf(ff(source.read_files(files))):
                for v in self._fetch_values(context, values):
                    yield ts, v, group

    def _fetch_values(self, context, values):
        # TODO:
        #   - Add Grouping on split
        #   - OPTIMIZE: Move filter_row() in source
        if context.data_split:
            if context.functions:
                context.functions_reset()
                for _, v in values:
                    items = context.data_split(v)
                    if context.filter_row(items): continue
                    context.functions_apply(items)
                results = context.functions_results()
                if results is not None: yield results
            else:
                results = defaultdict(list)
                for _, v in values:
                    items = context.data_split(v)
                    if context.filter_row(items): continue
                    for ik, iv in items.iteritems():
                        results[ik].append(iv)
                if len(results) > 0: yield dict(results)
        else:
            yield [v for _, v in values]

# From {files: ['a', 'b', 'c']}
# Time Intervals [datetime.datetime(2012, 1, 1, 0, 0)]
# Group By ['key', 'months']
# Split ['a', 'b', 'c'] On [':']
# Store {'average': Func avg(a), 'total': Func sum(b)}
def execute_query(engine, query):
    context, source, keys, group_by_key = parse_query(query)
    return engine.fetch(context, source, keys, group_by_key)

def parse_query(query):
    # Parse the user query
    query = tdql.parse(query)

    # Create a new context for Aggregation Engine
    context = AggregationContext()

    # Data Split Options
    if query.stmt_split is not None:
        context.data_split = DataSplitter(query.stmt_split.results, query.stmt_split.delimiters)

    # Data Filter
    if query.stmt_where is not None:
        context.data_filters.append(query.stmt_where.evaluator())

    # Data Store Options
    if query.stmt_store is not None:
        for key, func in query.stmt_store.functions():
            context.functions[key] = func

    # Extract filtering functions
    if query.stmt_time is not None:
        if query.stmt_time.start is not None:
            context.time_period = (query.stmt_time.start, query.stmt_time.end)

    # Extract grouping functions
    group_by_key = False
    if query.stmt_group is not None:
        group_by_key = query.stmt_group.key
        if query.stmt_group.time_period is not None:
            func_name = 'group_by_' + query.stmt_group.time_period
            func = getattr(timestamps, func_name)
            if func is None:
                raise Exception("Invalid grouping function '%s'!" % func_name)
            context.group_period = func

    return context, query.stmt_from.source, query.stmt_from.keys, group_by_key
