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

from datetime import datetime, timedelta

# Date Grouping Functions
# for date_key, data in group_by_day(tsdata):
#     for timestamp, record in data:
#         ...
class _group_by_date(object):
    # [k for k, g in groupby('AAAABBBCCDAABBB')] --> A B C D A B
    # [list(g) for k, g in groupby('AAAABBBCCD')] --> AAAA BBB CC D
    def __init__(self, tsdata, key, date=None):
        self.datefunc = lambda d: d if date is None else date
        self.keyfunc = key
        self.it = iter(tsdata)
        self.tgtkey = self.currkey = self.currdate = self.currvalue = object()

    def __iter__(self):
        return self

    def next(self):
        while self.currkey == self.tgtkey:
            self._next_key()
        self.tgtkey = self.currkey
        return (self.currkey, self._grouper(self.tgtkey))

    def _grouper(self, tgtkey):
        while self.currkey == tgtkey:
            yield self.currvalue
            self._next_key()

    def _next_key(self):
        self.currvalue = next(self.it)    # Exit on StopIteration
        timestamp = datetime.fromtimestamp(self.currvalue[0])
        self.currkey = self.keyfunc(timestamp)
        self.currdate = self.datefunc(timestamp)

def group_by_minute(tsdata):
    date_key = lambda d: d.strftime('%Y-%m-%d-%H.%M')
    return _group_by_date(tsdata, date_key)

def group_by_hour(tsdata):
    date_key = lambda d: d.strftime('%Y-%m-%d-%H')
    return _group_by_date(tsdata, date_key)

def group_by_day(tsdata):
    date_key = lambda d: d.strftime('%Y-%m-%d')
    return _group_by_date(tsdata, date_key)

def group_by_week(tsdata):
    date_key = lambda d: d.strftime('%Y-%W')
    return _group_by_date(tsdata, date_key)

def group_by_month(tsdata):
    date_key = lambda d: d.strftime('%Y-%m')
    return _group_by_date(tsdata, date_key)

def group_by_year(tsdata):
    date_key = lambda d: d.year
    return _group_by_date(tsdata, date_key)

# Date Filter Functions
def _filter_by_date(tsdata, keep_date_func):
    for ts, data in tsdata:
        if keep_date_func(datetime.fromtimestamp(ts)):
            yield ts, data

def filter_by_timeref(tsdata, tref):
    tref = datetime(tref.year, tref.month, tref.day, tref.hour, tref.minute)
    keep_func = lambda d, tref=tref: d >= tref
    return _filter_by_date(tsdata, keep_func)

def filter_by_last_years(tsdata, n):
    tref = datetime.today().year - n
    keep_func = lambda d, tref=tref: d.year >= tref
    return _filter_by_date(tsdata, keep_func)

def filter_by_last_months(tsdata, n):
    today = datetime.today()
    first_of_month = today - timedelta(days=(today.day - 1))
    tref = first_of_month - timedelta(days=(30 * (n - 1)))
    tref = datetime(tref.year, tref.month, 1)

    keep_func = lambda d, tref=tref: d >= tref
    return _filter_by_date(tsdata, keep_func)

def filter_by_last_weeks(tsdata, n):
    today = datetime.today()
    weekday = today.strftime('%W')
    for d in xrange(7):
        if weekday != (today - timedelta(days=(d + 1))):
            break
    tref = today - timedelta(days=(d + (7 * n)))
    return filter_by_timeref(tsdata, tref)

def filter_by_last_days(tsdata, n):
    tref = datetime.today() - timedelta(days=n)
    return filter_by_timeref(tsdata, tref)

def filter_by_last_hours(tsdata, n):
    tref = datetime.today() - timedelta(hours=n)
    return filter_by_timeref(tsdata, tref)

def filter_by_last_minutes(tsdata, n):
    tref = datetime.today() - timedelta(minutes=n)
    return filter_by_timeref(tsdata, tref)

def filter_by_year(tsdata, yfrom, yto):
    if yfrom is not None and yto is None:
        keep_func = lambda d: d.year >= yfrom
    elif yfrom is None and yto is not None:
        keep_func = lambda d: d.year <= yto
    else:
        assert yfrom is not None and yto is not None
        keep_func = lambda d: yfrom <= d.yfrom <= yto
    return _filter_by_date(tsdata, keep_func)

def filter_by_interval(tsdata, tstart, tend=None):
    if tend is None:
        keep_func = lambda d, tstart=tstart: d >= tstart
    else:
        keep_func = lambda d, tstart=tstart, tend=tend: tstart <= d <= tend
    return _filter_by_date(tsdata, keep_func)
