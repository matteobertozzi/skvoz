#!/usr/bin/env python

from skvoz.collection.client.uploader import StatsUploader, StatCounter, StatEvent
from random import randint
from time import time, sleep

import subprocess
import logging
import sys
import re

def call(cmd, stdin=None, **kwargs):
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    (stdout, stderr) = process.communicate(stdin)
    return (process.returncode, stdout, stderr)

def _counter_test(uploader):
    odd_counter = StatCounter('odd', uploader)
    even_counter = StatCounter('even', uploader)

    for i in xrange(120):
        if randint(0, 8192) % 2 == 0:
            even_counter.inc()
        else:
            odd_counter.inc()
        sleep(1)

def _ping_test(uploader):
    rx = re.compile('time=([0-9.]+)')

    def _ping_event(ts, host, hstat):
        _, res, _ = call(['ping', '-c', '1', host])
        r = rx.search(res)
        if r is not None:
            ping_time = r.groups()[0]
            hstat.pushts(ts, ping_time)

    HOSTS = ('google.com', 'kernel.org', 'github.com')
    events = [StatEvent('ping-' + host, uploader) for host in HOSTS]
    ts = int(time()) * 1000
    for _ in xrange(240):
        for host, hstat in zip(HOSTS, events):
            _ping_event(ts, host, hstat)
        ts += 1000

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "usage: test <collector service>"
        sys.exit(1)

    logging.basicConfig()
    uploader = StatsUploader([sys.argv[1]])
    #_counter_test(uploader)
    _ping_test(uploader)
    uploader.flush()


