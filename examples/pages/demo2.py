#!/usr/bin/env python

from datetime import datetime

if __name__ == '__main__':
    print '<html>'
    print '<head><title>Demo 2</title></head>'
    print '<body>'
    print '<h1>Hello Demo2!</h2>'
    print '<p>This page is generated by examples/pages/demo2.py at %s</p>' % datetime.now()
    print '</body>'
    print '</html>'