#!/usr/bin/env python

DEFAULT_COLLECTOR_PORT = 50595
DEFAULT_AGGREGATOR_PORT = 50596
DEFAULT_VISUALIZATOR_PORT = 50597

def resource_path():
    from os.path import dirname, join
    return join(dirname(__file__), 'resources')
