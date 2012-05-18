#!/usr/bin/env python

from distutils.core import setup

setup(name = 'skvoz',
      version = '0.1',
      license = 'New BSD License',
      author = 'Matteo Bertozzi',
      author_email = 'theo.bertozzi@gmail.com',
      url = 'http://github.com/matteobertozzi/skvoz',
      description = 'Lightweight data collector, aggregator, visualizator',

      packages = [
        'skvoz',
        'skvoz.collection',
        'skvoz.collection.server',
        'skvoz.collection.client',
        'skvoz.visualization',
        'skvoz.visualization.server',
        'skvoz.aggregation',
        'skvoz.aggregation.server',
        'skvoz.aggregation.tdql',
        'skvoz.aggregation.util',
        'skvoz.util',
      ],

      include_package_data = True,
      package_data = {'skvoz': ['resources/*']},

      scripts = [
        'bin/skvoz',
        'bin/skvoz-aggregator',
        'bin/skvoz-collector',
        'bin/skvoz-shell',
        'bin/skvoz-visualizator',
      ],
)

