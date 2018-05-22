#!/usr/bin/env python

from distutils.core import setup

setup(name='Monitor',
      version='0.1',
      description='File monitoring module to convert raw data files',
      author='Diego Pedrosa',
      author_email='dpedrosa@ucdavis.edu',
      url='https://bitbucket.org/fiehnlab/monitor',
      packages=['monitor', 'rest'],
      data_files=[('config', ['appconfig.yaml'])],
      )
