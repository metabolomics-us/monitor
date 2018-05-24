#!/usr/bin/env python

from setuptools import setup

setup(name='Monitor',
      version='0.1',
      description='File monitoring module to convert raw data files',
      author='Diego Pedrosa',
      author_email='dpedrosa@ucdavis.edu',
      url='https://bitbucket.org/fiehnlab/monitor',
      packages=['monitor', 'rest'],
      data_files=[('config', ['appconfig.yaml'])],
      install_requires=[
        'requests>=2.18.4',
        'simplejson>=3.15.0',
        'yamlconf>=0.2.3',
        'watchdog>=0.8.3',
      ],
      )
