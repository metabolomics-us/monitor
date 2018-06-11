#!/usr/bin/env python

from setuptools import setup

setup(name='Monitor',
      version='0.1',
      description='File monitoring module to convert raw data files',
      author='Diego Pedrosa',
      author_email='dpedrosa@ucdavis.edu',
      url='https://bitbucket.org/fiehnlab/monitor',
      license='GPLv3',
      packages=['monitor', 'rest'],
      data_files=[('config', ['appconfig.yml'])],
      install_requires=[
        'requests>=2.18.4',
        'simplejson>=3.15.0',
        'yamlconf>=0.2.3',
        'watchdog>=0.8.3',
          'boto3',
          'botocore'
      ],
      include_package_data=True,
      zip_safe=False,
      classifiers=[
          'Programming Language :: Python :: 3.6',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Intended Audience :: Science/Research',
      ]
      )

