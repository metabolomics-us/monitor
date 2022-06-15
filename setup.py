#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='Monitor',
      version='1.0',
      description='File monitoring module to convert raw data files',
      author='Diego Pedrosa',
      author_email='dpedrosa@ucdavis.edu',
      url='https://bitbucket.org/fiehnlab/monitor',
      license='GPLv3',
      packages=find_packages(exclude=("tests",)),
      data_files=[('config', ['appconfig.yml'])],
      install_requires=[
          'requests',
          'simplejson',
          'yamlconf',
          'watchdog',
          'boto3',
          'botocore',
          'loguru',
          'git+git://github.com/metabolomics-us/carpy.git@master#egg=stasis-client&subdirectory=stasis-client'

      ],
      include_package_data=True,
      zip_safe=False,
      classifiers=[
          'Programming Language :: Python :: 3.6',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Intended Audience :: Science/Research',
      ]
      )
