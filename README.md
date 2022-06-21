# Monitor

A Python file monitor that converts raw data files to mzML format and adds them to Stasis, our processing tracking system.

#### Supported raw formats
We use [ProteoWizard](http://proteowizard.sourceforge.net) for the conversion, and use only a subset of ProteoWizard's [supported formats](http://proteowizard.sourceforge.net/formats/index.html)

- Agilent .d
- Thermo .raw
- Waters .raw
- Open formats
    - .mzML
    

## Installation

Install from source by cloning our repository [source code](https://bitbucket.com/fiehnlab/monitor.git)

    $ git clone git://bitbucket.com/fiehnlab/monitor.git

and running:

    $ cd monitor
    $ python setup.py install

Python versions 3.5 and newer are supported.

Tests can be run using

    $ python setup.py test

## Usage

Please note this tool depends on ProteoWizard being installed on the same host.

Configure ProteoWizard's installation folder in appconfig.yml
 
### Command-line tool

This application can be run as follows:

    $ python bin/monitorLaunch.py [options]

Before running, please ensure you update the config file to your needs. 

Please use 

    $ python bin/monitorLaunch.py -h

to get a list of the available options.

To terminate the application press: *`Ctrl+C`*

NOTE: It is suggested to use a 'service wrapper' (like [nssm][1]) to run this app. 

## Docker

The docker folder contains a Dockerfile to build an image that can run the application.
Currently pip has issues cloning some git dependencies needed for the proper running of the monitor.

## Credits

This module was written by Diego Pedrosa.

[1]: <https://nssm.cc/>
