#!/usr/bin/env python

import os
import sys
import glob

if sys.version_info < (3, 6):
    print('ERROR: IceProd requires at least Python 3.6+ to run.')
    sys.exit(1)

try:
    # Use setuptools if available, for install_requires (among other things).
    import setuptools
    from setuptools import setup
except ImportError:
    setuptools = None
    from distutils.core import setup

kwargs = {}

current_path = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(current_path,'iceprod','__init__.py')) as f:
    for line in f.readlines():
        if '__version__' in line:
            kwargs['version'] = line.split('=')[-1].split('\'')[1]
            break
    else:
        raise Exception('cannot find __version__')

with open(os.path.join(current_path,'README.rst')) as f:
    kwargs['long_description'] = f.read()

try:
    # make dataclasses.js from dataclasses.py
    import inspect
    import json
    sys.path.append(current_path)
    from iceprod.core import dataclasses
    dcs = {}
    names = dataclasses._plurals.copy()
    for name, obj in inspect.getmembers(dataclasses,inspect.isclass):
        if name[0] != '_' and dict in inspect.getmro(obj):
            dcs[name] = obj().output()
            names[name] = obj.plural
    data = {'classes':dcs,'names':names}
    with open(os.path.join(current_path,'iceprod','server','data','www','dataclasses.js'),'w') as f:
        f.write('var dataclasses='+json.dumps(data,separators=(',',':'))+';')
except Exception:
    print('WARN: cannot make dataclasses.js')

if setuptools is not None:
    # If setuptools is not available, you're on your own for dependencies.
    install_requires = ['certifi','tornado>=5.1', 'setproctitle',
                        'pyOpenSSL', 'pyasn1', 'jsonschema', 'pymysql',
                        'psutil>=5.0.0', 'cryptography', 'requests',
                        'requests_toolbelt', 'requests-futures', 'statsd',
                        'cachetools>=2.0.0', 'sphinx>=1.4',
                        'coverage', 'flexmock', 'requests-mock','boto3',
                        'pymongo','PyJWT','motor','ldap3',
                       ]
    kwargs['install_requires'] = install_requires
    kwargs['zip_safe'] = False

setup(
    name='iceprod',
    scripts=glob.glob('bin/*'),
    packages=['iceprod', 'iceprod.core', 'iceprod.modules',
              'iceprod.server', 'iceprod.server.rest', 'iceprod.server.scheduled_tasks',
              'iceprod.server.modules', 'iceprod.server.plugins'],
    package_data={
        # data files need to be listed both here (which determines what gets
        # installed) and in MANIFEST.in (which determines what gets included
        # in the sdist tarball)
        'iceprod.server':['data/etc/*','data/www/*','data/www_templates/*'],
        },
    author="IceCube Collaboration",
    author_email="simprod@icecube.wisc.edu",
    url="https://github.com/WIPACrepo/iceprod",
    license="https://github.com/WIPACrepo/iceprod/blob/master/LICENSE",
    description="IceProd is a set of grid middleware and job tracking tools, developed for the IceCube Collaboration.",
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        'Operating System :: POSIX :: Linux',
        'Topic :: System :: Distributed Computing',

        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        ],
    **kwargs
)
