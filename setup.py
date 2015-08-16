#!/usr/bin/env python

import os
import sys
import glob

if sys.version_info < (2, 7) or (3, 0) <= sys.version_info < (3, 2):
    print('ERROR: IceProd requires at least Python 2.7 or 3.2 to run.')
    sys.exit(1)

try:
    # Use setuptools if available, for install_requires (among other things).
    import setuptools
    from setuptools import setup
except ImportError:
    setuptools = None
    from distutils.core import setup

kwargs = {}

version = "2.0.dev1"

current_path = os.path.dirname(os.path.realpath(__file__))

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
    install_requires = ['certifi','tornado>=3.0', 'pyzmq']
    extras_require = {
        'utils': ['setproctitle', 'pycurl', 'pyOpenSSL', 'pyasn1', 'jsonschema'],
        'docs': ['sphinx'],
        'tests': ['coverage', 'flexmock']
    }
    if sys.version_info < (3, 2):
        install_requires.append('futures')
    #    install_requires.append('backports.ssl_match_hostname')
    kwargs['install_requires'] = install_requires
    kwargs['extras_require'] = extras_require
    kwargs['zip_safe'] = False

setup(
    name='iceprod',
    version=version,
    scripts=glob.glob('bin/*'),
    packages=['iceprod', 'iceprod.client', 'iceprod.core',
              'iceprod.modules', 'iceprod.server',
              'iceprod.server.modules', 'iceprod.server.plugins'],
    package_data={
        # data files need to be listed both here (which determines what gets
        # installed) and in MANIFEST.in (which determines what gets included
        # in the sdist tarball)
        'iceprod.server':['data/etc/*','data/www/*','data/www_templates/*'],
        },
    author="IceCube Collaboration",
    author_email="simprod@icecube.wisc.edu", # TODO: better email address
    url="http://icecube.wisc.edu", # TODO: better url
    #license="http://www.apache.org/licenses/LICENSE-2.0", # TODO: licensing
    description="IceProd is a set of grid middleware and job tracking tools, developed for the IceCube Collaboration.",
    classifiers=[
#        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
#        'Programming Language :: Python :: Implementation :: PyPy',
        ],
    **kwargs
)