#!/usr/bin/env python

import os
import sys

# python version check to fail hard
major_ver,minor_ver = sys.version_info[:2] 
if (major_ver < 2 or (major_ver == 2 and minor_ver < 7) or
    (major_ver == 3 and minor_ver < 2)):
    raise Exception('Python is too old. IceProd requires 2.7+ or 3.2+')

try:
    # Use setuptools if available, for install_requires (among other things).
    import setuptools
    from setuptools import setup
except ImportError:
    setuptools = None
    from distutils.core import setup

kwargs = {}

version = "2.0.dev1"

with open('README.rst') as f:
    kwargs['long_description'] = f.read()

if setuptools is not None:
    # If setuptools is not available, you're on your own for dependencies.
    install_requires = ['pycurl', 'tornado>=2.4', 'jsonrpclib', 'lxml',
                        'setproctitle']
    extras_require = {
        'docs': ['sphinx'],
        'tests': ['coverage', 'flexmock']
    }
    #if sys.version_info < (3, 2):
    #    install_requires.append('backports.ssl_match_hostname')
    kwargs['install_requires'] = install_requires
    kwargs['extras_require'] = extras_require

setup(
    name='iceprod',
    version=version,
    packages = ['iceprod', 'iceprod.client', 'iceprod.core', 'iceprod.server'],
    package_data = {
        # data files need to be listed both here (which determines what gets
        # installed) and in MANIFEST.in (which determines what gets included
        # in the sdist tarball)
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