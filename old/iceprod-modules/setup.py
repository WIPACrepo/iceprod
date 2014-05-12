#!/usr/bin/env python
# -*- coding: utf-8 -*-
from distutils.dir_util import remove_tree
from distutils.core import setup
from distutils import sysconfig
import os
import glob

dirstoremove = [
    os.path.expandvars('./build'),
    os.path.expandvars(os.path.join(sysconfig.PREFIX,
                       'lib/python2.7/site-packages/iceprod/modules')),
    ]
for d in dirstoremove:
    print 'Removing',d
    try:
        remove_tree(d)
    except:
        pass

setup(name='IceProd-Modules',
      version='trunk',
      description='IceProd Grid Production Software - Modules ',
      author='Juan Carlos Díaz Vélez',
      author_email='juancarlos@icecube.wisc.edu',
      url='http://code.icecube.wisc.edu/svn/projects/iceprod-modules/trunk',
      packages=['iceprod.modules'],
      package_dir={'iceprod': 'lib/iceprod'},
      data_files=[
         ('share/doc/iceprod/rst/projects/modules',
            glob.glob('resources/docs/*'))
         ],
     )

try:
    remove_tree(os.path.expandvars('./build'))
except:
    pass
