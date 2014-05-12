#!/usr/bin/env python
# -*- coding: utf-8 -*-
from distutils.core import setup, Extension

setup(name='IceProd-Client',
      version='trunk',
      description='IceProd Grid Production Software - GTK client',
      author='Juan Carlos Díaz Vélez',
      author_email='juancarlos@icecube.wisc.edu',
      url='http://code.icecube.wisc.edu/svn/projects/iceprod-client/trunk',
      packages=['iceprod.client','iceprod.client.gtk'],
      package_dir={'iceprod': 'lib/iceprod'},
      scripts=[
         'bin/iceprodsh', 
         'bin/paramparser.py', 
         'bin/xiceprod',
         ],
      data_files=[],
     )

try:
    remove_tree(os.path.expandvars('./build'))
except:
    pass