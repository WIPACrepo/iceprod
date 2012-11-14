#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup
import glob

setup(name='IceProd',
      version='trunk',
      description='IceProd Grid Production Software',
      author='Juan Carlos Díaz Vélez',
      author_email='juancarlos@icecube.wisc.edu',
      url='http://code.icecube.wisc.edu/svn/meta-projects/iceprod/trunk',
      packages=['iceprod'],
      package_dir={'iceprod': 'lib/iceprod'},
      data_files=[
         ('share/doc/iceprod/rst',
            glob.glob('resources/docs/*'))
         ],
     )

