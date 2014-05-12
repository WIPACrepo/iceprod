#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.dir_util import remove_tree
from distutils.core import setup
from distutils import sysconfig
import os
import glob

def make_docs(src='resources/docs',dest='share/doc/iceprod/rst/projects/core'):
    final_dirs = {'':[]}
    for r,d,f in os.walk(src):
        for dd in reversed(d):
            if dd[0] == '.':
                d.remove(dd)
        for ff in f:
            if ff[0] == '.':
                continue # skip files that are private
            fullpath = os.path.abspath(r)
            extrapath = fullpath[len(os.path.abspath(src)):]
            if extrapath and extrapath[0] == '/':
                extrapath = extrapath[1:]
            if extrapath not in final_dirs:
                final_dirs[extrapath] = []
            final_dirs[extrapath].append(os.path.join(r,ff))
    return zip([os.path.join(dest,x) for x in final_dirs.keys()],final_dirs.values())

data_files = [('share/iceprod', ['resources/shared/histo.php',
                                 'resources/shared/histo.css',
                                 'resources/shared/iceprod.v3.dtd',
                                 'resources/shared/i3paramdb.dtd']),
              ('etc/grid-security/certificates',glob.glob('resources/certificates/*'))
             ]
data_files.extend(make_docs())

dirstoremove = [
    os.path.expandvars('./build'),
    os.path.expandvars(os.path.join(sysconfig.PREFIX,
                       'lib/python2.7/site-packages/iceprod/core')),
    ]
for d in dirstoremove:
    print 'Removing',d
    try:
        remove_tree(d)
    except:
        pass

setup(name='IceProd-Core',
      version='trunk',
      description='IceProd Grid Production Software - Core libs',
      author='Juan Carlos Díaz Vélez',
      author_email='juancarlos@icecube.wisc.edu',
      url='http://code.icecube.wisc.edu/svn/projects/iceprod-core/trunk',
      packages=['iceprod.core','iceprod.core.tests'],
      package_dir={'iceprod':'lib/iceprod'},
      data_files=data_files,
      scripts=[
          'bin/loader.sh',
          'bin/i3exec.py',
          'bin/core_tester',
          'bin/core_tester.py',
          'bin/coverage_tester'
          ],
     )

try:
    remove_tree(os.path.expandvars('./build'))
except:
    pass
