#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.dir_util import remove_tree
from distutils.core import setup
from distutils import sysconfig
import os
import glob

def make_docs(src='resources/docs',dest='share/doc/iceprod/rst'):
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

dirstoremove = [
    os.path.expandvars('./build'),
    ]
dirstoremove.extend(glob.glob(os.path.expandvars(os.path.join(sysconfig.PREFIX,
                    'share/doc/iceprod/*/*'))))
for d in dirstoremove:
    print 'Removing',d
    try:
        if os.path.isdir(d):
            remove_tree(d)
        else:
            os.remove(d)
    except Exception as e:
        print 'Failed to remove',d,e

setup(name='IceProd',
      version='trunk',
      description='IceProd Grid Production Software',
      author='Juan Carlos Díaz Vélez',
      author_email='juancarlos@icecube.wisc.edu',
      url='http://code.icecube.wisc.edu/svn/meta-projects/iceprod/trunk',
      packages=['iceprod'],
      package_dir={'iceprod': 'lib/iceprod'},
      data_files=make_docs(),
     )

try:
    remove_tree(os.path.expandvars('./build'))
except:
    pass
