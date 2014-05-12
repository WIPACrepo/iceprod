#!/usr/bin/env python
# -*- coding: utf-8 -*-
from distutils.dir_util import remove_tree
from distutils.core import setup, Extension
from distutils import sysconfig
import os
import glob
from collections import OrderedDict

def make_docs(src='resources/docs',dest='share/doc/iceprod/rst/projects/server'):
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

def make_www(src='resources/www',dest='var/www'):
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

def make_templates(src='resources/www_templates',dest='var/www_templates'):
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

data_files = [('etc/iceprod', ['resources/etc/validation.cfg']),
              ('share/iceprod', ['resources/shared/wgetrc']),
             ]
data_files.extend(make_docs())
data_files.extend(make_www())
data_files.extend(make_templates())


dirstoremove = [
    os.path.expandvars('./build'),
    os.path.expandvars(os.path.join(sysconfig.PREFIX,
                       'lib/python2.7/site-packages/iceprod/server')),
    ]
for d in dirstoremove:
    print 'Removing',d
    try:
        remove_tree(d)
    except:
        pass


# This fails to compile on Darwin (maybe others)
if os.uname()[0] == 'Linux':
    procname = Extension('iceprod.procname',
                         sources = ['lib/iceprod/procname.c'],
                         define_macros=[('LINUX', '1'),])
else:
    procname = Extension('iceprod.procname',
                         sources = ['lib/iceprod/procname.c'])
extmod = [procname]


setup(name='IceProd-Server',
      version='trunk',
      description='IceProd Grid Production Software - Server daemons',
      author='Juan Carlos Díaz Vélez',
      author_email='juancarlos@icecube.wisc.edu',
      maintainer='David Schultz',
      maintainer_email='dschultz@icecube.wisc.edu',
      url='http://internal.icecube.wisc.edu/simulation',
      download_url='http://code.icecube.wisc.edu/svn/projects/iceprod-server/trunk',
      packages=['iceprod.server','iceprod.server.plugins','iceprod.server.modules','iceprod.server.tests'],
      package_dir={'iceprod': 'lib/iceprod'},
      data_files=data_files,
      scripts=[ 
           'bin/iceprod',
           'bin/iceprod_server.py',
           'bin/server_tester.py',
           ],
      ext_modules=extmod,
     )

# modify db module documentation to include table list
tables = OrderedDict()
cur_table_name = None
cur_table = OrderedDict()
read_table = False
for line in open('lib/iceprod/server/modules/db.py'):
    if read_table:
        comment = ''
        line = line.strip()
        if '#' in line:
            # there is a comment
            line,comment = line.split('#',1)
        if '{' in line:
            # beginning of all tables
            line = line.split('{',1)[1]
        if '}' in line:
            # end of all tables
            break
        if ':' in line:
            # start of new table
            table_name,line = line.split(':',1)
            cur_table_name = table_name.strip('\'" ')
            cur_table = OrderedDict()
        if '])' in line:
            # end of table
            tables[cur_table_name] = cur_table
        line = line.replace('OrderedDict([','').strip(',() ')
        if ',' in line:
            # got some values
            name,value = line.split(',',1)
            if comment:
                value = value+' -- '+comment
            cur_table[name.strip('\'" ')] = value
    elif '# define tables' not in line:
        continue
    else:
        # we are in the right place
        read_table = True

file = os.path.join(sysconfig.PREFIX,
                   'share/doc/iceprod/rst/projects/server/modules/db.rst')
with open(file,'a') as f:
    f.write('\n\nTables\n------\n\n')
    for t in tables:
        f.write('* %s\n'%t)
        for k in tables[t]:
            f.write('   * %s : %s\n'%(k,tables[t][k]))

try:
    remove_tree(os.path.expandvars('./build'))
except:
    pass
