# -*- coding: utf-8 -*-
"""
  Collection of python modules for scheduling and running Grid IceTray based jobs in 
  a grid environment

  @version: trunk
  @date: mar ago 26 16:16:30 CDT 2008
  @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""
import os
import os.path 
from os.path import join,expandvars
from ConfigParser import SafeConfigParser

__version__ = 'trunk'


def zipfile(fmt= "iceprod-%(version)s"):
	vars = {'version':__version__, 'platform':os.uname()[0],'arch': os.uname()[4]}
	return fmt % vars


def mktar(rootdir,module,outfile,mode='w'):
	curdir = os.getcwd()
	os.chdir(rootdir)
	if mode == 'a':
	   os.system("zip -q -g -r %s.zip %s -i \*.py" % (outfile,module))
	else:
	   os.system("zip -q -r %s.zip %s -i \*.py" % (outfile,module))
	os.chdir(curdir)

apr1 = [65,108,108,32,119,111,114,107,32,97,110,100,32,110,111,32,112,108,
         97,121,32,109,97,107,101,115,32,74,97,99,107,32,97,32,100,117,108,
         108,32,98,111,121,46,32,10,9] 
