"""
  Collection of python modules for scheduling and running Grid IceTray based jobs in 
  a grid environment

  @version: $Id$
  @date: $Date: $
  @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>

  U{Userguide and other documentation <http://www.icecube.wisc.edu/~juancarlos/software/sim-prod>}

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
