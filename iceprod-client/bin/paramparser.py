#! /usr/bin/env python
#

"""
 A class for parsing an IceTray XML configuration and building a Python
 IceTrayConfig object.

 copyright  (c) 2005 the icecube collaboration

 @version: $Revision:  $
 @date: $Date: 2005/04/06 17:32:56 $
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>

"""

import sys,os
import string,re
import getpass
import getopt
import types
import signal
from os.path import expandvars
from iceprod.core.dataclasses import *
from iceprod.core.xmlparamdb import *

sys_argv = sys.argv[0:] # copy argv befre inspectv2 modifies it

from iceprod.core.inspectv2 import *
from os.path import exists,join
import logging
logging.basicConfig()

def sighandler(signum, frame):
	print >> sys.stderr, sys.argv[0],"aborted by user"
	os._exit(1)

def usage(arguments):
	print "Usage: %s [option] " % arguments[0]
	print "   "
	print "   where option is one of"
	print "   "
	print "   -h,--help                 : This screen"
	print "   "
	print "   -v,--version=<version>    : (1 or 2) version of icetray-inspect "
	print "   "
	print "   -r,--url=<URL>            : Specify soaptray URL "
	print "   "
	print "   -u,--username=<username>  : Specify database username"
	print "   "
	print "   -x,--xml                  : print xml tree instead of uploading"
	print "   "
	print "   -i,--input                : read iceprod cache  "
	print "   "
	print "   -o,--output               : write iceprod cache  "
	print "   "
	print "   --metaname                : override metaproject name "
	print "   "
	print "   --metaversion             : override metaproject version"



def main(arguments):
	""" 
	Main method
	@param arguments: array of commandline arguments
	"""

	username  = getpass.getuser()
	xmlflag   = False
	maxiter   = 1
	password  = ""
	url       = 'https://condor.icecube.wisc.edu:9080'
	path      = expandvars("$I3_SRC")
	libdir    = os.path.join(expandvars("$I3_BUILD"),"lib")
	version   = 3
	infile    = None
	metaname  = None
	metaversion = None
	cache     = expandvars("$I3_BUILD/inspect_cache.xml")

	opts,args = getopt.getopt(
					arguments[1:], 
					'i:r:u:l:s:v:f:o:hx',
					["output=","metaversion=", 
                    "metaname=","url=","version=",
                    "xml","help", "input=",
                    "username=","libdir=",
                    "sourcedir="])

	for o, a in opts:
		if o in ("-u", "--username"):
			username = a
		if o in ("-r","--url"):
			url = a
		if o in ("-x","--xml"):
			xmlflag = True
		if o in ("-o","--output"):
			xmlflag = True
			cache   = a
		if o in ("-h", "--help"):
			usage(arguments)
			sys.exit()
		if o in ("-v", "--version"):
				version = int(a)
		if o in ("-i", "--input"):
				infile = a
		if o in ("--metaname",):
				metaname = a
		if o in ("--metaversion",):
				metaversion= a
		if o in ("-l", "--libdir"):
				if a.startswith('/'):
				    libdir = a
				else:
				    libdir = os.path.join(os.getcwd(),a)
		if o in ("-s", "--sourcedir"):
				if a.startswith('/'):
				    path = a
				else:
				    path = os.path.join(os.getcwd(),a)


	if version == 3: 
	    i3inspect = IceTrayInspectV3(path,libdir)
	elif version == 2: 
	    i3inspect = IceTrayInspectV2(path,libdir)
	else:
	    i3inspect = IceTrayInspect(path,libdir)
	if infile:
	    i3inspect.readXML(infile)
	else:
	    projects = i3inspect.InspectProjects(expandvars('$I3_SRC'))
	    projectlist = projects.keys()
	    i3inspect.InspectIceProdModules()

	# override parsed name/version
	if metaname:
	   i3inspect.GetMetaproject().SetName(metaname)
	if metaversion:
	   i3inspect.GetMetaproject().SetVersion(metaversion)

	if xmlflag:
		print >> sys.stderr, "writting database to %s." % cache
		i3inspect.toXML(cache)
		print >> sys.stderr, "done."
		sys.exit( 0 )
	else:
		password = getpass.getpass('Password for \'%s\': ' % username)
		print >> sys.stderr, "\nuploading parameter info to database"
		if i3inspect.upload(url,username,password):
		  print >> sys.stderr, "done."
		else:
		  print >> sys.stderr, "failed."
		sys.exit( 0 )


if __name__ == '__main__':
  signal.signal(signal.SIGQUIT, sighandler)
  signal.signal(signal.SIGINT, sighandler)
  main(sys_argv)

