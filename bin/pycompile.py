#! /usr/bin/env python
#

import sys,os
import shutil
import getopt
from os.path import basename
from os.path import join
from distutils.util import byte_compile

def usage(arguments):
	"""
	print usage/help info

  	@param arguments: cmdline args passed to program
	"""
	print "Usage: %s [option] source" % arguments[0]
	print "   "
	print "   where option is one of"
	print "   "
	print "   -h,"
	print "   --help                : This screen"
	print "   "
	print "   -o <destination>   : target directory"
	print "   "
	print "   -O 				 : optimize"
	print "   "
	print "   -v 			     : verbose"

if __name__ == '__main__':


	opts,args = getopt.getopt(
					sys.argv[1:],
					'ho:vO',
					["help","output=","verbose","optimize"])

	source = args[0]     
	target = None
	optimize = 0
	verbose  = 0

	for o, a in opts:
		if o in ("-o","--output"):
			target   = a
		if o in ("-O","--optimize"):
			optimize = 1
		if o in ("-v","--verbose"):
			verbose  = 1
		if o in ("-h","--help"):
			usage(sys.argv)


	byte_compile([source], optimize=optimize, force=0, verbose=verbose, dry_run=0)

	if target: 
		#copy python code to library directory
		shutil.copy( source,target) 

		#copy python bytecode to library directory
		if optimize: 
			source = source.replace('.py','.pyo')
		else: 
			source = source.replace('.py','.pyc')

		target = join(target,basename(source))
		shutil.copy( source,target) 
