# -*- coding: utf-8 -*-
"""
 Interface for configuring pre/post icetray scripts

 copyright  (c) 2005 the icecube collaboration

 @version: $Revision: $
 @date: Wed Aug 27 18:47:09 CDT 2008
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""
import ipmodule
import fileutils
import inspect
from modulefinder import *

__version__ = '0.1.1'

plugins = {}

plugins['ipmodule.Hello'] = ipmodule.Hello
plugins['ipmodule.GoodBye'] = ipmodule.GoodBye
plugins['fileutils.RenameFile']  = fileutils.RenameFile
plugins['fileutils.RemoveFiles']  = fileutils.RemoveFiles
plugins['fileutils.RenameMultipleFiles'] = fileutils.RenameMultipleFiles

# Aliases for backwards compatibility
plugins['RenameFile']  = fileutils.RenameFile
plugins['RemoveFiles']  = fileutils.RemoveFiles
plugins['RenameMultipleFiles'] = fileutils.RenameMultipleFiles

sep = '.'

def dirname(path):
	return sep.join(path.split(sep)[:-1])
	
def basename(path):
	return path.split(sep)[-1]

def get_plugin(pname):
	if plugins.has_key(pname):
		return plugins[pname]
	else:
	    modname   = dirname(pname)
	    classname = basename(pname)
	    mod   = __import__(modname, globals(),locals(),[classname])
	    plugins[pname] = getattr(mod,classname)
	    return plugins[pname]

def has_plugin(pname):
	return plugins.has_key(pname)

def configured_plugins():
	return plugins.keys()

def ipinspect(pname,pretty=False):
    mod = __import__(pname, globals(),locals(),['*'])
    classlist = {}
    if pretty:
	   print '%s' % mod.__name__
    for i in dir(mod):
       c = getattr(mod,i)
       if inspect.isclass(c) and issubclass(c,ipmodule.IPBaseClass):
	      classlist["%s.%s"%(pname,i)] = c().ShowParameters()
	      if pretty: 
	         print '\t  %s' % c.__name__
	         for p in c().ShowParameters():
	           print "\t     %s (%s)" % (p[0],mtype(p[1]))
	           print "\t        Description: %s" % p[2]
	           print "\t        Default    : %s" % p[1]
    return classlist

def mtype(var):
    if isinstance(var,(str,unicode)): return 'string'
    elif isinstance(var,int): return 'int'
    elif isinstance(var,float): return 'float'
    elif isinstance(var,bool): return 'bool'
    elif isinstance(var,list): return 'list'
    elif isinstance(var,dict): return 'list'
    else: return 'NaT'


