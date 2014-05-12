
#!/bin/env python
#
"""
 Interface for configuring pre/post icetray scripts

 copyright  (c) 2005 the icecube collaboration

 @version: $Revision: $
 @date: $Date: $
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import os
import re
import sys
import math
import dircache
import time
import string
import shutil
import cPickle
from ipmodule import IPBaseClass
import logging

class RenameFile(IPBaseClass):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.AddParameter('infile','Name of file you want to rename','')
	    self.AddParameter('outfile','What you want to rename the file to','')
	    self.logger = logging.getLogger('iceprod::RenameFile')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    infile = self.GetParameter('infile')
	    outfile = self.GetParameter('outfile')
	    cmd = "mv %s %s" % (infile,outfile)
	    retval = os.system(cmd)
	    if retval == 0:
	        return retval
	    else:
	        self.logger.error("Failed to execute command '%s'" % cmd)
	        raise Exception, "Failed to execute command '%s'" % cmd


class RenameMultipleFiles(IPBaseClass):
	"""
	This class provides functionality for renaming multiple files using Bash
	substitutions.
	"""

	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.AddParameter('inpattern','Pattern to match against file names when checking for files to rename','')
	    self.AddParameter('outpattern','Bash substitution expression to be applied to filenames when output','')
	    self.logger = logging.getLogger('iceprod::RenameMultipleFiles')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    inpattern  = self.GetParameter('inpattern')
	    outpattern = self.GetParameter('outpattern')
	    cmd = "for i in %s; do mv $i ${i/%s}; done" % (inpattern, outpattern)
	    retval = os.system(cmd)
	    if retval == 0:
	        return retval
	    else:
	        msg = "Failed to execute command '%s'" % cmd
	        self.logger.error(msg)
	        raise Exception, msg


class RemoveFiles(IPBaseClass):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.AddParameter('filelist','The names of the files you want to delete',[])
	    self.AddParameter('IgnoreError','ignore error from cmd',False)
	    self.logger = logging.getLogger('iceprod::RemoveFiles')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    filelist = self.GetParameter('filelist')
	    retval = 0
	    cmd = ''
	    for file in filelist:
	        cmd = "rm -f %s" % file
	        retval += os.system(cmd)
	    if retval == 0:
	        return retval
	    else:
	        self.logger.error("Failed to execute command '%s'" % cmd)
	        raise Exception, "Failed to execute command '%s'" % cmd

class CompressFile(IPBaseClass):
    """
    This class provides an interface for compressing files in iceprod
    """

    def __init__(self):
        IPBaseClass.__init__(self)
        self.AddParameter('infile','Name of file you want to compress','')
        self.AddParameter('outfile','Name of output file','')
        self.logger = logging.getLogger('iceprod::CompressFile')


    def Execute(self,stats):
        if not IPBaseClass.Execute(self,stats): return 0
        infile = self.GetParameter('infile')
        outfile = self.GetParameter('outfile')
        cmd = "gzip -f -q -6 -c %s > %s" % (infile,outfile)
        retval = os.system(cmd)
        if retval == 0:
            return retval
        else:
            self.logger.error("Failed to execute command '%s'" % cmd)
            raise Exception, "Failed to execute command '%s'" % cmd

class Tarball(IPBaseClass):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.AddParameter('filelist','The names of the files you want to delete',[])
	    self.AddParameter('outputfile','Name of output tarball',"Tarball.tar")
	    self.logger = logging.getLogger('iceprod::Tarball')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    filelist = self.GetParameter('filelist')
	    retval = 0
	    cmd = ''
	    for file in filelist:
	        dir = os.path.dirname(file)
	        cmd = "tar"
	        if os.path.isdir(dir):
	           cmd += " -C%s " % dir
	        if not os.path.exists(tar):
	           cmd += " -cf"
	        else:
	           cmd += " -uf"
	        cmd += " %s %s" %(tar,os.path.basename(file))
	        self.logger.debug(cmd)
	        retval += os.system(cmd)
	    if retval == 0:
	        return retval
	    else:
	        self.logger.error("Failed to execute command '%s'" % cmd)
	        raise Exception, "Failed to execute command '%s'" % cmd


class SummaryMerger(IPBaseClass):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.AddParameter('inputfilelist','The names of the files you want to merge',[])
	    self.AddParameter('outputfile','Name of output tarball',"summary.xml")
	    self.logger = logging.getLogger('iceprod::SummaryMerger')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    from iceprod.core.lex import XMLSummaryParser

	    filelist = self.GetParameter('inputfilelist')
	    outfile  = self.GetParameter('outputfile')
	    retval = 0
	    cmd = ''
	    summary = XMLSummaryParser()
	    summary.summary_map = stats
	    for file in filelist:
	        stats = XMLSummaryParser().ParseFile(file)
	        for key,value in stats.items():
	            if summary.summary_map.has_key(key):
	                summary.summary_map[key] += value
	            else:
	                summary.summary_map[key]  = value
	    summary.Write(outfile)
	    return 0

