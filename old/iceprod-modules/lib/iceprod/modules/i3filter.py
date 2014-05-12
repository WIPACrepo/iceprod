
#!/bin/env python
#
"""
 Interface for configuring and running python filtering scripts 

 copyright  (c) 2005 the icecube collaboration

 @version: $Revision: $
 @date: $Date: $
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import os
import re
import sys
import string
import os.path
from ipmodule import IPBaseClass
from iceprod.core import functions
import logging

class I3MCFilter(IPBaseClass):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.AddParameter('URL','SVN URL of python script',
                'http://code.icecube.wisc.edu/svn/meta-projects/std-processing/releases/V03-03-04/scripts/IC40/level1_sim_IC40.py')
	    self.AddParameter('Revision','SVN revision of python script',0)
	    self.AddParameter('GCDFILE','Input GCD file','')
	    self.AddParameter('INFILE','Input simulation file','')
	    self.AddParameter('OUTFILE','write output to this file','')
	    self.AddParameter('NUMEVENTS','Number of events to process',0)
	    self.AddParameter('MC','Configure prescales for normal MC data',0)
	    self.AddParameter('IT','Configure prescales for IceTop MC data',0)
	    self.AddParameter('PhotoRecDriverFile','DriverFile for PhotoRec tables','')
	    self.AddParameter('TableDir','directory where the photonics tables are located','')
	    self.AddParameter('SummaryFile','XML Summary file','')
	    self.logger = logging.getLogger('iceprod::I3MCFilter')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    url        = self.GetParameter('URL')
	    gcdfile    = self.GetParameter('GCDFILE')
	    infile     = self.GetParameter('INFILE')
	    outfile    = self.GetParameter('OUTFILE')
	    numevents  = self.GetParameter('NUMEVENTS')
	    mc         = self.GetParameter('MC')
	    it         = self.GetParameter('IT')
	    driverfile = self.GetParameter('PhotoRecDriverFile')
	    tabledir   = self.GetParameter('TableDir')
	    summaryfile = self.GetParameter('SummaryFile')
	    filt       = os.path.basename(url)

	    if functions.wget(url):
	        raise Exception, "Failed to retrieve i3filter from '%s'" % url

	    cmd = "python %s -g%s -i%s -o%s" % (filt,gcdfile,infile,outfile)
	    if numevents:
	       cmd += " -n%d "  % numevents
	    if mc:
	       cmd += " -m " 
	    if it:
	       cmd += " -y " 
	    if tabledir:
	       cmd += " -t%s" % tabledir
	    if driverfile:
	       cmd += " -d%s" % driverfile
	    if summaryfile:
	       cmd += " -x%s" % summaryfile
	    self.logger.info(cmd)
	    retval = os.system(cmd)
	    if retval == 0:
	        return retval
	    else:
	        self.logger.error("Failed to execute command '%s'" % cmd)
	        raise Exception, "Failed to execute command '%s'" % cmd
