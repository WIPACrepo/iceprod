#!/bin/env python
#
"""
 Interface for configuring pre/post icetray scripts

 copyright  (c) 2009 the icecube collaboration

 @version: $Revision: $
 @date: $Date: $
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
 @author: Fabian Clevermann <fabian.clevermann@udo.edu>
"""

import os,sys
import time
import string
import glob
import commands
from os.path import expandvars,basename
from ipmodule import IPBaseClass
from iceprod.core.inventory import FileInventory
import xmlrpclib
import logging


class LFN_CR_Copy(IPBaseClass):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""
	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.executable = 'lcg-cr'
	    self.AddParameter('destinationPATH','Path on the SE where the files go.','')    # lfn:/grid/icecube/organizing/folders/output.file
	    self.AddParameter('source','source URL to copy','') # file:$RUNDIR/output.file
	    self.AddParameter('destination','On this SE will the files be copied. Leave empty or you get problems if this SE is down/full.','')
	    self.AddParameter('ldpath','library path to lfn','')		
	    self.AddParameter('path','path to lfn bin directory','')	
	    self.AddParameter('opts','lcg-cr options',['--vo icecube'])
	    self.AddParameter('executable','name of lcg-cr executable','lcg-cr')
	    self.AddParameter('inventory','File with source dest mappings','$I3_TOPDIR/inventory.xml')
	    self.AddParameter('emulate',"Don't actually transfer files. Just write inventory",False)
	    self.logger = logging.getLogger('iceprod::lcg-cr_Copy')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    src      = self.GetParameter('source')
	    destinationSE     = self.GetParameter('destination')
	    destinationPATH = self.GetParameter('destinationPATH')
	    opts     = self.GetParameter('opts')
	    ldpath   = self.GetParameter('ldpath')
	    path     = self.GetParameter('path')
	    inventory= self.GetParameter('inventory')
	    emulate  = self.GetParameter('emulate')
	    exe      = self.GetParameter('executable')
	    exe      = os.path.join(path,exe)

	    oi = FileInventory()
	    inventory = expandvars(inventory)
	    if os.path.exists(inventory):
	       oi.Read(inventory)


	    os.putenv('LD_LIBRARY_PATH',expandvars("%s:$LD_LIBRARY_PATH" % ldpath))
	    os.putenv('PATH',expandvars("%s:$PATH" % path))

#	    lcg-cr --vo icecube [-d $SE] -l lfn:/grid/icecube/$NAME/here/the/new/file.txt file:/here/the/old/local/file.txt
	    cmd = []
	    cmd.append(exe)
	    cmd.extend(opts)

	    if destinationSE:
	        cmd.append("-d %s"% destinationSE)
	    cmd.append("-l %s"% destinationPATH)
	    cmd.append(src)
	    cmd = " ".join(cmd)

	    SE_Path = os.path.split(destinationPATH)[0][4:] # The Path on the SE without lfn: and without the filename
	    os.system("export LFC_HOST=`lcg-infosites --vo icecube lfc`")   # needed for lfc-mkdir
	    os.system("export LCG_GFAL_INFOSYS=grid-bdii.desy.de:2170")     # also needed
	    
	    if not emulate:
	       status, output = commands.getstatusoutput("lfc-mkdir -p " + SE_Path)
	       if status:
	           self.logger.error("Failed to execute command 'lfc-mkdir -p %s',%s" % (SE_Path,output))
	           raise Exception, "Failed to execute command 'lfc-mkdir -p %s',%s" % (SE_Path,output)
	       else:
	           status, output = commands.getstatusoutput(cmd)
	           if  status:
	               self.logger.error("Failed to execute command '%s',%s" % (cmd,output))
	               raise Exception, "Failed to execute command '%s',%s" % (cmd,output)

	    oi.AddFile(src,dest)
	    oi.Write(inventory)
	    self.logger.info(output)
	    return 0


class LFN_CR_GlobCopy(LFN_CR_Copy):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    LFN_CR_Copy.__init__(self)
	    self.logger = logging.getLogger('iceprod::URLGlobCopy')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    src      = self.GetParameter('source')
	    destinationSE     = self.GetParameter('destination')
	    destinationPATH = self.GetParameter('destinationPATH')
	    opts     = self.GetParameter('opts')
	    ldpath   = self.GetParameter('ldpath')
	    path     = self.GetParameter('path')
	    inventory= self.GetParameter('inventory')
	    emulate = self.GetParameter('emulate')
	    exe      = self.GetParameter('executable')
	    exe      = os.path.join(path,exe)


	    os.putenv('LD_LIBRARY_PATH',os.path.expandvars("%s:$LD_LIBRARY_PATH" % ldpath))
	    os.putenv('PATH',os.path.expandvars("%s:$PATH" % path))

	    oi = FileInventory()
	    inventory = expandvars(inventory)
	    if os.path.exists(inventory):
	       oi.Read(inventory)

	    retval = 0

	    for file in glob.glob(expandvars(src.replace('file:',''))):
	     
	     oi.AddFile(file,dest)
	     cmd = []
	     cmd.append(exe)
	     cmd.extend(opts)
	     if destinationSE:
	         cmd.append("-d %s"% destinationSE)
	     cmd.append("-l %s"% destinationPATH)
	     cmd.append('file:'+os.path.abspath(os.path.normpath(file)))

	     cmd = " ".join(cmd)
	     SE_Path = os.path.split(destinationPATH)[0][4:] # The Path on the SE without lfn: and without the filename
	     os.system("export LFC_HOST=`lcg-infosites --vo icecube lfc`")   # needed for lfc-mkdir
	     os.system("export LCG_GFAL_INFOSYS=grid-bdii.desy.de:2170")     # also needed
	     if not emulate:
	              self.logger.info(cmd)
	              status, output = commands.getstatusoutput("lfc-mkdir -p " + SE_Path)
	              if status:
	                  self.logger.error("Failed to execute command 'lfc-mkdir -p %s',%s" % (SE_Path,output))
	                  raise Exception, "Failed to execute command 'lfc-mkdir -p %s',%s" % (SE_Path,output)
	              else:
	                  status, output = commands.getstatusoutput(cmd)
	                  self.logger.info(output)
	                  if  status:
	                      self.logger.error("Failed to execute command '%s',%s" % (cmd,output))
	                      raise Exception, "Failed to execute command '%s',%s" % (cmd,output)

	    oi.Write(inventory)
	    return 0


class LFN_CR_MultiCopy(LFN_CR_Copy):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    LFN_CR_Copy.__init__(self)
	    self.logger = logging.getLogger('iceprod::URLMultiCopy')
	    self.AddParameter('sourcelist','list of source URLs (files) to copy','')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    src      = self.GetParameter('sourcelist')
	    destinationSE     = self.GetParameter('destination')
	    destinationPATH = self.GetParameter('destinationPATH')
	    opts     = self.GetParameter('opts')
	    ldpath   = self.GetParameter('ldpath')
	    path     = self.GetParameter('path')
	    inventory= self.GetParameter('inventory')
	    emulate = self.GetParameter('emulate')
	    exe      = self.GetParameter('executable')
	    exe      = os.path.join(path,exe)


	    os.putenv('LD_LIBRARY_PATH',os.path.expandvars("%s:$LD_LIBRARY_PATH" % ldpath))
	    os.putenv('PATH',os.path.expandvars("%s:$PATH" % path))

	    oi = FileInventory()
	    inventory = expandvars(inventory)
	    if os.path.exists(inventory):
	       oi.Read(inventory)

	    retval = 0
	    for file in src:
	     oi.AddFile(file,dest)
	     cmd = []
	     cmd.append(exe)
	     cmd.extend(opts)
	     if destinationSE:
	         cmd.append("-d %s"% destinationSE)
	     cmd.append("-l %s"% destinationPATH)
	     cmd.append('file:'+os.path.abspath(os.path.normpath(file)))

	     cmd = " ".join(cmd)
	     SE_Path = os.path.split(destinationPATH)[0][4:] # The Path on the SE without lfn: and without the filename
	     os.system("export LFC_HOST=`lcg-infosites --vo icecube lfc`")   # needed for lfc-mkdir
	     os.system("export LCG_GFAL_INFOSYS=grid-bdii.desy.de:2170")     # also needed
	     if not emulate:
	              self.logger.info(cmd)
	              status, output = commands.getstatusoutput("lfc-mkdir -p " + SE_Path)
	              if status:
	                  self.logger.error("Failed to execute command 'lfc-mkdir -p %s',%s" % (SE_Path,output))
	                  raise Exception, "Failed to execute command 'lfc-mkdir -p %s',%s" % (SE_Path,output)
	              else:
	                  status, output = commands.getstatusoutput(cmd)
	                  self.logger.info(output)
	                  if  status:
	                      self.logger.error("Failed to execute command '%s',%s" % (cmd,output))
	                      raise Exception, "Failed to execute command '%s',%s" % (cmd,output)

	    oi.Write(inventory)
	    return 0
	
class LFN_CP_Copy(IPBaseClass):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""
	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.executable = 'lcg-cp'
	    self.AddParameter('destinationPATH','Path where the files go.','')  # /here/the/new/local/output.file
	    self.AddParameter('source','source on the SE to copy','')   # lfn:/grid/icecube/$name/output.file
#	    self.AddParameter('destination','destination URL to copy to','')
	    self.AddParameter('ldpath','library path to lfn','')		
	    self.AddParameter('path','path to lfn bin directory','')	
	    self.AddParameter('opts','lcg-cr options',['-v','--vo icecube'])
	    self.AddParameter('executable','name of lcg-cr executable','lcg-cp')
	    self.AddParameter('inventory','File with source dest mappings','$I3_TOPDIR/inventory.xml')
	    self.AddParameter('emulate',"Don't actually transfer files. Just write inventory",False)
	    self.logger = logging.getLogger('iceprod::lcg-cp_Copy')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    src      = self.GetParameter('source')
#	    destinationSE     = self.GetParameter('destination')
	    destinationPATH = self.GetParameter('destinationPATH')
	    opts     = self.GetParameter('opts')
	    ldpath   = self.GetParameter('ldpath')
	    path     = self.GetParameter('path')
	    inventory= self.GetParameter('inventory')
	    emulate = self.GetParameter('emulate')
	    exe      = self.GetParameter('executable')
	    exe      = os.path.join(path,exe)

	    oi = FileInventory()
	    inventory = expandvars(inventory)
	    if os.path.exists(inventory):
	       oi.Read(inventory)


	    os.putenv('LD_LIBRARY_PATH',expandvars("%s:$LD_LIBRARY_PATH" % ldpath))
	    os.putenv('PATH',expandvars("%s:$PATH" % path))
	    
#       lcg-cp --vo icecube -v lfn:/grid/icecube/$NAME/here/the/old/output.file file:/here/the/new/local/output.file
	    cmd = []
	    cmd.append(exe)
	    cmd.extend(opts)
#	    cmd.append("-d %s"% destinationSE)
	    cmd.append(src)
	    cmd.append("file:%s"% destinationPATH)

	    cmd = " ".join(cmd)

	    destinationPATH = os.path.split(destinationPATH)[0] # remove filename for mkdir
	    if not emulate:
	        status, output = commands.getstatusoutput("mkdir -p %s"% destinationPATH)
	        if status:
	            self.logger.error("Failed to execute command 'mkdir -p %s',%s" % (destinationPATH,output))
	            raise Exception, "Failed to execute command 'mkdir -p %s',%s" % (destinationPATH,output)
	        else:
	            status, output = commands.getstatusoutput(cmd)
	            if  status:
	                self.logger.error("Failed to execute command '%s',%s" % (cmd,output))
	                raise Exception, "Failed to execute command '%s',%s" % (cmd,output)

	    oi.AddFile(src,dest)
	    oi.Write(inventory)
	    self.logger.info(output)
	    return 0
