#!/bin/env python
#
"""
 Interface for configuring pre/post icetray scripts

 copyright  (c) 2005 the icecube collaboration

 @version: $Revision: $
 @date: $Date: $
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import os,sys
import time
import string
import glob
import commands
from os.path import expandvars,basename
from ipmodule import IPBaseClass
from iceprod.core.inventory import FileInventory
from iceprod.core import functions
import xmlrpclib
import logging


class URLCopy(IPBaseClass):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.executable = 'globus-url-copy'
	    self.AddParameter('source','source URL to copy','')
	    self.AddParameter('destination','detination URL to copy to','')
	    self.AddParameter('certdir','certificate directory','')
	    self.AddParameter('proxyfile','File path to globus proxy','$X509_USER_PROXY')
	    self.AddParameter('ldpath','library path to globus','globus/lib')
	    self.AddParameter('path','path to globus bin directory','globus/bin')
	    self.AddParameter('opts','globus-url-copy options',
                       ['-rst','-cd','-r','-nodcau','-rst-retries 5','-rst-interval 60']
                      )
	    self.AddParameter('executable','name of gridFTP executable','globus-url-copy')
	    self.AddParameter('StorageElement','LFN SE','')
	    self.AddParameter('lfn-opts','LFN Options','')
	    self.AddParameter('inventory','File with source dest mappings','$I3_TOPDIR/inventory.xml')
	    self.AddParameter('emulate',"Don't actually transfer files. Just write inventory",False)
	    self.logger = logging.getLogger('iceprod::URLCopy')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    src      = self.GetParameter('source')
	    dest     = self.GetParameter('destination')
	    proxy    = self.GetParameter('proxyfile')
	    opts     = self.GetParameter('opts')
	    ldpath   = self.GetParameter('ldpath')
	    path     = self.GetParameter('path')
	    inventory= self.GetParameter('inventory')
	    emulate  = self.GetParameter('emulate')
	    exe      = self.GetParameter('executable')
	    se       = self.GetParameter('StorageElement')
	    lfnopts  = self.GetParameter('lfn-opts')
	    #exe      = os.path.join(path,exe)

	    if src.startswith('lfn:') or dest.startswith('lfn:'):
	       self.logger.info("detected LFN URL. Hading control to lfn module")
	       from lfn import LFN_CR_Copy
	       ipmod = LFN_CR_Copy()
	       ipmod.SetParameter('source',src)
	       ipmod.SetParameter('destinationPATH',dest)
	       if se:
	          ipmod.SetParameter('destination',se)
	       if lfnopts:
	          ipmod.SetParameter('opts',lfnopts)
	       return ipmod.Execute(stats)
	      

	    certdir  = self.GetParameter('certdir')
	    if certdir and os.path.exists(expandvars(certdir)):
	       os.putenv('X509_CERT_DIR',expandvars(certdir))

	    inventory = expandvars(inventory)
	    oi = FileInventory()
	    if os.path.exists(inventory):
	       oi.Read(inventory)

	    os.putenv('X509_USER_PROXY',expandvars(proxy))
	    os.chmod(expandvars(proxy),0600)
	    os.putenv('LD_LIBRARY_PATH',expandvars("%s:$LD_LIBRARY_PATH" % ldpath))
	    os.putenv('PATH',expandvars("%s:$PATH" % path))

	    cmd = []
	    cmd.append(exe)
	    cmd.extend(opts)
	    cmd.append(src)
	    cmd.append(dest)
	    cmd = " ".join(cmd)

	    if not emulate:
	       status, output = commands.getstatusoutput(cmd)
	       if  status:
	           self.logger.error("Failed to execute command '%s',%s" % (cmd,output))
	           raise Exception, "Failed to execute command '%s',%s" % (cmd,output)
	       self.logger.info(output)
	    else:
	       try: # get xfer stats
	         if dest.startswith('gsiftp:') and src.startswith('file:'):
	            stats['data-out'] = float(os.path.getsize(expandvars(src.replace('file:',''))))
	         elif src.startswith('gsiftp:') and dest.startswith('file:'):
	            stats['data-in'] = float( os.path.getsize(
	                                 os.path.join(expandvars(dest.replace('file:','')),basename(src))
                                   ))
	       except: pass
	       oi.AddFile(src,dest)
	       oi.Write(inventory)
	    return 0


class URLGlobCopy(URLCopy):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    URLCopy.__init__(self)
	    self.logger = logging.getLogger('iceprod::URLGlobCopy')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    src      = self.GetParameter('source')
	    dest     = self.GetParameter('destination')
	    proxy    = self.GetParameter('proxyfile')
	    opts     = self.GetParameter('opts')
	    ldpath   = self.GetParameter('ldpath')
	    path     = self.GetParameter('path')
	    inventory= self.GetParameter('inventory')
	    emulate  = self.GetParameter('emulate')
	    exe      = self.GetParameter('executable')
	    #exe      = os.path.join(path,exe)

	    certdir  = self.GetParameter('certdir')
	    if certdir:
	       os.putenv('X509_CERT_DIR',expandvars(certdir))

	    os.putenv('X509_USER_PROXY',expandvars(proxy))
	    os.chmod(expandvars(proxy),0600)
	    os.putenv('LD_LIBRARY_PATH',os.path.expandvars("%s:$LD_LIBRARY_PATH" % ldpath))
	    os.putenv('PATH',os.path.expandvars("%s:$PATH" % path))

	    oi = FileInventory()
	    inventory = expandvars(inventory)
	    if os.path.exists(inventory):
	       oi.Read(inventory)

	    retval = 0
	    if src.startswith('file:'):
	       for file in glob.glob(expandvars(src.replace('file:',''))):
	          
	          oi.AddFile(file,dest)
	          cmd = []
	          cmd.append(exe)
	          cmd.extend(opts)
	          cmd.append('file://'+os.path.abspath(os.path.normpath(file)))
	          cmd.append(dest)
	          cmd = " ".join(cmd)
	          if not emulate:
	              self.logger.info(cmd)
	              status, output = commands.getstatusoutput(cmd)
	              self.logger.info(output)
	              if  status:
	                  self.logger.error("Failed to execute command '%s',%s" % (cmd,output))
	                  raise Exception, "Failed to execute command '%s',%s" % (cmd,output)
	    else:
	       oi.AddFile(src,dest)
	       cmd = []
	       cmd.append(exe)
	       cmd.extend(opts)
	       cmd.append(src)
	       cmd.append(dest)
	       cmd = " ".join(cmd)
	       if not emulate:
	          self.logger.info(cmd)
	          status, output = commands.getstatusoutput(cmd)
	          self.logger.info(output)
	          if  status:
	             self.logger.error("Failed to execute command '%s',%s" % (cmd,output))
	             raise Exception, "Failed to execute command '%s',%s" % (cmd,output)

	    if emulate:
	       oi.Write(inventory)
	    return 0


class URLMultiCopy(URLCopy):
	"""
	This class provides an interface for preprocessing files in iceprod
	"""

	def __init__(self):
	    URLCopy.__init__(self)
	    self.logger = logging.getLogger('iceprod::URLMultiCopy')
	    self.AddParameter('sourcelist','list of source URLs (files) to copy','')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    src      = self.GetParameter('sourcelist')
	    dest     = self.GetParameter('destination')
	    proxy    = self.GetParameter('proxyfile')
	    opts     = self.GetParameter('opts')
	    ldpath   = self.GetParameter('ldpath')
	    path     = self.GetParameter('path')
	    inventory= self.GetParameter('inventory')
	    emulate = self.GetParameter('emulate')
	    exe      = self.GetParameter('executable')
	    #exe      = os.path.join(path,exe)

	    certdir  = self.GetParameter('certdir')
	    if certdir:
	       os.putenv('X509_CERT_DIR',expandvars(certdir))

	    os.putenv('X509_USER_PROXY',expandvars(proxy))
	    os.chmod(expandvars(proxy),0600)
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
	        cmd.append(expandvars(file))
	        cmd.append(dest)
	        cmd = " ".join(cmd)

	        if not emulate:
	          self.logger.info(cmd)
	          status, output = commands.getstatusoutput(cmd)
	          self.logger.info(output)
	          if  status:
	             self.logger.error("Failed to execute command '%s',%s" % (cmd,output))
	             raise Exception, "Failed to execute command '%s',%s" % (cmd,output)
	    if emulate:
	       oi.Write(inventory)
	    return 0

class TrackURLCopy(URLCopy):
	"""
	This class provides an interface for preprocessing files in iceprod
	It also tracks the destination of files in the monitoring database
	throught the soapmon server.
	"""

	def __init__(self):
	    URLCopy.__init__(self)
	    self.logger = logging.getLogger('iceprod::TrackURLCopy')
	    self.AddParameter(
              'monitorURL',
              'soapmon url',
              'http://x2100.icecube.wisc.edu/cgi-bin/simulation/mon/soapmon-cgi')
	    self.AddParameter('dataset','dataset ID',0)
	    self.AddParameter('job','job ID',0)
	    self.AddParameter('key','Temporary password for soapmon','')

	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    url       = self.GetParameter('monitorURL')
	    src       = self.GetParameter('source')
	    dest      = self.GetParameter('destination')
	    datasetid = int(self.GetParameter('dataset'))
	    jobid     = int(self.GetParameter('job'))
	    passcode  = self.GetParameter('key')
	    starttime = time.time()
	    if not URLCopy.Execute(self,stats):
	       md5sum       = ''
	       filesize     = 0.
	       transfertime = time.time() - starttime

	       if src.startswith('file:'):
	          md5sum = functions.md5sum(expandvars(src.replace('file:','')))
	          filesize = float(os.path.getsize(expandvars(src.replace('file:',''))))
	       server = xmlrpclib.ServerProxy(url)
	       if dest.endswith('/'):
	          dest += basename(src)
	       self.logger.info('%s %s' % (dest,md5sum))
	       if not server.AddFileURL(datasetid,jobid,dest,md5sum,filesize,transfertime,passcode):
	          raise Exception, "Failed to set URL for for %s -> %s" % (src,dest)
	       return 0
	    return 1

class AltSourceURLCopy(URLCopy):
	"""
	This class provides an interface for preprocessing files in iceprod
	It also tracks the destination of files in the monitoring database
	throught the soapmon server.
	"""

	def __init__(self):
	    URLCopy.__init__(self)
	    self.logger = logging.getLogger('iceprod::AltSourceURLCopy')
	    self.AddParameter('source1','backup source URL to copy','')
	    self.AddParameter('source2','backup source URL to copy','')
	    self.AddParameter('source3','backup source URL to copy','')

	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    src       = self.GetParameter('source')
	    src1      = self.GetParameter('source1')
	    src2      = self.GetParameter('source2')
	    src3      = self.GetParameter('source3')
	    dest      = self.GetParameter('destination')
	    for s in [src,src1,src2,src3]:
	        if not s: continue
	        self.SetParameter('source',s)
	        try:
	           if not URLCopy.Execute(self,stats) : return 0
	        except Exception,e:
	           self.logger.error(e)
	    raise Exception, "Failed to copy %s to %s" % (src,dest)
