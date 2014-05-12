
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
from os.path import expandvars
import iceprod.modules
from ipmodule import IPBaseClass
from gsiftp import URLCopy,TrackURLCopy
from iceprod.core import odict,functions
import logging

class IceTray(IPBaseClass):
    """
    This class provides an interface for preprocessing files in iceprod
    """

    def __init__(self):
        IPBaseClass.__init__(self)
        self.AddParameter('IPModuleURL','SVN URL of python script', '')
        self.AddParameter('IPModuleRevision','SVN revision of python script',0)
        self.AddParameter('IPModuleClass','class to load','')
        self.AddParameter('IPModuleCache','should cache downloads',True)
        self.logger = logging.getLogger('iceprod::IPIceTray')

        self.child_parameters    = odict.OrderedDict()

    def SetParameter(self, param, value):
        """
         Overload SetParameter in order to allow for undefined parameters
        """
        if param in ('IPModuleURL','IPModuleRevision','IPModuleClass'):
           IPBaseClass.SetParameter(self,param,value)
        else:
           self.child_parameters[param] = value


    def Execute(self,stats):
        if not IPBaseClass.Execute(self,stats): return 0

        # set python path
        sys.path.insert(0,os.getcwd())

        url        = self.GetParameter('IPModuleURL')
        classname  = self.GetParameter('IPModuleClass')
        cache_src  = self.GetParameter('IPModuleCache')
        filt       = os.path.basename(url)
         
        if functions.wget(url,cache=cache_src):
            raise Exception, "Failed to retrieve i3filter from '%s'" % url

        mod = iceprod.modules.get_plugin(classname)()
        mod.SetParser(self.parser)

        # Pass parameters to module
        for name,value in self.child_parameters.items():
            mod.SetParameter(name,value)

        return mod.Execute(stats)


class Processing(IceTray,TrackURLCopy):
    """
    This class provides an interface for preprocessing files in iceprod
    """

    def __init__(self):
        
        import copy
        IceTray.__init__(self)
        TrackURLCopy.__init__(self)

        self.AddParameter('NameOfInputFileList',
                        'Name of input file list to pass to child module',
                        'InputFileList')
        self.AddParameter('OutFilePattern','Name or pattern of ouput files to transfer on completion','')
        self.logger = logging.getLogger('iceprod-modules::i3.Processing')

    def SetParameter(self, param, value):
        """
         Overload SetParameter in order to allow for undefined parameters
        """
        if param.lower() in self.parameters.keys():
           IPBaseClass.SetParameter(self,param,value)
        else:
           self.child_parameters[param] = value


    def Execute(self,stats):
        if not IPBaseClass.Execute(self,stats): return 0
        import xmlrpclib
        import cPickle
        import time
        import glob

        # set python path
        sys.path.insert(0,os.getcwd())

        module_url = self.GetParameter('IPModuleURL')
        classname  = self.GetParameter('IPModuleClass')
        cache_src  = self.GetParameter('IPModuleCache')
        filt       = os.path.basename(module_url)

        # SoapMon configuration 
        monitor_url = self.GetParameter('monitorURL')
        src         = self.GetParameter('source')
        dest        = self.GetParameter('destination')
        datasetid   = int(self.GetParameter('dataset'))
        jobid       = int(self.GetParameter('job'))
        passcode    = self.GetParameter('key')

        outfile_pat = self.GetParameter('OutFilePattern')

        starttime   = time.time()

        print "module_url", module_url
        if functions.wget(module_url,cache=cache_src):
            raise Exception, "Failed to retrieve i3filter from '%s'" % module_url

        mod = iceprod.modules.get_plugin(classname)()
        mod.SetParser(self.parser)

        server = xmlrpclib.ServerProxy(monitor_url)
        files  = cPickle.loads(server.get_storage_url(datasetid,jobid,passcode,'INPUT'))

        input_file_list = []
        self.SetParameter('destination',expandvars("file:$PWD/"))
        for file in files: # fetch input files
            self.SetParameter('source',os.path.join(file['path'],file['name']))

            URLCopy.Execute(self,stats)
            md5sum   = functions.md5sum(file['name'])
            filesize = float(os.path.getsize(file['name']))

            if md5sum != file['md5sum'].replace(':',''):
               self.logger.error('md5sum mismatch %s : %s - for %s' % (file['md5sum'],md5sum,file['name']))
               raise Exception, 'md5sum mismatch %s : %s - for %s' % (file['md5sum'],md5sum,file['name'])

            input_file_list.append(file['name'])

        # Pass parameters to module
        for name,value in self.child_parameters.items():
            mod.SetParameter(name,value)

        listname = self.GetParameter('NameOfInputFileList')
        mod.SetParameter(listname,input_file_list)

        retval = mod.Execute(stats)

        self.SetParameter('destination',dest) # set original destination

        # Upload output files
        for outfile in glob.glob(outfile_pat): # upload output files
            if not outfile in input_file_list:
               self.SetParameter('source',expandvars(os.path.join("file:$PWD",outfile)))
               TrackURLCopy.Execute(self,stats)

        # Finally upload log files
        self.SetParameter('source',expandvars("file:$ICEPROD_STDOUT"))
        URLCopy.Execute(self,stats)
        self.SetParameter('source',expandvars("file:$ICEPROD_STDERR"))
        URLCopy.Execute(self,stats)
        icetraylog = expandvars("$I3_TOPDIR/icetray.%06u.log" % jobid)
        if os.path.exists(icetraylog):
           self.SetParameter('source',expandvars("file:%s" % icetraylog))
           URLCopy.Execute(self,stats)

        return retval
