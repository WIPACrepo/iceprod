
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

class StagePhotoTables(IPBaseClass):
    """
    This class provides an interface for preprocessing files in iceprod
    """

    def __init__(self):
        IPBaseClass.__init__(self)
        self.cleanup = []
        self.AddParameter("DriverFileDirectory",
                       "photonics driver file parent directory",
                       "$PWD/");

        self.AddParameter("PhotonicsAngularSelectionHigh",
                       "Maximum theta angle of tables to stage",
                       180.0);

        self.AddParameter("PhotonicsAngularSelectionLow",
                       "Minimum theta angle of tables to stage",
                       0.0);

        self.AddParameter("TablesDirectory",
                       "Location where tables should be found or staged if not found",
                       "$system(photontablesdir)");

        self.AddParameter("TablesRepository",
                       "Location where tables should be fetched from if not found",
                       "$system(photontablesrepo)");

        self.AddParameter("PhotonicsLevel1DriverFile","","$steering(PHOTONTABLES::L1_IC)");
        self.AddParameter("PhotonicsLevel2DriverFile","","$steering(PHOTONTABLES::L2_IC)");

        self.logger = logging.getLogger('iceprod::StagePhotoTables')


    def Execute(self,stats):
        if not IPBaseClass.Execute(self,stats): return 0
        self.cleanup = []

        # photonics 
        # angular range is evaluated from steering paramters
        pt_amax = self.GetParameter("PhotonicsAngularSelectionHigh");
        pt_amin = self.GetParameter("PhotonicsAngularSelectionLow");
        pt_l1   = self.parser.parse( self.GetParameter("PhotonicsLevel1DriverFile") );
        pt_l2   = self.parser.parse( self.GetParameter("PhotonicsLevel2DriverFile") );
        pt_dir  = self.parser.parse( self.GetParameter("TablesDirectory") );
        pt_repo = self.parser.parse( self.GetParameter("TablesRepository") );
        pt_driver_dir  = self.parser.parse( self.GetParameter("DriverFileDirectory") );

        if not os.path.isdir(pt_dir): 
            os.makedirs(pt_dir)

        l1 = open(os.path.join(pt_driver_dir,pt_l1),'r')
        for line in l1.readlines():
            if line.startswith('#'): continue
            line = line.split()
            pt_file = line[0].strip()
            amax    = float(line[-1].strip())
            amin    = float(line[-2].strip())
            zmax    = float(line[-3].strip())
            zmin    = float(line[-4].strip())

            #if (amax > pt_amax): continue
            #if (amin < pt_amin): continue
            if (amin < pt_amin) or (amin > pt_amax): continue

            for s in ['.abs','.prob']:
               if not os.path.exists(os.path.join(pt_dir,pt_file+s)):
                  self.logger.debug('copying %s' % pt_file+s)
                  if not os.path.exists(os.path.dirname(os.path.join(pt_dir,pt_file+s))):
                     os.makedirs(os.path.dirname(os.path.join(pt_dir,pt_file)))
                  if os.system('cp %s %s' % (os.path.join(pt_repo,pt_file+s),os.path.join(pt_dir,pt_file+s))):
                     raise Exception, "cannot copy photon tables from repository"
                  self.cleanup.append(os.path.join(pt_dir,pt_file+s))
        l1.close()

        l2 = open(os.path.join(pt_driver_dir,pt_l2),'r')
        for line in l2.readlines():

            if line.startswith('#'): continue

            line = line.split()
            pt_file = line[0].strip()
            pt_file = pt_file.replace('@starting:','')
            pt_file = pt_file.replace('@stopping:','')
            amax    = float(line[-1].strip())
            amin    = float(line[-2].strip())
            zmax    = float(line[-3].strip())
            zmin    = float(line[-4].strip())

            #if (amax > pt_amax): continue
            #if (amin < pt_amin): continue
            if (amin < pt_amin) or (amin > pt_amax): continue

            for s in ['.abs','.prob']:
               if not os.path.exists(os.path.join(pt_dir,pt_file+s)):
                  self.logger.debug('copying %s' % pt_file+s)
                  if not os.path.exists(os.path.dirname(os.path.join(pt_dir,pt_file+s))):
                     os.makedirs(os.path.dirname(os.path.join(pt_dir,pt_file)))
                  if os.system('cp %s %s' % (os.path.join(pt_repo,pt_file+s),os.path.join(pt_dir,pt_file+s))):
                     raise Exception, "cannot copy photon tables from repository"
                  self.cleanup.append(os.path.join(pt_dir,pt_file+s))
        l2.close()

        return 0

    def Finish(self,stats):
        if not IPBaseClass.Execute(self,stats): return 0
        for f in self.cleanup:
            if os.system('rm -f %s' % f):
                 raise Exception, "cannot remove cached photon tables"
        return 0
