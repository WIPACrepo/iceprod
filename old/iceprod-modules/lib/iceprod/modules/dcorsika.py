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
import glob
import dircache
import time
import string
import shutil
import cPickle
import ipmodule
from ipmodule import IPBaseClass
from os import system
from os.path import expandvars
import logging
import popen2
import getpass
from commands import getstatusoutput
from iceprod.core import exe
from iceprod.core.dataclasses import I3Tarball

class Corsika(IPBaseClass):
    """
    This class provides an interface for preprocessing files in iceprod
    """

    def __init__(self):
        IPBaseClass.__init__(self)
        self.logger = logging.getLogger('iceprod::Corsika')
        self.name   = 'corsika'
        self.parameters['arrang'] = -119.

        self.AddParameter('version','Corsika version','v6900')
        self.AddParameter('platform','compliler platform','')
        self.AddParameter('cache','Should cache taball?',False)
        self.AddParameter('cachedir','Cache directory',
                          '$system(cache)/%s_icesoft/%s'% (getpass.getuser(), self.name))
        self.AddParameter('URL','fetch tarball from URL',None)
        self.AddParameter('runnum','Run number','')
        self.AddParameter('seed','Random seed','1')
        self.AddParameter('egs_seed_offset','value to be added to EGS seed (for debugging)','0')
        self.AddParameter('nevents','Number of Events',0)
        self.AddParameter('outfile','Output file',"DAT%(runnum)06d") 
        self.AddParameter('logfile','Log file','%(outfile)s.log')
        self.AddParameter('outdir','Output directory','.')
        self.AddParameter('topdir','Top directory','')
        self.AddParameter('tmpdir','Temporary directory','%(outdir)s/dcors%(runnum)d')
        self.AddParameter('model','Physics Model','SIBYLL')
        self.AddParameter('lemodel','Low Energy Physics Model','gheisha')
        self.AddParameter('donkg','Run NKG',0)  # no NKG by default
        self.AddParameter('doegs','Run EGS',0)  # no EGS by default
        self.AddParameter('eslope','CR spectral index (only if ranpri=0)',-2.7)  
        self.AddParameter('crtype','CR primary type',14) 
        self.AddParameter('cthmin','Min theta of injected cosmic rays',0.0)  
        self.AddParameter('cthmax','Max theta of injected cosmic rays',89.99)  
        self.AddParameter('cphmin','Min phi of injected cosmic rays',0.0)  
        self.AddParameter('cphmax','Max phi of injected cosmic rays',360.0)  
        self.AddParameter('emin','CR min energy',600.)  
        self.AddParameter('emax','CR max energy',1.e11) 
        self.AddParameter('ecuts','hadron/em energy cut (deprecated: instead use ecuts(i),i=1..4)',0)  
        self.AddParameter('ecuts1','hadron min energy (see corsika docs)',273)  
        self.AddParameter('ecuts2','muon min energy (see corsika docs)',273)  
        self.AddParameter('ecuts3','electron min energy (see corsika docs)',0.003)  
        self.AddParameter('ecuts4','photon min energy (see corsika docs)',0.003)  
        self.AddParameter('atmod','Atmosphere model (October=13)',13) 
        self.AddParameter('debug','boolean: enable disable debug mode',False) 


    def stage(self):
        """
        Stage files and executables
        """

        par = self.parameters
        par['tmpdir'] = self.GetParameter('tmpdir') % par;

        self.logger.info('setting up working directory: %(tmpdir)s' % par)
        if not os.path.exists(par['tmpdir']):
           os.makedirs(par['tmpdir']);     # create temporary directory
        os.chdir(par['tmpdir']);        # cd to temporary directory

        self.logger.info('caching: %s' % self.GetParameter('cache'))
        if self.GetParameter('cache'):
           cachedir = self.parser.parse(self.GetParameter('cachedir'))
           if not os.path.exists(cachedir):
              os.makedirs(cachedir)
        else:
           cachedir = expandvars("$PWD")
        baseurl = par['url']

        meta = I3Tarball()
        meta.version  = self.GetParameter('version')
        meta.platform = self.GetParameter('platform')
        meta.name     = self.name
        meta.suffix   = ".tar.gz"
        if meta.platform:
              meta.filebase = "%s-%s.%s" % (meta.name, meta.version, meta.platform)
        else:
              meta.filebase = "%s-%s" % (meta.name, meta.version)
        meta.md5sum  = "%s.md5sum" % meta.filebase
        meta.url = "%s/%s" % (baseurl,meta.filebase)
        exe.fetch_tarball(meta,cachedir)
        par['topdir'] = meta.path

        # link data files for corsika
        # Files necessary for QGSJET and QGSJET-II included
        # DPMJET and VENUS files are *not* 
        os.symlink("%(topdir)s/bin/NUCNUCCS"     % par, "NUCNUCCS");    
        os.symlink("%(topdir)s/bin/QGSDAT01"     % par, "QGSDAT01");
        os.symlink("%(topdir)s/bin/SECTNU"       % par, "SECTNU");
        os.symlink("%(topdir)s/bin/qgsdat-II-03" % par, "qgsdat-II-03");
        os.symlink("%(topdir)s/bin/sectnu-II-03" % par, "sectnu-II-03");
        os.symlink("%(topdir)s/bin/GLAUBTAR.DAT" % par, "GLAUBTAR.DAT");
        os.symlink("%(topdir)s/bin/NUCLEAR.BIN"  % par, "NUCLEAR.BIN");
        os.environ['LD_LIBRARY_PATH'] = expandvars("%(topdir)s/lib:$LD_LIBRARY_PATH"  % par);
        
        # If we have EGS files too, link those
        egsfiles = glob.glob("%(topdir)s/bin/EGSDAT*"  % par)
       
        # If we have EPOS files too, link those
        eposfiles = glob.glob("%(topdir)s/bin/epos.*"  % par)

        for file in egsfiles + eposfiles:
            os.symlink(file, os.path.basename(file));
 
        # If FLUKA exists, use it
        if os.path.exists("%(topdir)s/fluka" % par):
            os.environ['FLUPRO'] = expandvars("%(topdir)s/fluka" % par);

        return meta.path

        

    def Execute(self,stats):
        """ 
         Run CORSIKA
         corsika output is stdout: must create a temporary directory and cd there
        """ 

        cwd = os.getcwd()
        par = self.parameters

        # Retrieve tarball and stage environment
        self.stage()
        self.configure()
        self.write_steering()

        # CORSIKA binary
        # New standard binary name style
        par['model'] = par['model'].upper()
        par['versionnumber'] = par['version'].lstrip('v')
        par['corsbinary'] = "%(topdir)s/bin/corsika%(versionnumber)sLinux_%(model)s_%(lemodel)s" % par;
        if not os.path.exists(par['corsbinary']):
            os.chdir(cwd);           # cd back to original directory
            print >> sys.stderr,"CORSIKA binary does not exist: corsika%(versionnumber)sLinux_%(model)s_%(lemodel)s\n" % par;
            raise Exception,"CORSIKA binary does not exist: corsika%(versionnumber)sLinux_%(model)s_%(lemodel)s\n" % par;
        # Old symlink style
        #par['corsbinary'] = "%(topdir)s/bin/corsika.%(model)s.Linux" % par;
        system("cp %(corsbinary)s corsika.%(runnum)s.Linux" % par);

        # Corsika output file 
        par['outfile'] = os.path.join(expandvars(par['outdir']%par),expandvars(par['outfile']%par))
        par['corout']  = "DAT%(runnum)06d" % par;      # Plain CORSIKA output
        par['logfile'] = par['logfile'] % par

        # Execution command
        cors_cmd = "%(tmpdir)s/corsika.%(runnum)d.Linux < %(inputfile)s > %(logfile)s " % par;
        self.logger.info(cors_cmd);

        # Run CORSIKA
        status, output = getstatusoutput(cors_cmd);
        self.logger.info(output);
        if status: 
            os.chdir(cwd);           # cd back to original directory
            raise Exception, "dCorsika python sucks! %s\n" % output;

        # Check if dCORSIKA Output exists
        if os.path.exists("%(tmpdir)s/%(corout)s.gz" % par):
            system("cp %(tmpdir)s/%(corout)s.gz %(outfile)s" % par);
        # Check if CORSIKA Output exists    
        elif os.path.exists("%(tmpdir)s/%(corout)s" % par):
            system("cp %(tmpdir)s/%(corout)s %(outfile)s" % par);
        # if neither output exists, quit
        else:
            os.chdir(cwd);           # cd back to original directory
            print >> sys.stderr,"CORSIKA Output does not exist: exit before UCR\n";
            raise Exception,"CORSIKA Output does not exist: exit before UCR\n";

        

        # check if the output is OK
        nevcorsika = 0
        try:
           status,nevcorsika=getstatusoutput("cat %(logfile)s|grep \"GENERATED EVENTS\"|grep -v NEV|awk '{ print $6 }'" % par);
           nevcorsika = int(nevcorsika.strip());
           stats['nevcorsika'] = nevcorsika
        except Exception,e: 
            self.logger.error(e);
        if nevcorsika == self.GetParameter('nevents'):
            system('touch %(outdir)s/corsika.%(runnum)d.isok' % par) 
            self.logger.info("OK");
            print >> sys.stderr, "Corsika OK\n";
        else :
            system('touch %(outdir)s/corsika.%(runnum)d.isnotok' % par) 
            print >> sys.stderr, "Corsika not OK\n";
            self.logger.error("NOT OK");
            self.logger.error(exe.tail(self.GetParameter('logfile')));
            os.chdir(cwd);       # cd back to original directory
            return 1;

        os.chdir(cwd);           # cd back to original directory
        return 0;

    def configure(self):
        """ 
         Configure and write INPUTS steering file
        """ 
        par   = self.parameters
        seed  = self.GetParameter('seed')
        egs_seed_offset	= self.GetParameter('egs_seed_offset')
	par['seed1'] = int(seed)+0;
        par['seed2'] = int(seed)+1+int(egs_seed_offset);
        par['seed3'] = int(seed)+2;

        # NKG/EGS
        NKGparams = "";
        nkg = 'F'
        egs = 'F'
        if par['donkg']: nkg = 'T'
        if par['doegs']: egs = 'T'
        NKGparams  =    "ELMFLG  %s  %s " % (nkg,egs) 
        NKGparams +=                    "                       em. interaction flags (NKG,EGS)\n";
        if par['donkg']: # if NKG parameterizations ON
           NKGparams += "RADNKG  2.E5                           outer radius for NKG lat.dens.determ.\n";
        par['NKGparams'] = NKGparams 

        # Construct HE interaction model steering commands
        modelStr = "";
        model = self.GetParameter('model')
        if model in ("qgsjet","qgsii"):
          modelStr += "QGSJET  T  0                           use qgsjet for high energy hadrons\n";
          modelStr += "QGSSIG  T                              use qgsjet hadronic cross sections";
        elif model == "dpmjet":
          modelStr += "DPMJET  T  0                           use dpmjet for high energy hadrons\n";
          modelStr += "DPJSIG  T                              all hail Glaubtar!";
        elif model == "sibyll":
          modelStr += "SIBYLL  T  0                           use sibyll for high energy hadrons\n";
          modelStr += "SIBSIG  T                              use sibyll hadronic cross sections";
        elif model == "epos":
          modelStr += "EPOS    T  0                           use epos for high energy hadrons\n";
          modelStr += "EPOSIG  T                              use epos hadronic cross sections\n";
          modelStr += "EPOPAR input epos.param                !initialization input file for epos\n";
          modelStr += "EPOPAR fname inics epos.inics          !initialization input file for epos\n";
          modelStr += "EPOPAR fname iniev epos.iniev          !initialization input file for epos\n";
          modelStr += "EPOPAR fname initl epos.initl          !initialization input file for epos\n";
          modelStr += "EPOPAR fname inirj epos.inirj          !initialization input file for epos\n";
          modelStr += "EPOPAR fname inihy epos.ini1b          !initialization input file for epos\n";
          modelStr += "EPOPAR fname check none                !dummy output file for epos\n";
          modelStr += "EPOPAR fname histo none                !dummy output file for epos\n";
          modelStr += "EPOPAR fname data  none                !dummy output file for epos\n";
          modelStr += "EPOPAR fname copy  none                !dummy output file for epos";

        # Turn on/off dCORSIKA debugging
        if self.GetParameter('debug'):
           par['debug'] = "T";
        else:
           par['debug'] = "F";

        # Check if old-style ecuts parameter is set
        ecuts = self.GetParameter('ecuts')
        if ecuts:
           self.SetParameter('ecuts1',ecuts)
           self.SetParameter('ecuts2',ecuts)
        
        # Convert input phi from IceCube Coordinates to CORSIKA Coordinates
        # CORSIKA will rotate the particles back to IceCube Coordinates in
        # the output routine.
        par['cphmin_cc'] = par['cphmin'] + par['arrang']
        par['cphmax_cc'] = par['cphmax'] + par['arrang']
        
        # Check the domain of min and max phi and fix if needed
        if par['cphmax_cc'] - par['cphmin_cc'] > 360.0:
          self.logger.error('Phi range greater than 360deg')
        if par['cphmin_cc'] < -360.0:
          par['cphmin_cc'] += 360.0
          par['cphmax_cc'] += 360.0
        if par['cphmax_cc'] > 360.0:
          par['cphmin_cc'] -= 360.0
          par['cphmax_cc'] -= 360.0


        # Write out the Corsika INPUTS steering file
        input = ""
        input += "RUNNR   %(runnum)d                        number of run\n";
        input += "EVTNR   1                                 number of first shower event\n";
        input += "NSHOW   %(nevents)d                       number of showers to generate\n";
        input += "PRMPAR  %(crtype)d                        particle type of prim. particle\n";
        input += "ESLOPE  %(eslope)f                        slope of primary energy spectrum\n";
        input += "ERANGE  %(emin)f  %(emax)f                energy range of primary particle\n";
        input += "THETAP  %(cthmin)f  %(cthmax)f            range of zenith angle (degree)\n";
        input += "PHIP    %(cphmin_cc)f  %(cphmax_cc)f            range of azimuth angle (degree)\n";
        input += "SEED    %(seed1)d   0   0                 seed for 1. random number sequence\n";
        input += "SEED    %(seed2)d   0   0                 seed for 2. random number sequence\n";
        input += "SEED    %(seed3)d   0   0                 seed for 3. random number sequence\n";
        input += "OBSLEV  2834.E2                           observation level (in cm)\n";
        input += "%(NKGparams)s";
        input += "ARRANG  %(arrang)f                        rotation of array to north\n";
        input += "FIXHEI  0.  0                             first interaction height & target\n";
        input += "FIXCHI  0.                                starting altitude (g/cm**2)\n";
        input += "MAGNET  16.4  -53.4                       magnetic field south pole\n";
        input += "HADFLG  0  1  0  1  0  2                  flags hadr.interact. & fragmentation\n";
        input += "%s \n" % modelStr;
        input += "ECUTS   %(ecuts1).04f %(ecuts2).04f %(ecuts3).04f %(ecuts4).04f              energy cuts for particles\n";
        input += "MUADDI  T                                 additional info for muons\n";
        input += "MUMULT  T                                 muon multiple scattering angle\n";
        input += "LONGI   F  20.  F  F                      longit.distr. & step size & fit\n";
        input += "MAXPRT  0                                 max. number of printed events\n";
        input += "ECTMAP  100                               cut on gamma factor for printout\n";
        input += "STEPFC  1.0                               mult. scattering step length fact.\n";
        input += "DEBUG   %(debug)s  6  F  1000000          debug flag and log.unit for out\n";
        input += "DIRECT  ./                                output directory\n";
        input += "ATMOD   %(atmod)s                         october atmosphere\n";
        self.steering = input


    def write_steering(self):
        # terminate steering and write it to file
        par   = self.parameters
        self.steering += "EXIT                                      terminates input\n";

        tmpdir    = par['tmpdir']
        inputfile = open(tmpdir+"/INPUTS",'w');
        inputfile.write(self.steering % par);
        par['inputfile'] = inputfile.name;
        inputfile.close();


class dCorsika(Corsika):
    """
    This class provides an interface for preprocessing files in iceprod
    """

    def __init__(self):
        Corsika.__init__(self)
        self.name   = 'dcorsika'
        self.logger = logging.getLogger('iceprod::dCorsika')
        self.parameters['arrang'] = 0.

        self.AddParameter('version','Corsika version','v6720')

        # dcorsika specific
        self.AddParameter('ranpri','CR spectrum: 0=individual nuclei, 1=Wiebel-Sooth, 2=Hoerandel, 3= 5-component',2) 
        self.AddParameter('dslope','CR spectral index modification (only if ranpri=1,2)',0.)  
        self.AddParameter('length','length of generation cylinder in m (for detcfg = length/2*radius calculation)',1400.)  
        self.AddParameter('radius','radius of generation cylinder in m (for detcfg = length/2*radius calculation)',700.) 
        self.AddParameter('depth','depth of the center of IceCube detector in m (for AMANDA it is 1730.)',1950.) 
        self.AddParameter('spric','separate primary energy cutoffs','T') 
        self.AddParameter('pnormH','proton 5-component relative contribution',1.0)
        self.AddParameter('pnormHe','Helium 5-component relative contribution',0.1)
        self.AddParameter('pnormN','Nitrogen 5-component relative contribution',2e-3)
        self.AddParameter('pnormAl','Aluminium 5-component relative contribution',6e-4)
        self.AddParameter('pnormFe','Iron 5-component relative contribution',1e-3)
        self.AddParameter('pgamH','proton 5-component spectral index',2)
        self.AddParameter('pgamHe','Helium 5-component spectral index',2)
        self.AddParameter('pgamN','Nitrogen 5-component spectral index',2)
        self.AddParameter('pgamAl','Aluminium 5-component spectral index',2)
        self.AddParameter('pgamFe','Iron 5-component spectral index',2)


        
    def configure(self):
        Corsika.configure(self)

        par = self.parameters
        # dCorsika specific
        inputd  = "DETCFG  %(detcfg)s                        detector information (l/d)\n";
        inputd += "F2000   T                                 choses F2000 format\n";
        inputd += "LOCUT   T 1.58                            enables skew angle cutoff\n";
        inputd += "RANPRI  %(ranpri)s                        random primary\n";
        inputd += "SPRIC   %(spric)s                          separate primary energy cutoffs\n";
        inputd += "FSEED   F                                 enable random generator seed recovery\n";
        inputd += "DSLOPE  %(dslope)s                        slope correction\n";
        inputd += "SCURV   T 6.4E8 1.95E5                    curved surf., radius of Earth, depth\n";
        inputd += "MFDECL  -27.05                            magnetic field declination (+E, -W)\n";

        if self.GetParameter('ranpri') == 3:
              inputd += "PNORM   %(pnormh)s %(pnormhe)s %(pnormn)s %(pnormal)s %(pnormfe)s      5-component relative contribution\n" 
              inputd += "PGAM    %(pgamh)s %(pgamhe)s %(pgamn)s %(pgamal)s %(pgamfe)s           5-component spectral indices\n" 

        self.steering += inputd
        
        # Write out DETPARAMS geometry file
        tmpdir    = par['tmpdir']
        detparams = open(tmpdir+"/DETPARAMS",'w');
        print >> detparams, "-LENGTH=%(length)s -RADIUS=%(radius)s -DEPTH=%(depth)s" % par
        detparams.close();

        length = self.GetParameter('length')
        radius = self.GetParameter('radius')
        par['detcfg'] = length/(2.*radius);


class ThinCorsika(Corsika):
    """
    This class provides an interface for preprocessing files in iceprod
    """

    def __init__(self):
        Corsika.__init__(self)
        self.name   = 'thincorsika'
        self.logger = logging.getLogger('iceprod::ThinCorsika')

        # ThinCorsika specific
        self.AddParameter('thinem_e','Fraction of primary energy where thinning algorithm is used for electromagnetic particles.',1.0E-6)
        self.AddParameter('thinem_wmax','maximum weight to be given to any thinned electromagnetic particle',10.0)  
        self.AddParameter('thinh_e','Energy(Gev) where thinning algorithm is used for hadrons',1.0)
        self.AddParameter('thinh_wmax','Maximum weight to be given to any thinned hadronic particle',1.0)
        
    def configure(self):
        Corsika.configure(self)

        par = self.parameters
        # Calculate parameters to be used for thinning
        par['efrcthn'] = par['thinem_e']
        par['wmax'] = par['thinem_wmax']
        par['rmax'] = 0.0
        par['thinrat'] = par['thinh_e']/par['thinem_e']
        par['weitrat'] = par['thinh_wmax']/par['thinem_wmax']
        
        inputd  = "THIN  %(efrcthn)f %(wmax)f %(rmax)f       EM thinning level weightmax rmax\n";
        inputd += "THINH  %(thinrat)f %(weitrat)f            Ratios for Hadronic thinning\n";

        self.steering += inputd


class AutoThinCorsika(Corsika):
    """
    This class provides an interface for preprocessing files in iceprod
    """

    def __init__(self):
        Corsika.__init__(self)
        self.name   = 'thincorsika'
        self.logger = logging.getLogger('iceprod::AutoThinCorsika')

        # ThinCorsika specific
        self.AddParameter('thin_method','Method for calculating thinning parameters.','2009')
        
    def configure(self):
        Corsika.configure(self)

        par = self.parameters
        if par['thin_method'] == '2009':
                # Calculate parameters to be used for thinning
                par['efrcthn'] = 1.0E-6
                par['wmax'] = par['emin']*par['efrcthn']	# Calculate optimum weight from Alessio
                if par['wmax'] < 1.0:	# Ensure max weight is at least 1
	            par['wmax'] = 1.0
                par['rmax'] = 0.0
                par['thinrat'] = 10.0/par['efrcthn']		# Just to be safe
                par['weitrat'] = 1.0/par['wmax']
        else:
	        self.logger.error('Specified thinning method not supported')

        inputd  = "THIN  %(efrcthn)f %(wmax)f %(rmax)f       EM thinning level weightmax rmax\n";
        inputd += "THINH  %(thinrat)f %(weitrat)f            Ratios for Hadronic thinning\n";

        self.steering += inputd


class UCR(ipmodule.IPBaseClass):

   def __init__(self):
        ipmodule.IPBaseClass.__init__(self)
        self.logger = logging.getLogger('iceprod::UCR')

        self.AddParameter('ucr-binary','UCR executable','$steering(ucr_binary)')
        self.AddParameter('ucr-opts','UCR options','$steering(ucr_opts1)')
        self.AddParameter('input','UCR input','$steering(ucr_opts1)')
        self.AddParameter('output','UCR output','$steering(ucr_opts1)')


   def Execute(self,stats):
        if not ipmodule.IPBaseClass.Execute(self,stats): return 0

        from commands import getstatusoutput

        ucr_bin   = self.GetParameter('ucr-binary')
        ucr_bin   = self.parser.parse(ucr_bin)

        ucr_opts  = self.GetParameter('ucr-opts')
        ucr_opts  = self.parser.parse(ucr_opts)

        ucr_in    = self.GetParameter('input')
        ucr_in    = self.parser.parse(ucr_in)

        ucr_out   = self.GetParameter('output')
        ucr_out   = self.parser.parse(ucr_out)

        ucr_cmd   = " ".join([ucr_bin,'-out='+ucr_out, ucr_in, ucr_opts])

        os.system("touch %s" % ucr_out) # for some reason ucr gets upset if the file doesn't exits
        # Run UCR 
        self.logger.info(ucr_cmd)
        status, output = getstatusoutput(ucr_cmd);

        self.logger.info(output)
        if status:
           self.logger.error(output)

        return status


