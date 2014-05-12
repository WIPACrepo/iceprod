#! /usr/bin/env python
"""
  daemon for merging ROOT histograms and generating graphs for iceprod datasets

  copyright (c) 2007 the icecube collaboration

  @version: $Revision: $
  @date: $Date: $
  @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import sys,os
import time
import os.path
import getopt
import glob
import signal
from ROOT import *
import logging
from iceprod.core.metadata import *
from iceprod.core.xmlwriter import IceTrayXMLWriter
from os.path import expandvars

import iceprod.core.logger
logger = logging.getLogger('Histos')

def makehistos( argv , outputpath ,dataset,exphisto=None):

  targetName = os.path.join(outputpath,'soaphisto_%u.root' % dataset)
  logger.info("Target file: %s" % targetName)
  Target = TFile.Open( targetName, "RECREATE" );

  Merge( Target, argv);
  Draw( Target, outputpath ,exphisto);

# Merge all files from sourcelist into the target directory.
def Merge( target, sourcelist ):

  path = "/"
  first_source =  TFile.Open( sourcelist[0] ) ;

  # normalization constant
  norm = float(len(sourcelist))

  # loop over all keys in directory
  for key in gDirectory.GetListOfKeys():

    # read object 
    first_source.cd( path );
    obj = key.ReadObj();

    if  obj.IsA().InheritsFrom( "TH1" ):
      # merge TH1 object
      h1 = obj;

      # loop over all source files and merge histogram
      for next in sourcelist:
	    nextsource =  TFile.Open( next ) ;
	    nextsource.cd( path );
	    h2 = gDirectory.Get( h1.GetName() );
	    if  h2:
	        h1.Add( h2 );
	        del h2;
	    nextsource.Close();
      h1.Scale(1.0/norm);

    elif  obj.IsA().InheritsFrom( "TDirectory" ):
      logger.debug("Found subdirectory %s" % obj.GetName())
      # create a new subdir of same name and title in the target file
      target.cd();
      newdir = target.mkdir( obj.GetName(), obj.GetTitle() );

      # Recursively merge subdirectories
      MergeRootfile( newdir, sourcelist );

    else:
      # object is of no type that we know or can handle
      logger.warn("Unknown object type, name: %s , title: %s" %
	     (obj.GetName(), obj.GetTitle()))

    # now write the merged histogram 
    if  obj: 
      target.cd();
      obj.Write( key.GetName() );

  first_source.Close()

def Draw( target ,outputpath, exphisto=None):

  path = "/"
  expfile =  TFile.Open( exphisto ) ;

  # loop over all keys in this directory
  for key in target.GetListOfKeys():

    # read object from first source file
    obj = key.ReadObj();

    if  obj.IsA().InheritsFrom( "TH1" ):
      # Draw histogram
      c1 = TCanvas("c1", "c1",123,71,699,499);
      h1 = obj;
      h1.SetLineColor(4);
      h1.SetLineWidth(1);
      h2 = None;

      expname = h1.GetName().strip();
      logger.debug(expname)
      if exphisto: 
          k2 = expfile.FindKey(expname)
          if k2:
            h2 = k2.ReadObj();
            h2.SetLineColor(2);
            h2.SetLineWidth(1);

      c1.SetBorderSize(2);
      c1.SetLogy(0);
      h1.Draw();

      if h2: 
          h2.Draw("same");
      c1.SaveAs(os.path.join(outputpath,h1.GetName()+".gif"));

      c1.SetLogy(1);
      h1.Draw();
      if h2:
          h2.Draw("same");
      c1.SaveAs(os.path.join(outputpath,h1.GetName()+"_log.gif"));

    elif  obj.IsA().InheritsFrom( "TDirectory" ):
      logger.debug("Found subdirectory %s"% obj.GetName())
      target.cd();
      newdir = target.mkdir( obj.GetName(), obj.GetTitle() );

      # Draw histos from current directory
      Draw( newdir );

    else:
      logger.warn("Unknown object type, name: %s Title: %s"% (obj.GetName(), obj.GetTitle()))


class Histos:
	def __init__(self,cfg,i3db):
		self.cfg = cfg
		self.i3db = i3db
		self.resources = os.path.join(cfg.get('path','basedir'),"shared")
		self.exphistos = cfg.get('path','exp_histos',raw=True)


	def MakeHistos(self,set,basepath,histopath,pattern="histo*.root"):
		self.i3db.connect()
		dataset = set["dataset_id"]
		logger.info('getting path info for dataset %d ' % dataset)
		path = basepath % set
		histo_path = histopath % set
		exp_histos = expandvars(self.exphistos % set)
		logger.debug("exp_histos %s" % exp_histos)

		# Make sure exp histos exist
		if not os.path.exists(exp_histos):
		    exp_histos = None

		if os.path.exists(path):
		    # check if directory exists #os.makedirs(path)
		    logger.info("generating histograms in %s" % path)

		    logger.info('looking in %s/*/%s' % (path,pattern))
		    rootfilelist = glob.glob('%s/*/%s' % (path,pattern))
		    if len(rootfilelist) > 0:
			    if not os.path.exists(histo_path): 
			        os.makedirs(histo_path)
			    os.system("cp %s %s/index.php" % (os.path.join(self.resources,'histo.php'), histo_path))
			    os.system("cp %s %s/" % (os.path.join(self.resources,'histo.css'), histo_path ))
			    makehistos(rootfilelist,histo_path,dataset,exp_histos)
			    self.i3db.AddedHisto(dataset)
		    else:
			    logger.info('No root files found for dataset %d' % dataset)
		else:
			logger.error('%s path does not exist' % path)
		return


