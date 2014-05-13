#!/bin/env python
#   copyright  (c) 2005
#   the icecube collaboration
#   $Id: $
#
#   @version $Revision: $
#   @date $Date: $
#   @author Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
#	@brief icetray connections frame for GtkIcetraConfig application
#########################################################################
import pygtk
pygtk.require("2.0")
import gtk, gobject
from iceprod.core.dataclasses import *
import logging
from GtkModuleList import GtkModuleList 

logger = logging.getLogger('GtkServiceList')

class GtkServiceList(GtkModuleList):
    """ The GUI class is the controller for our application """

    fooclass = 'BarService'

    def __init__(self,iconfig,pdb,gtkmod):
        GtkModuleList.__init__(self,iconfig,pdb,gtkmod,"Available Services")
        return


    def display(self):
        """ Sets up and populates gtk.TreeStore """
        self.store = gtk.TreeStore( int, str, str, str, gobject.TYPE_BOOLEAN )
        row = [0,self.fooname,self.fooclass, self.fooproject,False]  
        self.store.append(None, row )

        if not self.pdb.cached and not self.pdb.connect(): return
        for mp in self.iconfig.GetMetaProjectList():
            for p in self.pdb.GetProjects(mp.GetId()):
            	pname = p.GetName()
            	pver  = p.GetVersion() 

            	services = self.pdb.GetServicesP(pname,pver)
            	for s in services:
            		if not self.iconfig.GetService(s.GetName()):
            			row = [s.GetId(),s.GetName(), s.GetClass(),pname,False]  
            			logger.debug('|'.join(map(str,row)))
            			self.store.append(None, row )

        for p in self.iconfig.GetProjectList():
            pname = p.GetName()
            pver  = p.GetVersion()

            services = self.pdb.GetServicesP(pname,pver)
            for s in services:
            	if not self.iconfig.GetService(s.GetName()):
            		row = [s.GetId(),s.GetName(), s.GetClass(),pname]  
            		logger.debug('|'.join(map(str,row)))
            		self.store.append(None,row )

    def add_service(self, tv, path, viewcol):
		""" 
		 Add Service
		"""
		sel = tv.get_selection()
		sel.select_path(path)
		model, iter = sel.get_selected() 
		id   = model.get_value(iter, 0)
		name  = model.get_value(iter, 1)
		cname  = model.get_value(iter, 2)
	
		proj_name   = model.get_value(iter, 3)
		proj = self.iconfig.GetProject(proj_name)

		newserv = Service()
		newserv.SetName(name)
		newserv.SetClass(cname)

		if not self.pdb.cached and not self.pdb.connect(): return
		proj = self.iconfig.GetProject(proj_name)
		if not proj: 
			for mp in self.iconfig.GetMetaProjectList():
				for p in self.pdb.GetProjectsSM(newserv,mp):
					mp.AddProject(p.GetName(),p)
					if p.GetName() == proj_name: proj = p
		if not proj: 
			raise Exception, "Could not find project for '%s'"% newserv.GetName()
		newserv.AddProject(proj_name,proj)

		parameters = self.pdb.GetParameters(int(id))
		for p in parameters:
			param = Parameter()
			param.SetName(p.GetName())
			param.SetType(p.GetType())
			param.SetDescription(p.GetDescription())
			param.SetValue(p.GetValue())
			newserv.AddParameter(param)

		self.iconfig.AddService(newserv)
		self.gtkmod.showservices()
		self.root.destroy()

    def commit_changes(self, widget, data=None):
        model = self.store
        self.mdict = {}
        if not self.pdb.cached and not self.pdb.connect(): return
        model.foreach(self.do_change)
        return

    def do_change(self, model, path, iter,connect=True):
		""" 
		 Add Service
		"""
		if model[path][4]:
			id     = model[path][0]
			name   = model[path][1]
			cname  = model[path][2]

			proj_name  = model[path][3]
			proj = self.iconfig.GetProject(proj_name)

			newserv = Service()
			newserv.SetName(name)
			newserv.SetClass(cname)

			proj = self.iconfig.GetProject(proj_name)
			if not proj: 
				for mp in self.iconfig.GetMetaProjectList():
					for p in self.pdb.GetProjectsSM(newserv,mp):
						mp.AddProject(p.GetName(),p)
						if p.GetName() == proj_name: proj = p

					if not proj: # search all projects in mp to match name
						for p in self.pdb.GetProjects(mp.GetId()):
							if p.GetName() == proj_name: 
								mp.AddProject(p.GetName(),p)
								proj = p
								break
			if not proj: 
				raise Exception, "Could not find project for '%s'" % newserv.GetName()
			newserv.AddProject(proj_name,proj)

			parameters = self.pdb.GetParameters(int(id))
			for p in parameters:
				param = Parameter()
				param.SetName(p.GetName())
				param.SetType(p.GetType())
				param.SetDescription(p.GetDescription())
				param.SetValue(p.GetValue())
				newserv.AddParameter(param)

			self.iconfig.AddService(newserv)
			self.gtkmod.showservices()
			self.root.destroy()


    def make_view( self, model ):
        """ Form a view for the Tree Model """
        self.view = gtk.TreeView( model )
        self.view.connect('row-activated', self.add_service)

        # setup the text cell renderer and allows these
        # cells to be edited.
        self.renderer = [None]*5
        for i in range(4):
        	self.renderer[i] = gtk.CellRendererText()
        self.renderer[4]  = gtk.CellRendererToggle()
        self.renderer[4].set_property('activatable', True)
        self.renderer[4].connect( 'toggled', self.row_toggled)
		
        self.column =  [None]*5
        self.column[0] = gtk.TreeViewColumn("DbID",self.renderer[0], text=0)
        self.column[0].set_sort_column_id(0)
        self.column[1] = gtk.TreeViewColumn("Default Name",self.renderer[1], text=1)
        self.column[1].set_sort_column_id(1)
        self.column[2] = gtk.TreeViewColumn("Class",self.renderer[2], text=2)
        self.column[2].set_sort_column_id(2)
        self.column[3] = gtk.TreeViewColumn("Project",self.renderer[3],text=3 )
        self.column[4] = gtk.TreeViewColumn('Selected')
        self.column[4].pack_start(self.renderer[4], True)
        self.column[4].add_attribute( self.renderer[4], "active", 4)

        for i in range(5):
        	self.view.append_column( self.column[i] )
        return self.view
