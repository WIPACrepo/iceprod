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

from iceprod.core.dataclasses import *
import pygtk
pygtk.require('2.0')
import gtk, gobject
import logging
from GtkIPModule import *

logger = logging.getLogger('GtkProjectEditor')
NotConnectedException = 'Not connected'


class GtkProjectEditor(GtkIPModule):

    def get_iconfig(self):
        return self.icetray

    def __init__(self,icetray,db):
        # create a liststore with three int columns
        gtk.VBox.__init__(self)
        self.icetray= icetray
        self.mdict = {}
        self.pdict = {}
        self.db = db

        self.textbuffer = None
        self.tree_store = gtk.TreeStore( int,str,str,gobject.TYPE_BOOLEAN )
        self.metaproject_dict = { }

        if not db.cached and not db.connect(): 
			raise NotConnectedException, 'Could not connect to database'
        self.rownum = 0
        self.rows = {}
        for mrow in db.GetMetaProjects():
        	mid    = mrow.GetId()
        	mpname = mrow.GetName()
        	mver   = mrow.GetVersion()
        	mselected  = False
        		
        	metaproj = self.get_iconfig().GetMetaProject(mpname)
        	if metaproj and metaproj.GetVersion() == mrow.GetVersion():
				mselected  = not mselected

        	metadict = [ mid, mpname, mver, mselected ]

        	projdict = {}
        	for prow in db.GetProjects(mid):
        		pid   = prow.GetId()
        		pname = prow.GetName()
        		ver   = prow.GetVersion()
        		selected  = False
        		
        		if mselected:
        			proj = self.get_iconfig().GetProject(pname)
        			if proj and proj.GetVersion() == prow.GetVersion():
					selected  = not selected
					self.pdict[proj.GetName()] = selected

        		projdict[pid]=[pid,pname,ver[0],ver[1],ver[2],selected]
        	self.rows[mid] = {
					'metaproject':metadict,'projects':projdict
			}
        if not db.cached: db.disconnect(); 
        
        self.sw = gtk.ScrolledWindow()

        # Set sort column
        self.tv = gtk.TreeView(self.tree_store)
        self.pack_start(self.sw)
        self.sw.add(self.tv)
        self.tv.column = [None]*7
        self.tv.column[0] = gtk.TreeViewColumn('DbID')
        self.tv.column[1] = gtk.TreeViewColumn('Meta-Project')
        self.tv.column[2] = gtk.TreeViewColumn('version')
        self.tv.column[3] = gtk.TreeViewColumn('Selected')
        self.tv.cell = [None]*7

        self.selection = self.tv.get_selection()
        self.selection.connect('changed', self.on_selection_changed)

        for i in range(3):
            self.tv.cell[i] = gtk.CellRendererText()
            self.tv.append_column(self.tv.column[i])
            self.tv.column[i].set_sort_column_id(i)
            self.tv.column[i].pack_start(self.tv.cell[i], True)
            self.tv.column[i].set_attributes(self.tv.cell[i], text=i)

        self.tv.cell[3] = gtk.CellRendererToggle()
        self.tv.cell[3].set_property('activatable', True)
        self.tv.cell[3].connect( 'toggled', self.row_toggled)
        self.tv.append_column(self.tv.column[3])
        self.tv.column[3].pack_start(self.tv.cell[3], True)
        self.tv.column[3].add_attribute( self.tv.cell[3], "active", 3)

        self.show_all()
        
        
        for m in self.rows.keys():
            metaproject = self.rows[m]['metaproject']
            projects = self.rows[m]['projects']
            parent = self.tree_store.append( None, metaproject )

    def set_text_buffer(self,textbuffer): 
		self.textbuffer = textbuffer

    def write_text_buffer(self,text): 
		if self.textbuffer:
			self.textbuffer.set_text(text)
		else:
			print text

    def on_selection_changed(self,selection): 
		model, iter = selection.get_selected() 
		id	    = model.get_value(iter, 0)
		name    = model.get_value(iter, 1)
		version = model.get_value(iter, 2)
		logger.info("selected(%d): %s.%s" % (id,name,version))

    def adddependencies(self, project, mp):
		"""
		Recursively add project and its dependencies to metaproject
		"""
		for dep in self.db.GetProjectDependencies(project.GetId(),mp.GetId()):
			if not mp.HasProject(project.GetName()):
				mp.AddProject(project.GetName(),project)
				project.AddDependency(dep)
				self.adddependencies(dep,mp)


    def getmetaproject(self, model, path):
		"""
		Add selected metaproject to configuration. Import modules and their
		parameters.
		"""
		# Create a new metaproject and set version
		newmp = MetaProject()
		newmp.SetId( model[path][0] )
		newmp.SetName( model[path][1] )
		newmp.SetVersion(model[path][2])

		# Create a temporary configuration
		new_config = IceTrayConfig()
		new_config.AddMetaProject(newmp.GetName(),newmp)

		# Get all configured modules in previous configuration
		for oldmod in self.get_iconfig().GetModules():
			for project in self.db.GetProjectsMM(oldmod,newmp): 
			    newmp.AddProject(project.GetName(),project)
			    self.adddependencies(project, newmp)

		# Get all configured services in previous configuration
		for oldserv in self.get_iconfig().GetServices():
			for project in self.db.GetProjectsMM(oldserv,newmp): 
			    newmp.AddProject(project.GetName(),project)
			    self.adddependencies(project, newmp)

		# Get missing projects and dependencies
		if self.get_iconfig().HasMetaProject(newmp.GetName()):
			oldmp = self.get_iconfig().GetMetaProject(newmp.GetName())
			for p in self.db.GetProjects(newmp.GetId()): 
				if oldmp.HasProject(p.GetName()) and not newmp.HasProject(p.GetName()):
					newmp.AddProject(p.GetName(),p)

		# Loop over modules in old configuration and fetch their projects
		for oldmod in self.get_iconfig().GetModules():
			if not new_config.HasModule(oldmod.GetName()):
				depend = self.db.GetProjectsMM(oldmod,newmp)
				if not depend:
					oldmod.SetDescription( 'class %s not found in metaproject %s %s' %(
											oldmod.GetClass(),
											newmp.GetName(),
											newmp.GetVersion()))
				else:
					oldmod.AddProject(depend[0].GetName(),depend[0])
				new_config.AddModule(oldmod)
			
		# Loop over services in old configuration and fetch their projects
		for oldserv in self.get_iconfig().GetServices():
			if not new_config.HasService(oldserv.GetName()):
				depend = self.db.GetProjectsMM(oldserv,newmp)
				if not depend:
					oldserv.SetDescription('class %s not found for project %s %s' %(
											oldserv.GetClass(),
											newmp.GetName(),
											newmp.GetVersion()))
				else:
					oldserv.AddProject(depend[0].GetName(),depend[0])
				new_config.AddService(oldserv)
		
		# Overwrite metaproject
		self.get_iconfig().AddMetaProject(newmp.GetName(),newmp)

		# Overwrite modules
		for newmod in new_config.GetModules():
			self.get_iconfig().AddModule(newmod)

		# Overwrite services
		for newserv in new_config.GetServices():
			self.get_iconfig().AddService(newserv)

		self.mdict[newmp.GetId()] = newmp
		return newmp


    def do_change(self, model, path, iter,connect=True):
        if not self.db.cached and \
				not self.db.connect():
				return

        if model[path][3]:
        	self.getmetaproject(model,path)
        	self.madd[ model[path][1] ]  = True

        else: 
        	self.mdelete[ model[path][1] ]  = True
        return

    def commit_changes(self, widget, data=None):
        self.madd = {}
        self.mdelete = {}
        model = self.tree_store
        model.foreach(self.do_change)
        
        # delete any metaproject that is not being added or overwritten
        for mp in self.mdelete.keys():
				if not self.madd.has_key(mp):
					self.get_iconfig().RemoveMetaProject(mp)
        return

    def toggle_group(self,model, iter,bval):
        """
        Toggle parent (metaproject) and children (individual projects)
        """
       	while iter:
       		cpath   =  model.get_path(iter)
       		project =  model[cpath][1]
       		model[cpath][3] = bval
       		if self.pdict.has_key(project): 
       			self.pdict[project] = bval
       		iter =  model.iter_next(iter)

    def row_toggled( self,cell, path):
        """
        Sets the toggled state on the toggle button to true or false.
        """
        model = self.tree_store
        iter = model.get_iter(path)
        value_to_set = not model[path][3] 
        self.toggle_group(model,model.get_iter_first(),False)
        if model.iter_has_child(iter): # This is a meta project
        	# check all projects in this metaproject
        	chiter =  model.iter_children(iter)
        	self.toggle_group(model,chiter,value_to_set)
        model[path][3] = value_to_set
        self.pdict[model[path][1]] = value_to_set
        
