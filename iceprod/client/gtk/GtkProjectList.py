#!/bin/env python
#   copyright  (c) 2005
#   the icecube collaboration
#   $Id: $
#
#   @version $Revision: $
#   @date $Date: $
#   @author Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
#	@brief Project display frame for GtkIcetraConfig application
#########################################################################
import pygtk
pygtk.require("2.0")
import gtk, gobject
from iceprod.core.dataclasses import *
import logging

logger = logging.getLogger('GtkProjectList')

class GtkProjectList:
    """ The GUI class is the controller for our application """

    def __init__(self,metaproject,pdb,gtkmod,title="Available Projects"):

        self.metaproject = metaproject
        self.pdb = pdb
        self.gtkmod = gtkmod
        self.printer = None
        self.display()

        # setup the main window
        self.root = gtk.Window(type=gtk.WINDOW_TOPLEVEL)
        self.sw = gtk.ScrolledWindow()
        self.root.set_title(title)
        self.root.set_size_request(600, 475)

        # Get the model and attach it to the view
        self.view = self.make_view( self.store )

        b = gtk.Button(stock=gtk.STOCK_APPLY)
        b.connect('clicked', self.commit_changes)
        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(b, False, False, 1)

        # Add our view into the main window
        self.sw.add(self.view)

        self.vbox = gtk.VBox(False,0)
        self.vbox.pack_start(self.sw,True,True,2)
        self.vbox.pack_end(self.hbbox,False,False,2)
        self.root.add(self.vbox)
        self.root.show_all()
        return

    def SetPrinter(self,printer):
        self.printer = printer

    def SetParent(self,parent):
        self.parent = parent

    def Print(self,txt):
        if self.printer:
			self.printer(txt)
        else:
			print txt


    def display(self):
        """ Sets up and populates gtk.TreeStore """
        self.store = gtk.TreeStore( int, str, str, gobject.TYPE_BOOLEAN )

        if not self.pdb.cached and not self.pdb.connect(): return
        for p in self.pdb.GetProjects(self.metaproject.GetId()):
           	pname = p.GetName()
           	pver  = p.GetVersion() 

           	row = [p.GetId(),pname, pver,False]  
           	self.store.append(None, row )

    def add_project(self, tv, path, viewcol):
		""" 
		 Add Project
		"""
		sel = tv.get_selection()
		sel.select_path(path)
		model, iter = sel.get_selected() 
		id       = model.get_value(iter, 0)
		name     = model.get_value(iter, 1)
		version  = model.get_value(iter, 2)

		newproj = Project()
		newproj.SetName(name)
		newproj.SetVersion(version)

		self.metaproject.AddProject(newproj.GetName(),newproj)
		self.gtkmod.cancel_edit_projects()
		self.root.destroy()

    def commit_changes(self, widget, data=None):
        model = self.store
        self.mdict = {}
        if not self.pdb.cached and not self.pdb.connect(): return
        model.foreach(self.do_change)
        return

    def do_change(self, model, path, iter,connect=True):
		""" 
		 Add Project
		"""
		if model[path][3]:
			id       = model[path][0]
			name     = model[path][1]
			version  = model[path][2]

			newproj = Project()
			newproj.SetName(name)
			newproj.SetVersion(version)

			self.metaproject.AddProject(newproj.GetName(),newproj)
			self.gtkmod.cancel_edit_projects()
			self.root.destroy()


    def row_toggled( self,cell, path):
        """
        Sets the toggled state on the toggle button to true or false.
        """
        model = self.store
        model[path][3] =  not model[path][3] 

        if not model[path][3]:
        	self.renderer[1].set_property('editable', False)
        	self.renderer[2].set_property('editable', False)
        	return


    def make_view( self, model ):
        """ Form a view for the Tree Model """
        self.view = gtk.TreeView( model )
        self.view.connect('row-activated', self.add_project)

        # setup the text cell renderer and allows these
        # cells to be edited.
        self.renderer = [None]*4
        for i in range(3):
        	self.renderer[i] = gtk.CellRendererText()
        self.renderer[3]  = gtk.CellRendererToggle()
        self.renderer[3].set_property('activatable', True)
        self.renderer[3].connect( 'toggled', self.row_toggled)
		
        self.column =  [None]*6
        self.column[0] = gtk.TreeViewColumn("DbID",self.renderer[0], text=0)
        self.column[0].set_sort_column_id(0)
        self.column[1] = gtk.TreeViewColumn("Name",self.renderer[1], text=1)
        self.column[1].set_sort_column_id(1)
        self.column[2] = gtk.TreeViewColumn("version",self.renderer[2], text=2)
        self.column[2].set_sort_column_id(2)
        self.column[3] = gtk.TreeViewColumn('Selected')
        self.column[3].pack_start(self.renderer[3], True)
        self.column[3].add_attribute( self.renderer[3], "active", 3)

        for i in range(4):
        	self.view.append_column( self.column[i] )
        return self.view
