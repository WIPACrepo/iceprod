#!/bin/env python
#

"""
 Display window for Configuration database

 copyright (c) 2005 the icecube collaboration

 @version: $Revision: $
 @date: $Date: $
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import pygtk
pygtk.require("2.0")
import gtk, gobject
from iceprod.core.dataclasses import *
import time
import logging
import iceprod.core.logger

logger = logging.getLogger('GtkMetaProjectList')

class GtkMetaProjectList:
    def __init__(self,pdb,parent):

        self.store = gtk.ListStore( int, str, str )
        self.pdb = pdb
        self.parent = parent
        self.resume_action = None
        self.init()

    def init(self):
        if not self.display(): return

        # setup the main window
        self.root = gtk.Window(type=gtk.WINDOW_TOPLEVEL)
        self.sw = gtk.ScrolledWindow()
        self.root.set_title('Available Metaprojects')
        self.root.set_size_request(500, 375)

        # Get the model and attach it to the view
        self.view = self.make_view( self.store )

        self.vbox = gtk.VBox(False,1)

        # Add search field
        self.entry = gtk.Entry()
        self.entry.set_max_length(50)
        self.entry.connect("activate", self.enter_callback, self.entry)
        self.entry_frame = gtk.Frame('Search keyword')
        self.entry_frame.add(self.entry)
        self.vbox.pack_start(self.entry_frame, False, False, 1)
        self.entry.show()

        # Add view into frame and into main window
        self.sw.add(self.view)
        self.view_frame = gtk.Frame('Database entries')
        self.view_frame.add(self.sw)
        self.vbox.add(self.view_frame)

        self.root.add(self.vbox)
        self.root.show_all()
        return

    def enter_callback(self, widget, entry):
        entry_text = entry.get_text()
        self.display(entry_text)

    def set_resume_action(self,action):
        self.resume_action = action

    def resume_action_callback(self):
        if self.resume_action:
        	self.resume_action()
        self.resume_action = None

    def display(self,search_string=None):
        """ Populates gtk.TreeStore """

        self.store.clear()
			
        self.set_resume_action(self.init)
        self.pdb.connect()
        runs =  self.pdb.fetch_metaproject_list()
        for r in runs:
           	row = [ r['metaproject_id'], r['name'], r['versiontxt'] ]  
           	logger.debug(','.join(map(str,row)))
           	self.store.append( row )
        return True

    def switcharoo(self, tv, path, viewcol):
		""" 
		 Get config from database server
		"""
		sel = tv.get_selection()
		sel.select_path(path)
		model, iter = sel.get_selected() 
		id      = model.get_value(iter, 0)
		name    = model.get_value(iter, 1)
		version = model.get_value(iter, 2)

		self.set_resume_action(self.init)
		self.pdb.connect() 
		steering = self.parent.GetSteering()
		traylist = steering.GetTrays()
		for tray in traylist:
		    if tray.HasMetaProject(name):
		       mp = self.pdb.SwitchMetaProject(tray,id,name,version)
		       tray.AddMetaProject(mp.GetName(),mp)
		self.parent.ReloadWidgets()
		self.root.destroy()


    def make_view( self, model ):
        """ Form a view for the Tree Model """
        self.view = gtk.TreeView( model )
        self.view.connect('row-activated', self.switcharoo)

        # setup the text cell renderer and allows these
        # cells to be edited.
        self.renderer = [None]*5
        for i in range(5):
        	self.renderer[i] = gtk.CellRendererText()
		
        self.column =  [None]*3
        self.column[0] = gtk.TreeViewColumn("ID",self.renderer[0], text=0)
        self.column[1] = gtk.TreeViewColumn("name",self.renderer[1],text=1 )
        self.column[2] = gtk.TreeViewColumn("version",self.renderer[2], text=2)

        for i in range(3):
        	self.view.append_column( self.column[i] )
        return self.view

