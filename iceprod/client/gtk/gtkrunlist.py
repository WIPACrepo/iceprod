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

logger = logging.getLogger('GtkRunList')

class GtkRunList:
    def __init__(self,cdb,parent):

        self.store = gtk.ListStore( int, str, str, str, str)
        self.cdb = cdb
        self.parent = parent
        self.resume_action = None
        self.init()

    def init(self):
        if not self.display(): return

        # setup the main window
        self.root = gtk.Window(type=gtk.WINDOW_TOPLEVEL)
        self.sw = gtk.ScrolledWindow()
        self.root.set_title('Configuration Database')
        self.root.set_size_request(800, 375)

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

    def display(self,search_string=""):
        """ Populates gtk.TreeStore """

        self.store.clear()
        self.set_resume_action(self.init)
        runs =  self.cdb.show_dataset_table(search_string)
        for r in runs:
           	row = [
				r['dataset_id'],
				r['startdate'],
				r['username'],
				r['description'],
				r['hostname'],
				]  
           	logger.debug(','.join(map(str,row)))
           	self.store.append( row )
        return True

    def get_config(self, tv, path, viewcol):
		""" 
		 Get config from database server
		"""
		sel = tv.get_selection()
		sel.select_path(path)
		model, iter = sel.get_selected() 
		id     = model.get_value(iter, 0)
		name   = model.get_value(iter, 1)

		self.set_resume_action(self.init)
		self.parent.configfile = "dataset.%d.xml" % int(id)
		self.parent.CloseConfig()
		self.parent.LoadConfig( self.cdb.download_config( int(id),self.parent.showdefaults,self.parent.showdefaults) )
		self.root.destroy()


    def make_view( self, model ):
        """ Form a view for the Tree Model """
        self.view = gtk.TreeView( model )
        self.view.connect('row-activated', self.get_config)

        # setup the text cell renderer and allows these
        # cells to be edited.
        self.renderer = [None]*5
        for i in range(5):
        	self.renderer[i] = gtk.CellRendererText()
		
        self.column =  [None]*5
        self.column[0] = gtk.TreeViewColumn("run ID",self.renderer[0], text=0)
        self.column[1] = gtk.TreeViewColumn("date",self.renderer[1],text=1 )
        self.column[2] = gtk.TreeViewColumn("user",self.renderer[2], text=2)
        self.column[3] = gtk.TreeViewColumn("description",self.renderer[3],text=3 )
        self.column[4] = gtk.TreeViewColumn("host",self.renderer[4], text=4)

        for i in range(5):
        	self.view.append_column( self.column[i] )
        return self.view

    def cdbauth(self,db):

		auth_dialog = gtk.Dialog(title='authentication', parent=None, flags=0 );

		username_label = gtk.Label("Username:")
		username_label.show()

		username_entry = gtk.Entry()
		username_entry.set_text(self.parent._getproduser()) 
		username_entry.show()

		password_label = gtk.Label("Password:")
		password_label.show()

		password_entry = gtk.Entry()
		password_entry.set_visibility(False)
		password_entry.show()

		server_label = gtk.Label("Server:")
		server_entry = gtk.Entry()
		if self.parent._getprodserver(): 
			server_entry.set_text(self.parent._getprodserver()) 
		server_label.show()
		server_entry.show()

		database_label = gtk.Label("Database:")
		database_entry = gtk.Entry()
		if self.parent._getproddb():
			database_entry.set_text(self.parent._getproddb()) 
		database_label.show()
		database_entry.show()

		cancel_button = gtk.Button('Cancel')
		cancel_button.show()
		cancel_button.connect("clicked", lambda widget: auth_dialog.destroy())

		submit_button = gtk.Button('OK')
		submit_button.show()
		authfunc = lambda x: (db.authenticate(
						self.parent._setprodserver(server_entry.get_text()),
						self.parent._setproduser(username_entry.get_text()), 
						self.parent._setprodpass(password_entry.get_text()),
						self.parent._setproddb(database_entry.get_text()),
						True), auth_dialog.destroy(),self.resume_action_callback() )

		username_entry.connect("activate", authfunc)
		password_entry.connect("activate", authfunc)
		server_entry.connect("activate", authfunc)
		database_entry.connect("activate", authfunc)
		submit_button.connect("clicked", authfunc)

		auth_dialog.vbox.pack_start(username_label, True, True, 0)
		auth_dialog.vbox.pack_start(username_entry, True, True, 0)
		auth_dialog.vbox.pack_start(password_label, True, True, 0)
		auth_dialog.vbox.pack_start(password_entry, True, True, 0)
		auth_dialog.vbox.pack_start(server_label, True, True, 0)
		auth_dialog.vbox.pack_start(server_entry, True, True, 0)
		auth_dialog.vbox.pack_start(database_label, True, True, 0)
		auth_dialog.vbox.pack_start(database_entry, True, True, 0)
		auth_dialog.action_area.pack_start(cancel_button, True, True, 0)
		auth_dialog.action_area.pack_start(submit_button, True, True, 0)
		auth_dialog.show()
