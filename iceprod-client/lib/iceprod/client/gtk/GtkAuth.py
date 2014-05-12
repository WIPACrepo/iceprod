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
import threading
import getpass
pygtk.require('2.0')
import gtk


class GtkAuth:

	def __init__(self,submit,cancel=None):

		self.dialog = gtk.Dialog(title='authentication', parent=None, flags=0 );

		username_label = gtk.Label("Username:")
		username_label.show()

		username_entry = gtk.Entry()
		username_entry.set_text(getpass.getuser()) 
		username_entry.show()
		username_entry.connect("activate", self.go,submit)
		self.username_entry = username_entry

		password_label = gtk.Label("Password:")
		password_label.show()

		password_entry = gtk.Entry()
		password_entry.set_visibility(False)
		password_entry.show()
		password_entry.connect("activate", self.go,submit)
		self.password_entry = password_entry

		host_label = gtk.Label("Host:")
		host_label.show()

		host_entry = gtk.Entry()
		host_entry.set_text(getpass.getuser()) 
		host_entry.show()
		host_entry.connect("activate", self.go,submit)
		self.host_entry = host_entry

		database_label = gtk.Label("Database:")
		database_label.show()

		database_entry = gtk.Entry()
		database_entry.set_text(getpass.getuser()) 
		database_entry.show()
		database_entry.connect("activate", self.go,submit)
		self.database_entry = database_entry

		cancel_button = gtk.Button('Cancel')
		cancel_button.show()
		cancel_button.connect("clicked", self.bail,cancel)


		submit_button = gtk.Button('Submit')
		submit_button.show()
		submit_button.connect("clicked", self.go,submit)


		self.dialog.vbox.pack_start(username_label, True, True, 0)
		self.dialog.vbox.pack_start(username_entry, True, True, 0)
		self.dialog.vbox.pack_start(password_label, True, True, 0)
		self.dialog.vbox.pack_start(password_entry, True, True, 0)
		self.dialog.vbox.pack_start(host_label, True, True, 0)
		self.dialog.vbox.pack_start(host_entry, True, True, 0)
		self.dialog.vbox.pack_start(database_label, True, True, 0)
		self.dialog.vbox.pack_start(database_entry, True, True, 0)
		self.dialog.action_area.pack_start(cancel_button, True, True, 0)
		self.dialog.action_area.pack_start(submit_button, True, True, 0)
		self.dialog.show()

	def go(self,widget,submit):
		submit( 
				self.host_entry.get_text(),
				self.username_entry.get_text(),
				self.username_entry.get_text(),
				self.password_entry.get_text(),
				self.database_entry.get_text())
		self.dialog.destroy()

	def bail(self,widget,cancel):
		if cancel:
			cancel()
		self.dialog.destroy()



def echo(*str):
	print str

#gtkauth = GtkAuth(echo)
#gtk.main()
