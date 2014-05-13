#!/bin/env python
#
"""
  IceTray connections frame for GtkIcetraConfig application

  copyright  (c) 2005 the icecube collaboration

  @version: $Revision: $
  @date: $Date:  $
  @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import pygtk
pygtk.require("2.0")
import gtk, gobject
from iceprod.core.dataclasses import Parameter
import logging

logger = logging.getLogger('GtkParameterList')

class GtkEditConnection:
    """ The GUI class is the controller for our application """
    def __init__(self,parent,iconfig,connection):

        # setup the main window
        self.parent = parent
        self.iconfig = iconfig
        self.root = gtk.Window(type=gtk.WINDOW_TOPLEVEL)
        self.root.set_title("Connection")
        self.root.set_size_request(310, 180)
        self.table = gtk.Table(2, 4)

        self.vbox = gtk.VBox()
        self.vbox.pack_start(self.table)

#        self.b = gtk.Button('Apply')
        self.b = gtk.Button(stock=gtk.STOCK_APPLY)
        self.b.connect('clicked', self.commit)
        self.hbok = gtk.HButtonBox()
        self.hbok.pack_start(self.b)
        self.vbox.pack_end(self.hbok)

        separator = gtk.HSeparator()
        self.vbox.pack_end(separator)


        inbox  = connection.GetInbox()
        self.inbox  = inbox
        outbox = connection.GetOutbox()
        self.outbox = outbox

        modulemenu1 = gtk.Menu()
        outmodule = outbox.GetModule()
        item1 = self.make_menu_item (outmodule,self.edit_connection,outmodule,outbox) 
        modulemenu1.append(item1)

        modulemenu2 = gtk.Menu()
        inmodule = inbox.GetModule()
        item2 = self.make_menu_item (inmodule,self.edit_connection,inmodule,inbox) 
        modulemenu2.append(item2)

        for module in self.iconfig.GetModules(): 

        	if module.GetName() != outmodule:
        		item1 = self.make_menu_item (module.GetName(), 
						self.edit_connection,module.GetName(),outbox) 
        		modulemenu1.append(item1)

        	if module.GetName() != inmodule:
        		item2 = self.make_menu_item (module.GetName(), 
						self.edit_connection, module.GetName(),inbox) 
        		modulemenu2.append(item2)

        modopt1 = gtk.OptionMenu()
        modopt1.set_menu(modulemenu1)
        modopt2 = gtk.OptionMenu()
        modopt2.set_menu(modulemenu2)

       	from_frame = gtk.Frame()
       	from_label = gtk.Label()
       	from_label.set_markup("<b>From</b>")
       	from_frame.add(from_label)
        self.table.attach(from_frame,0,1,0,1,xpadding=1,ypadding=1)

       	outbox_frame = gtk.Frame()
       	outbox_label = gtk.Label()
       	outbox_label.set_markup("<b>Outbox</b>")
       	outbox_frame.add(outbox_label)
        self.table.attach(outbox_frame,1,2,0,1,xpadding=1,ypadding=1)

        self.table.attach(modopt1,0,1,1,2)
        self.outbox_entry = gtk.Entry(10)
        self.outbox_entry.connect("activate", self.commit)
        self.outbox_entry.set_text(outbox.GetBoxName())
        self.table.attach(self.outbox_entry,1,2,1,2)

       	to_frame = gtk.Frame()
       	to_label = gtk.Label()
       	to_label.set_markup("<b>To</b>")
       	to_frame.add(to_label)
        self.table.attach(to_frame,0,1,2,3,xpadding=1,ypadding=1)

       	inbox_frame = gtk.Frame()
       	inbox_label = gtk.Label()
       	inbox_label.set_markup("<b>Inbox</b>")
       	inbox_frame.add(inbox_label)
        self.table.attach(inbox_frame,1,2,2,3,xpadding=1,ypadding=1)

        self.table.attach(modopt2,0,1,3,4)
        self.inbox_entry = gtk.Entry(10)
        self.inbox_entry.connect("activate", self.commit)
        self.inbox_entry.set_text(inbox.GetBoxName())
        self.table.attach(self.inbox_entry,1,2,3,4)

        # Add our view into the main window
        self.root.add(self.vbox)
        self.root.show_all()
        return

    def make_menu_item(self,name, callback, module,box):
		item = gtk.MenuItem(name)
		item.connect("activate", callback, module,box)
		item.show()
		return item

    def edit_connection( self, widget, module, box):
        """
        Canges the value of the connection
        """
        logger.debug("Change '%s' to '%s'" % (box.GetBoxName(),module))
        box.SetModule(module)

    def commit( self, widget ):
        if  self.iconfig.HasModule(self.inbox.GetModule()) \
		and self.iconfig.HasModule(self.outbox.GetModule()):

        	self.inbox.SetBoxName(self.inbox_entry.get_text())
        	self.outbox.SetBoxName(self.outbox_entry.get_text())
        	print self.inbox.GetBoxName()
        	print self.outbox.GetBoxName()
       		self.parent.showconnections()
       		self.root.destroy()
