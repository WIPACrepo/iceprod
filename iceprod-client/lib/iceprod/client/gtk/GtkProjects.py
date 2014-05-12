#!/bin/env python
#   copyright  (c) 2005
#   the icecube collaboration
#   $Id: $
#
#   @version $Revision: $
#   @date $Date: $
#   @author Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
#	@brief Project Frame for GtkIcetraConfig application
#########################################################################
from iceprod.core.dataclasses import Project
from GtkProjectDisplay import GtkProjectDisplay
from GtkProjectEditor import *
from GtkProjectList import GtkProjectList 
import sys
import pygtk
pygtk.require('2.0')
import gtk, gobject
from GtkIPModule import *

class GtkProjects(GtkIPModule):

    def commit_changes(self, b):
        if self.box:
        	self.box.commit_changes(None,None)
        	self.box.destroy()
        if self.hbbox:
        	self.hbbox.destroy()
        
        self.edit = not self.edit
        self.box = GtkProjectDisplay(self.icetray,self.db)
        self.box.set_text_buffer(self.textbuffer)
        self.pack_start(self.box)
        b0 = gtk.Button('Edit')
        b0.connect('clicked', self.edit_projects)

        b1 = gtk.Button('Add Project',stock=gtk.STOCK_ADD)
        b1.connect('clicked', self.add_project, self.box )

        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(b0, False, False, 1)
#        self.hbbox.pack_start(b1, False, False, 1)

        self.pack_start(self.hbbox, False)
        self.show_all()

    def cancel_edit_projects(self, b):
        if self.box:
        	self.box.destroy()
        if self.hbbox:
        	self.hbbox.destroy()

        self.edit = not self.edit
        self.box = GtkProjectDisplay(self.icetray,self.db)
        self.box.set_text_buffer(self.textbuffer)


        self.pack_start(self.box)

        b0 = gtk.Button('Edit')
        b0.connect('clicked', self.edit_projects)
        b1 = gtk.Button('Add Project',stock=gtk.STOCK_ADD)
        b1.connect('clicked', self.add_project, self.box)

        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(b0, False, False, 1)
#        self.hbbox.pack_start(b1, False, False, 1)

        self.pack_start(self.hbbox, False)
        self.show_all()

    def edit_projects(self, b):
        if self.box:
        	self.box.destroy()
        if self.hbbox:
        	self.hbbox.destroy()

        try:
        	self.edit = not self.edit
        	self.box = GtkProjectEditor(self.icetray,self.db)
        	self.box.set_text_buffer(self.textbuffer)
        	self.pack_start(self.box)

        	b0 = gtk.Button('Cancel')
        	b0.connect('clicked', self.cancel_edit_projects)

        	b1 = gtk.Button('Commit Changes')
        	b1.connect('clicked', self.commit_changes)

        	self.hbbox = gtk.HButtonBox()
        	self.hbbox.pack_start(b0, False, False, 1)
        	self.hbbox.pack_start(b1, False, False, 1)
        	self.pack_start(self.hbbox, False)
        	self.show_all()
        except NotConnectedException, except_msg:
        	self.cancel_edit_projects(b)
        
    def SetPrinter(self,printer):
		self.printer = printer

    def set_text_buffer(self,textbuffer): 
		self.textbuffer = textbuffer

    def reload(self,icetray): 
        self.icetray = icetray
        self.cancel_edit_projects(None)

    def write_text_buffer(self,text): 
		if self.textbuffer:
			self.textbuffer.set_text(text)
		else:
			self.printer(str(text))

    def basic_printer(self,text): 
		print text

    def __init__(self,icetray,db):
        # create a liststore with three int columns
        gtk.VBox.__init__(self)
        self.icetray = icetray
        self.box = None
        self.hbbox = None
        self.edit = False
        self.db = db
        self.textbuffer = None
        self.printer = self.basic_printer
        self.cancel_edit_projects(None)

    def add_project(self, b, pjdisplay):
		metaproject = pjdisplay.GetSelected()
		if metaproject.GetId() > 0:
			self.plist = GtkProjectList(metaproject,self.db,self)
			self.plist.SetPrinter(self.printer)
