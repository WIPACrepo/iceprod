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
pygtk.require('2.0')
import gtk
import logging
from iceprod.core.dataclasses import *
from GtkIPModule import *

logger = logging.getLogger('soaptray')

class GtkSteerDepend(GtkIPModule):

    def on_selection_changed(self,selection): 
		try: 
			model, iter = selection.get_selected() 
			name     = model.get_value(iter, 0)
			logger.debug("selected dependency: %s" % name)
		except: 
			None


    def delete_dependency(self, b):
		sel = self.tv.get_selection()
		model, iter = sel.get_selected() 
		path = model.get_path(iter)
		depend = model.get_value(iter, 0)
		self.steering.RemoveDependency(depend)
		model.remove(iter)

    def add_dependency(self, b):
	  	"""
		add steering dependency file
		"""
		depend = 'new_dependency.tar'
		self.steering.AddDependency(depend)
	  	row = [depend]

	  	i0 = self.liststore.append(row)
		sel = self.tv.get_selection()
		sel.select_iter(i0)
		
		

    def __init__(self,steering):
        self.steering = steering
        gtk.VBox.__init__(self)

        # create a liststore with three int columns
        self.liststore = gtk.ListStore(str)
        self.sw = gtk.ScrolledWindow()

        # Set sort column
        self.tv = gtk.TreeView(self.liststore)
        self.pack_start(self.sw)

        self.b0 = gtk.Button('Add Dependency')
        self.b1 = gtk.Button('Delete Dependency')
        self.b0.connect('clicked', self.add_dependency)
        self.b1.connect('clicked', self.delete_dependency)

        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(self.b0, False, False, 1)
        self.hbbox.pack_start(self.b1, False, False, 1)

        self.pack_start(self.hbbox, False)
        self.sw.add(self.tv)

        self.tv.column = gtk.TreeViewColumn('FileName')
        self.selection = self.tv.get_selection()
        self.selection.connect('changed', self.on_selection_changed)

        self.tv.cell = gtk.CellRendererText()
        self.tv.cell.set_property( 'editable', True)
        self.tv.cell.connect( 'edited', self.cell_edit,self.liststore,0)
        self.tv.append_column(self.tv.column)

        self.tv.column.pack_start(self.tv.cell, True)
        self.tv.column.set_attributes(self.tv.cell, text=0)

        self.showdependencies()
        self.show_all()
        
    def reload(self,steering):
	  self.steering = steering
	  self.showdependencies()
        
    def showdependencies(self):
	  self.liststore.clear()
	  logger.debug('dependencies:')
	  for s in self.steering.GetDependencies():

	  	row = [s]
	  	self.liststore.append(row)

    def cell_edit( self, cell, path, new_text,model,col ):
        """
        Canges the value of the steering parameter
        """
        logger.debug("Change '%s' to '%s'" % (model[path][col], new_text))
        row = model[path]
        dname = row[col]
        depend =  self.steering.GetDependency(dname)
        if not depend:
			logger.error("dependency %s not found" % dname)
			return
        else:
			if new_text:
				self.steering.RemoveDependency(dname)
				self.steering.AddDependency(new_text)

        self.showdependencies()


