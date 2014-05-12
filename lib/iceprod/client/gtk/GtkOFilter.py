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
from iceprod.core.dataclasses import *
import logging

logger = logging.getLogger('GtkOFilter')

class GtkOFilter(gtk.VBox):

    def on_selection_changed(self,selection): 
		try: 
			model, iter = selection.get_selected() 
			name     = model.get_value(iter, 0)
			logger.debug("selected path:  %s " % path)
		except: 
			None


    def delete_event(self, widget, event, data=None):
        gtk.main_quit()
        return False

    def delete_path(self, b):
		sel = self.tv.get_selection()
		model, iter = sel.get_selected() 
		path   = model.get_value(iter, 1)
		index = model.get_value(iter, 0)
		del self.steering.GetOF().GetPaths()[index]
		model.remove(iter)

    def add_path(self, b):
	  	"""
		add filter search path 
		"""
	  	row = []
		path = Path('')
		rownum = len(self.steering.GetOF().GetPaths())

	  	row.append(rownum)
	  	row.append(path.path)
	  	row.append(path.regex)
	  	self.steering.GetOF().AddPath(path)
	  	i0 = self.liststore.append(row)
		sel = self.tv.get_selection()
		sel.select_iter(i0)

    def __init__(self,steering):
        self.steering = steering
        gtk.VBox.__init__(self)

        # create a liststore with one int and a string column
        self.liststore = gtk.ListStore(int,str,str)

        self.sw = gtk.ScrolledWindow()

        # Set sort column
        self.tv = gtk.TreeView(self.liststore)
        self.pack_start(self.sw)

        self.b0 = gtk.Button('Add Path')
        self.b1 = gtk.Button('Delete Path')
        self.b0.connect('clicked', self.add_path)
        self.b1.connect('clicked', self.delete_path)

        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(self.b0, False, False, 1)
        self.hbbox.pack_start(self.b1, False, False, 1)

        self.pack_start(self.hbbox, False)
        self.sw.add(self.tv)

        self.tv.column = [None]*3
        self.tv.column[0] = gtk.TreeViewColumn('index')
        self.tv.column[1] = gtk.TreeViewColumn('path')
        self.tv.column[2] = gtk.TreeViewColumn('regex')

        self.tv.cell = [None]*3

        self.selection = self.tv.get_selection()
        self.selection.connect('changed', self.on_selection_changed)

        self.tv.cell[0] = gtk.CellRendererText()
        self.tv.cell[0].set_property( 'editable', False)
        self.tv.column[0].set_sort_column_id(0)
        self.tv.cell[1] = gtk.CellRendererText()
        self.tv.cell[1].set_property( 'editable', True)
        self.tv.cell[1].connect( 'edited', self.cell_edit,self.liststore,1)
        self.tv.cell[2] = gtk.CellRendererText()
        self.tv.cell[2].set_property( 'editable', True)
        self.tv.cell[2].connect( 'edited', self.cell_edit,self.liststore,2)

        self.tv.append_column(self.tv.column[0])
        self.tv.append_column(self.tv.column[1])
        self.tv.append_column(self.tv.column[2])
        self.tv.column[0].pack_start(self.tv.cell[0], True)
        self.tv.column[1].pack_start(self.tv.cell[1], True)
        self.tv.column[2].pack_start(self.tv.cell[2], True)
        self.tv.column[0].set_attributes(self.tv.cell[0], text=0)
        self.tv.column[1].set_attributes(self.tv.cell[1], text=1)
        self.tv.column[2].set_attributes(self.tv.cell[2], text=2)

        self.showsteerings()
        self.show_all()
        
    def reload(self,steering):
	  self.steering = steering
	  self.showsteerings()
        
    def showsteerings(self):
	  self.liststore.clear()
	  index = 0
	  if not self.steering.GetOF(): return
	  for s in self.steering.GetOF().GetPaths():

	  	row = [index,s.path,s.regex]
	  	index += 1
	  	self.liststore.append(row)

       	# select the new row in each view
	  	sel = self.tv.get_selection()
#       sel.select_iter(i1)

    def cell_edit( self, cell, path, new_text,model,col ):
        """
        Canges the value of the steering parameter
        """
        logger.debug("Change '%s' to '%s'" % (model[path][col], new_text))
        row = model[path]
        if (col == 1):
			self.steering.GetOF().GetPaths()[row[0]].path = new_text
			row[col] = u'%s' % new_text
        elif (col == 2):
			self.steering.GetOF().GetPaths()[row[0]].regex = new_text
			row[col] = u'%s' % new_text


