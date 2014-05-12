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
from GtkIPModule import *
import logging

logger = logging.getLogger('GtkSteering')

class GtkSteering(GtkIPModule):

    def on_selection_changed(self,selection): 
		try: 
			model, iter = selection.get_selected() 
			name     = model.get_value(iter, 0)
			value    = model.get_value(iter, 1)
		except: 
			None


    def delete_event(self, widget, event, data=None):
        gtk.main_quit()
        return False

    def delete_steering(self, b):
		sel = self.tv.get_selection()
		model, iter = sel.get_selected() 
		name     = model.get_value(iter, 1)
		self.steering.RemoveParameter(name)
		model.remove(iter)

    def add_steering(self, b):
	  	"""
		add steering parameter
		"""
	  	row = []
		param = Parameter()
		param.SetName('new_parameter')
		param.SetType('string')
		param.SetValue('NULL')
		self.steering.AddParameter(param)

	  	row.append(param.GetType())
	  	row.append(param.GetName())
	  	row.append(param.GetValue())
	  	i0 = self.liststore.append(row)
		sel = self.tv.get_selection()
		sel.select_iter(i0)
		
		

    def __init__(self,steering):
        self.steering = steering
        gtk.VBox.__init__(self)

        # create a liststore with three int columns
        self.liststore = gtk.ListStore(str,str,str)

        self.sw = gtk.ScrolledWindow()

        # Set sort column
        self.tv = gtk.TreeView(self.liststore)
        self.pack_start(self.sw)

        self.b0 = gtk.Button('Add Parameter')
        self.b1 = gtk.Button('Delete Parameter')
        self.b0.connect('clicked', self.add_steering)
        self.b1.connect('clicked', self.delete_steering)

        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(self.b0, False, False, 1)
        self.hbbox.pack_start(self.b1, False, False, 1)

        self.pack_start(self.hbbox, False)
        self.sw.add(self.tv)

        self.tv.column = [None]*3
        self.tv.column[0] = gtk.TreeViewColumn('Type')
        self.tv.column[1] = gtk.TreeViewColumn('Parameter')
        self.tv.column[2] = gtk.TreeViewColumn('Value')

        self.tv.cell = [None]*3

        self.selection = self.tv.get_selection()
        self.selection.connect('changed', self.on_selection_changed)


        for i in range(3):
            self.tv.cell[i] = gtk.CellRendererText()
            self.tv.cell[i].set_property( 'editable', True)
            self.tv.cell[i].connect( 'edited', self.cell_edit,self.liststore,i)
            self.tv.append_column(self.tv.column[i])
            self.tv.column[i].set_sort_column_id(i)
            self.tv.column[i].pack_start(self.tv.cell[i], True)
            self.tv.column[i].set_attributes(self.tv.cell[i], text=i)

        self.showsteerings()
        self.show_all()
        
    def reload(self,steering):
	  self.steering = steering
	  self.showsteerings()
        
    def showsteerings(self):
	  self.liststore.clear()
	  for s in self.steering.GetParameters():

	  	row = [s.GetType(),s.GetName(),s.GetValue()]
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
        pname = row[col]
        param =  self.steering.GetParameter(row[1])
        if not param:
			logger.warn("parameter %s not found" % row[1])
			return
        if (col == 0):
			param.SetType(new_text)
        elif (col == 1):
			self.steering.RemoveParameter(model[path][col])
			param.SetName(new_text)
			self.steering.AddParameter(param)
        elif (col == 2):
			param.SetValue(new_text)
        else:
			logger.error("unknown column %d " % col)
        row[col] = u'%s' % new_text


