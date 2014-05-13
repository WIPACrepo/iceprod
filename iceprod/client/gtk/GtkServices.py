#!/bin/env python


"""
  Gtk from for displaying and configuring IceTray Services 
  in GtkIcetraConfig application

  copyright  (c) 2005 the icecube collaboration

  @version: $Revision: $
  @date: $Date:  $
  @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import pygtk
pygtk.require('2.0')
import gtk
from GtkParameterList import GtkParameterList
from GtkServiceList import GtkServiceList 
from iceprod.core.dataclasses import *
from GtkIPModule import *
import logging

logger = logging.getLogger('GtkServices')

class GtkServices(GtkIPModule):

    TARGETS = [ ('MY_TREE_MODEL_ROW', gtk.TARGET_SAME_WIDGET, 0), ]

    def on_selection_changed(self,selection): 
		model, iter = selection.get_selected() 
		if iter:
			name   = model.get_value(iter, 0)
			cname  = model.get_value(iter, 1)
			dep    = model.get_value(iter, 2)
			logger.debug("selected: %s %s %s" % (name,cname,dep))

    def add_service(self, b):
		self.mlist = GtkServiceList(self.iconfig,self.pdb,self)
		self.mlist.SetPrinter(self.printer)

    def delete_service(self, b):
		sel = self.tv.get_selection()
		model, iter = sel.get_selected() 
		name   = model.get_value(iter, 0)
		cname  = model.get_value(iter, 1)
		dep    = model.get_value(iter, 2)
		path = model.get_path(iter)
		self.iconfig.RemoveService(name)
		self.showservices()
		logger.debug("deleted: %s %s %s" % (name,cname,dep))
		
    def configure_service(self, tv, path, viewcol):
		sel = self.tv.get_selection()
		sel.select_path(path)
		model, iter = sel.get_selected() 
		name   = model.get_value(iter, 0)
		cname  = model.get_value(iter, 1)
		dep    = model.get_value(iter, 2)
		logger.debug("clicked on: %s %s %s" % (name,cname,dep))
		service = self.iconfig.GetService(name)
		GtkParameterList(service)

    def col_edited( self, cell, path, new_text, model ):
        """
        Canges the name of the module
		@todo: add type checking for input values
		@todo: add descriptions
        """
        logger.debug("Change '%s' to '%s'" % (model[path][0], new_text))
        row = model[path]
        sname = row[0]
        serv = self.iconfig.GetService(sname)
        if serv:
			self.iconfig.RemoveService(serv.GetName())
			serv.SetName(u'%s' % new_text)
			self.iconfig.AddService(serv)
			row[0] = u'%s' % new_text

    def __init__(self,iconfig,pdb):
        self.iconfig = iconfig
        self.pdb = pdb
        gtk.VBox.__init__(self)
        self.printer = None

        # create a liststore with three int columns
        self.liststore = gtk.ListStore(str,str,str)
        self.sw = gtk.ScrolledWindow()
        self.tv = gtk.TreeView(self.liststore)
        self.pack_start(self.sw)

        self.b0 = gtk.Button('Add Service',stock=gtk.STOCK_ADD)
        self.b0.connect('clicked', self.add_service)
        self.b1 = gtk.Button('Delete Service',stock=gtk.STOCK_DELETE)
        self.b1.connect('clicked', self.delete_service)

        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(self.b0, False, False, 1)
        self.hbbox.pack_start(self.b1, False, False, 1)

        self.tv.connect('row-activated', self.configure_service)
        self.pack_start(self.hbbox, False)
        self.sw.add(self.tv)

        self.tv.column = [None]*6
        self.tv.column[0] = gtk.TreeViewColumn('name')
        self.tv.column[1] = gtk.TreeViewColumn('class')
        self.tv.column[2] = gtk.TreeViewColumn('project')

        self.tv.cell = [None]*6

        self.selection = self.tv.get_selection()
        self.selection.connect('changed', self.on_selection_changed)

        for i in range(3):
            self.tv.cell[i] = gtk.CellRendererText()
            self.tv.append_column(self.tv.column[i])
            self.tv.column[i].set_sort_column_id(i)
            self.tv.column[i].pack_start(self.tv.cell[i], True)
            self.tv.column[i].set_attributes(self.tv.cell[i], text=i)

        self.tv.cell[0].set_property( 'editable', True)
        self.tv.cell[0].connect( 'edited', self.col_edited,self.liststore)

        self.showservices()
        self.show_all()

    def set_drag_n_drop(self, value=True):
        # Allow enable drag and drop of rows including row move
        self.tv.enable_model_drag_source( gtk.gdk.BUTTON1_MASK,
                                                self.TARGETS,
                                                gtk.gdk.ACTION_DEFAULT|
                                                gtk.gdk.ACTION_MOVE)
        self.tv.enable_model_drag_dest(self.TARGETS,
                                             gtk.gdk.ACTION_DEFAULT)

        self.tv.connect("drag_data_get", self.drag_data_get_data)
        self.tv.connect("drag_data_received", self.drag_data_received_data)


    def drag_data_get_data(self, treeview, context, selection, target_id, etime):
        treeselection = treeview.get_selection()
        model, iter = treeselection.get_selected()
        data = model.get_value(iter, 0)
        selection.set(selection.target, 8, data)

    def drag_data_received_data(self, treeview, context, x, y, selection,
                                info, etime):
        model = treeview.get_model()
        data = selection.data
        drop_info = treeview.get_dest_row_at_pos(x, y)
        if drop_info:
            path, position = drop_info
            iter = model.get_iter(path)

            row = []
            service = self.iconfig.RemoveService(data)
            row.append(service.GetName())
            row.append(service.GetClass())
            row.append(','.join([p.GetName() for p in service.GetProjectList()]))
            if (position == gtk.TREE_VIEW_DROP_BEFORE
                or position == gtk.TREE_VIEW_DROP_INTO_OR_BEFORE):
                model.insert_before(iter, row)
                self.iconfig.InsertService(path[0],service)
            else:
                model.insert_after(iter, row)
                self.iconfig.InsertService(path[0]+1,service)
        if context.action == gtk.gdk.ACTION_MOVE:
            context.finish(True, True, 0)
            #context.finish(True, True, etime)
        self.reload(self.iconfig)
        return
        
    def SetPrinter(self,printer):
        self.printer = printer

    def Print(self,txt):
        if self.printer:
			self.printer(txt)
        else:
			print txt

    def reload(self,icetray):
	  self.iconfig = icetray
	  self.showservices()
        
    def showservices(self):
	  self.liststore.clear()
	  services = self.iconfig.GetServices()
	  for s in services: 
	  	row = []
	  	row.append(s.GetName())
	  	row.append(s.GetClass())
	  	row.append(','.join([p.GetName() for p in s.GetProjectList()]))
	  	i0 = self.liststore.append(row)

