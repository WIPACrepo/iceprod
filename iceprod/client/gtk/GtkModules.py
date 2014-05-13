#!/bin/env python
#
"""
  Gtk from for displaying and configuring IceTray Modules in 
  GtkIcetraConfig application

  copyright  (c) 2005 the icecube collaboration

  @version: $Revision: $
  @date: $Date:  $
  @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import pygtk
pygtk.require('2.0')
import gtk
from GtkParameterList import GtkParameterList
from GtkModuleList import GtkModuleList 
from iceprod.core.dataclasses import *
from GtkIPModule import *
import logging

logger = logging.getLogger('GtkModules')

class GtkModules(GtkIPModule):

    TARGETS = [ ('MY_TREE_MODEL_ROW', gtk.TARGET_SAME_WIDGET, 0), ]


    def on_selection_changed(self,selection): 
		model, iter = selection.get_selected() 
		if iter:
			name   = model.get_value(iter, 0)
			cname  = model.get_value(iter, 1)
			dep    = model.get_value(iter, 2)
			logger.debug("selected: %s %s %s" % (name,cname,dep))

    def add_module(self, b):
		self.mlist = GtkModuleList(self.iconfig,self.pdb,self)
		self.mlist.SetPrinter(self.printer)
		
    def delete_module(self, b):
		sel = self.tv.get_selection()
		model, iter = sel.get_selected() 
		name   = model.get_value(iter, 0)
		cname  = model.get_value(iter, 1)
		dep    = model.get_value(iter, 2)
		path = model.get_path(iter)
		self.iconfig.RemoveModule(name)
		self.showmodules()
		logger.debug("deleted: %s %s %s" % (name,cname,dep))
		
    def configure_module(self, tv, path, viewcol):
		sel = self.tv.get_selection()
		sel.select_path(path)
		model, iter = sel.get_selected() 
		name   = model.get_value(iter, 0)
		cname  = model.get_value(iter, 1)
		dep    = model.get_value(iter, 2)
		logger.debug("clicked on: %s %s %s" % (name,cname,dep))
		module = self.iconfig.GetModule(name)
		GtkParameterList(module)

    def col_edited( self, cell, path, new_text, model ):
        """
        Canges the name of the module
		@todo: add type checking for input values
		@todo: add descriptions
        """
        logger.debug("Change '%s' to '%s'" % (model[path][0], new_text))
        row = model[path]
        mname = row[0]
        mod = self.iconfig.GetModule(mname)
        if mod:
			mod.SetName(u'%s' % new_text)
			row[0] = u'%s' % new_text

    def __init__(self,iconfig,pdb):
        self.iconfig = iconfig
        self.pdb = pdb
        gtk.VBox.__init__(self)
        self.printer   = None
        self.dragndrop = False

        # create a liststore with three int columns
        self.liststore = gtk.ListStore(str,str,str)
        self.sw = gtk.ScrolledWindow()
        self.tv = gtk.TreeView(self.liststore)
        self.pack_start(self.sw)

        self.b0 = gtk.Button('Add Module',stock=gtk.STOCK_ADD)
        self.b0.connect('clicked', self.add_module)
        self.b1 = gtk.Button('Delete Module',stock=gtk.STOCK_DELETE)
        self.b1.connect('clicked', self.delete_module)

        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(self.b0, False, False, 1)
        self.hbbox.pack_start(self.b1, False, False, 1)

        self.tv.connect('row-activated', self.configure_module)
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

        self.showmodules()
        self.show_all()

    def SetDragNDrop(self,value): 
        self.dragndrop = value
        if self.dragndrop:
          # Allow enable drag and drop of rows including row move
          self.tv.enable_model_drag_source( gtk.gdk.BUTTON1_MASK,
                                                self.TARGETS,
                                                gtk.gdk.ACTION_DEFAULT|
                                                 gtk.gdk.ACTION_MOVE)
                                                #gtk.gdk.ACTION_ASK)
          self.tv.enable_model_drag_dest(self.TARGETS,
                                             gtk.gdk.ACTION_DEFAULT)

          self.tv.connect("drag_data_get", self.drag_data_get_data)
          self.tv.connect("drag_data_received", self.drag_data_received_data)


    def drag_data_get_data(self, treeview, context, selection, target_id, etime):
        if not self.dragndrop: return
        treeselection = treeview.get_selection()
        model, iter = treeselection.get_selected()
        data = model.get_value(iter, 0)
        selection.set(selection.target, 8, data)

    def drag_data_received_data(self, treeview, context, x, y, selection,
                                info, etime):
        if not self.dragndrop: return
        model = treeview.get_model()
        data = selection.data
        drop_info = treeview.get_dest_row_at_pos(x, y)
        if drop_info:
            path, position = drop_info
            iter = model.get_iter(path)

            row = []
            module = self.iconfig.RemoveModule(data)
            row.append(module.GetName())
            row.append(module.GetClass())
            row.append(','.join([p.GetName() for p in module.GetProjectList()]))
            if (position == gtk.TREE_VIEW_DROP_BEFORE
                or position == gtk.TREE_VIEW_DROP_INTO_OR_BEFORE):
                model.insert_before(iter, row)
                self.iconfig.InsertModule(path[0],module)
            else:
                model.insert_after(iter, row)
                self.iconfig.InsertModule(path[0]+1,module)
        if context.action == gtk.gdk.ACTION_MOVE:
            context.finish(True, True, long(0))
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
	  self.showmodules()
        
    def showmodules(self):
	  self.liststore.clear()
	  modules = self.iconfig.GetModules()
	  for m in modules: 
	  	row = []
	  	row.append(m.GetName())
	  	row.append(m.GetClass())
	  	row.append(','.join([p.GetName() for p in m.GetProjectList()]))
	  	i0 = self.liststore.append(row)

