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
from GtkEditConnection import *
import logging

logger = logging.getLogger('GtkDAGRel')

class GtkDAGRel(GtkIPModule):

    TARGETS = [ ('MY_TREE_MODEL_ROW', gtk.TARGET_SAME_WIDGET, 0), ]

    def on_selection_changed(self,selection): 
		try: 
			model, iter = selection.get_selected() 
			src     = model.get_value(iter, 0)
			outbox  = model.get_value(iter, 1)
			dest    = model.get_value(iter, 2)
			inbox   = model.get_value(iter, 3)
			logger.debug("selected connection: %s@%s => %s@%s" % (outbox,src,inbox,dest))

			self.tips.disable()
			src_class = self.iconfig.GetModule(src).GetClass()
			dest_class = self.iconfig.GetModule(dest).GetClass()
			self.tips.set_tip(self.tv, "%s <> %s" % (src_class,dest_class))
			self.tips.enable()
		except: 
			None


    def delete_event(self, widget, event, data=None):
        gtk.main_quit()
        return False

    def delete_connection(self, b):
		sel = self.tv.get_selection()
		model, iter = sel.get_selected() 
		path = model.get_path(iter)
		connection_number   = model.get_value(iter, 4)
		#self.iconfig.RemoveConnection(int(connection_number))
		self.showconnections()

    def add_connection(self, b):
	  	row = []
		#self.iconfig.AddConnection(con)
	  	self.showconnections()
	  	#GtkEditConnection(self,self.iconfig,con)
		

    def __init__(self,iconfig):
        self.iconfig = iconfig
        gtk.VBox.__init__(self)

        self.tips = gtk.Tooltips()
        self.tips.enable()

        # create a liststore with three int columns
        self.liststore = gtk.ListStore(str,str,int)

        self.sw = gtk.ScrolledWindow()

        # Set sort column
        self.tv = gtk.TreeView(self.liststore)
        self.pack_start(self.sw)
        self.b0 = gtk.Button('Add Connection')
        self.b1 = gtk.Button('Delete Connection')
        self.b0.connect('clicked', self.add_connection)
        self.b1.connect('clicked', self.delete_connection)

        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(self.b0, False, False, 1)
        self.hbbox.pack_start(self.b1, False, False, 1)

        self.pack_start(self.hbbox, False)
        self.sw.add(self.tv)

        self.tv.column = [None]*3
        self.tv.column[0] = gtk.TreeViewColumn('From')
        self.tv.column[1] = gtk.TreeViewColumn('To')

#        self.tv.connect('row-activated', self.edit_connection)

        self.tv.cell = [None]*3

        self.selection = self.tv.get_selection()
        self.selection.connect('changed', self.on_selection_changed)
# -------
#        # Allow enable drag and drop of rows including row move
#        self.tv.enable_model_drag_source( gtk.gdk.BUTTON1_MASK,
#                                                self.TARGETS,
#                                                gtk.gdk.ACTION_DEFAULT|
#                                                gtk.gdk.ACTION_MOVE)
#        self.tv.enable_model_drag_dest(self.TARGETS,
#                                             gtk.gdk.ACTION_DEFAULT)
#
#        self.tv.connect("drag_data_get", self.drag_data_get_data)
#        self.tv.connect("drag_data_received", self.drag_data_received_data)
# -------


        self.tv.cell[0] = gtk.CellRendererText()
        self.tv.cell[0].set_property( 'editable', False)
        self.tv.append_column(self.tv.column[0])
        self.tv.column[0].set_sort_column_id(0)
        self.tv.column[0].pack_start(self.tv.cell[0], True)
        self.tv.column[0].set_attributes(self.tv.cell[0], text=0)

        self.tv.cell[1] = gtk.CellRendererText()
        self.tv.cell[1].set_property( 'editable', True)
        self.tv.cell[1].connect( 'edited', self.cell_edit,self.liststore,1)
        self.tv.append_column(self.tv.column[1])
        self.tv.column[1].pack_start(self.tv.cell[1], True)
        self.tv.column[1].set_attributes(self.tv.cell[1], text=1)

        self.showconnections()
        self.show_all()
        

    def reload(self,steering):
	  self.iconfig = steering
	  self.showconnections()
        
    def showconnections(self):
	  self.liststore.clear()


	  for t in self.iconfig.GetTaskDefinitions().values():
	  	c = 0;
	  	for child in t.GetChildren():
	  	    row = []
	  	    row.append(t.GetName())
	  	    row.append(child)
	  	    row.append(c)
	  	    self.liststore.append(row)
	  	    c += 1


    def cell_edit( self, cell, path, new_text,model,col ):
        """
        Canges the value of the connection
        """
        logger.debug("Change '%s' to '%s'" % (model[path][col], new_text))
        row   = model[path]
        src   = row[1]
        child = row[2]
    	srctsk    = self.iconfig.GetTaskDefinitions()[src]
    	srctsk.GetChildren()[child] = new_text


    def drag_data_get_data(self, treeview, context, selection, target_id, etime):
        treeselection = treeview.get_selection()
        model, iter = treeselection.get_selected()
        data = str(model.get_value(iter, 4))
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
            old_row = int(data)
            new_row = int(model.get_value(iter, 4))
            conn = self.iconfig.RemoveConnection(old_row)
            self.iconfig.InsertConnection(new_row,conn)
            row.append(conn.GetOutbox().GetModule())
            row.append(conn.GetOutbox().GetBoxName())
            row.append(conn.GetInbox().GetModule())
            row.append(conn.GetInbox().GetBoxName())

            if (position == gtk.TREE_VIEW_DROP_BEFORE
                or position == gtk.TREE_VIEW_DROP_INTO_OR_BEFORE):
                row.append( new_row )
                model.insert_before(iter, row)
            else:
                row.append( new_row + 1)
                model.insert_after(iter, row)
        else:
            model.append([data])
        if context.action == gtk.gdk.ACTION_MOVE:
            context.finish(True, True, etime)
        return
