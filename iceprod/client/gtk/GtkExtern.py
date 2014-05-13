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

from iceprod.core.dataclasses import *
import pygtk
pygtk.require('2.0')
import gtk, gobject
import logging
from GtkIPModule import *

logger = logging.getLogger('GtkExtern')
NotConnectedException = 'Not connected'


class GtkExtern(GtkIPModule):

    param = {}
    param["Name"]       =   (Extern.GetName ,Extern.SetName)
    param["Version"]    =   (Extern.GetVersion,Extern.SetVersion)
    param["Description"]=   (Extern.GetDescription,Extern.SetDescription)
    param["Executable"] =   (Extern.GetExec,Extern.SetExec)
    param["Arguments"]  =   (Extern.GetArgs,Extern.SetArgs)
    param["Input File"] =   (Extern.GetInFile,Extern.SetInFile)
    param["Output File"]=   (Extern.GetOutFile,Extern.SetOutFile)
    param["Error File"] =   (Extern.GetErrFile,Extern.SetErrFile)

    def get_iconfig(self):
        return self.steering

    def __init__(self,steering):
        gtk.VBox.__init__(self)
        self.steering = steering
        self.tree_store = gtk.TreeStore( str,str , int)
        self.textbuffer = None
        self.sw = gtk.ScrolledWindow()

        # Set sort column
        self.tv = gtk.TreeView(self.tree_store)
        self.pack_start(self.sw)
        self.sw.add(self.tv)
        self.tv.column = [None]*2
        self.tv.column[0] = gtk.TreeViewColumn('Name')
        self.tv.column[1] = gtk.TreeViewColumn('Value')

        self.tv.cell = [None]*2
        self.tv.cell[0] = gtk.CellRendererText()
        self.tv.append_column(self.tv.column[0])
        self.tv.column[0].set_sort_column_id(0)
        self.tv.column[0].pack_start(self.tv.cell[0], True)
        self.tv.column[0].set_attributes(self.tv.cell[0], text=0)

        self.tv.cell[1] = gtk.CellRendererText()
        self.tv.cell[1].set_property('editable',True)
        self.tv.append_column(self.tv.column[1])
        self.tv.column[1].set_sort_column_id(1)
        self.tv.column[1].pack_start(self.tv.cell[1], True)
        self.tv.column[1].set_attributes(self.tv.cell[1], text=1)
        self.tv.cell[1].connect('edited', self.edited_cb, (self.tree_store, 1))



        self.showexterns()
        self.selection = None

    def reload(self,steering):
        self.steering = steering
        self.showexterns()

    def GetSelected(self):
        selection = self.tv.get_selection()
        model, iter = selection.get_selected() 
        if not model.iter_has_child(iter): # This is not the top level
			iter = model.iter_parent(iter) # get parent
        name   = model.get_value(iter, 0)
        value = model.get_value(iter, 1)
        extern = Extern()
        #extern.SetName(name)
        #extern.SetVersion(version)
        return extern

    def showexterns(self):
        # create a liststore with three int columns

        self.rownum = 0
        self.rows = {}
        externs = self.get_iconfig().GetExterns()
        for i in range(len(externs)): 
        	e = externs[i]
        	name = e.GetName()
        	version = e.GetVersion()
        	executable = e.GetExec()
        	args = e.GetArgs()
        	mselected  = False
        		
        	externdict = [ e.GetName() , e.GetVersion() , i]
        	paramdict = {}
        	for key in self.param.keys():
        	    func = self.param[key]
        	    paramdict[key]=[ key, self.param[key][0](e) ,i]
        	self.rows[e.GetName()] = {
					'extern':externdict,'parameters':paramdict,'obj': e
			}

        self.selection = self.tv.get_selection()
        self.selection.connect('changed', self.on_selection_changed)

        self.show_all()
        self.tree_store.clear()
        
        for m in self.rows.keys():
            extern = self.rows[m]['extern']
            params = self.rows[m]['parameters']

            parent = self.tree_store.append( None, extern)
            for p in params.values():
            	self.tree_store.append( parent, p )


    def set_text_buffer(self,textbuffer): 
		self.textbuffer = textbuffer

    def write_text_buffer(self,text): 
		if self.textbuffer:
			self.textbuffer.set_text(text)
		else:
			print text

    def edited_cb(self,cell, path, new_text, user_data):
        store, column = user_data
        iter = store.get_iter(path)
        name = store.get_value(iter, 0)
        value = store.get_value(iter, 1)
        index = store.get_value(iter, 2)
        store.set_value(iter, 1, new_text)
        if store.iter_has_child(iter): # This is the top level
			return
        else:
			iter = store.iter_parent(iter) # get parent
        e = self.rows[store.get_value(iter, 0)]["obj"]
        store.set_value(iter, 0,self.param["Name"][0](e) )
        store.set_value(iter, 1,self.param["Version"][0](e) )
        self.param[name][1](e,new_text)
        return

    def on_selection_changed(self,selection): 
		model, iter = selection.get_selected() 
		name   = model.get_value(iter, 0)
		value  = model.get_value(iter, 1)

