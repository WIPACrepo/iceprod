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
from GtkIPModule import *
import logging

logger = logging.getLogger('GtkProjectDisplay')
NotConnectedException = 'Not connected'


class GtkProjectDisplay(GtkIPModule):

    def get_iconfig(self):
        return self.icetray

    def __init__(self,icetray,db):
        gtk.VBox.__init__(self)
        self.db = db
        self.icetray= icetray
        self.tree_store = gtk.TreeStore( str,str ,int)
        self.textbuffer = None
        self.metaproject_dict = { }
        self.showprojects()
        self.selection = None

    def GetSelected(self):
        selection = self.tv.get_selection()
        model, iter = selection.get_selected() 
        if not iter: return MetaProject()
        if not model.iter_has_child(iter): # This is not a meta project
			iter = model.iter_parent(iter) # get parent
        name    = model.get_value(iter, 0)
        version = model.get_value(iter, 1)
        mid     = model.get_value(iter, 2)
        mp = MetaProject()
        mp.SetName(name)
        mp.SetVersion(version)
        mp.SetId(mid)
        return mp

    def showprojects(self):
        # create a liststore with three int columns

        self.rownum = 0
        self.rows = {}
        metaprojects = self.get_iconfig().GetMetaProjectList()
        for mrow in metaprojects: 
        	mid   = mrow.GetId()
        	mpname = mrow.GetName()
        	mver = mrow.GetVersion()
        	mselected  = False
        		
        	metaproj = self.get_iconfig().GetMetaProject(mpname)
        	metadict = [ mpname, mver, mid]

        	projdict = {}
        	projects = mrow.GetProjectList()
        	for prow in projects:
        		pid   = prow.GetId()
        		pname = prow.GetName()
        		ver   = prow.GetVersion()
        		projdict[pname]=[pname,ver,pid]
        	self.rows[mpname] = {
					'metaproject':metadict,'projects':projdict
			}
        self.sw = gtk.ScrolledWindow()

        # Set sort column
        self.tv = gtk.TreeView(self.tree_store)
        self.pack_start(self.sw)
        self.sw.add(self.tv)
        self.tv.column = [None]*5
        self.tv.column[0] = gtk.TreeViewColumn('Project Name')
        self.tv.column[1] = gtk.TreeViewColumn('version')
        self.tv.column[2] = gtk.TreeViewColumn('')
        self.tv.cell = [None]*6

        self.selection = self.tv.get_selection()
        self.selection.connect('changed', self.on_selection_changed)

        for i in range(2):
            self.tv.cell[i] = gtk.CellRendererText()
            self.tv.append_column(self.tv.column[i])
            self.tv.column[i].set_sort_column_id(i)
            self.tv.column[i].pack_start(self.tv.cell[i], True)
            self.tv.column[i].set_attributes(self.tv.cell[i], text=i)

        self.show_all()
        
        for m in self.rows.keys():
            metaproject = self.rows[m]['metaproject']
            projects = self.rows[m]['projects']

            parent = self.tree_store.append( None, metaproject )
            for p in projects.keys():
            	self.tree_store.append( parent, projects[p] )


    def set_text_buffer(self,textbuffer): 
		self.textbuffer = textbuffer

    def write_text_buffer(self,text): 
		if self.textbuffer:
			self.textbuffer.set_text(text)
		else:
			print text

    def on_selection_changed(self,selection): 
		model, iter = selection.get_selected() 
		name     = model.get_value(iter, 0)
		version  = model.get_value(iter, 1)
		pid      = model.get_value(iter, 2)
		logger.info("selected: %d %s.%s" % (pid,name,version) )

