#!/bin/env python
#   copyright  (c) 2005
#   the icecube collaboration
#   $Id: $
#
#   @version $Revision: $
#   @date $Date: $
#   @author Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
#	@brief Project display frame for GtkIcetraConfig application
#########################################################################
import pygtk
pygtk.require("2.0")
import gtk, gobject
from iceprod.core.dataclasses import *
import logging

logger = logging.getLogger('GtkIceProdPreList')

class GtkIceProdPreList:
    """ The GUI class is the controller for our application """

    fooname = 'foo'
    fooclass = 'FOOBAR'
    fooproject = 'iceprod.modules'

    def __init__(self,iconfig,pdb,gtkmod,title="Available IceProdPre Modules"):

        self.iconfig = iconfig
        self.pdb = pdb
        self.gtkmod = gtkmod
        self.printer = None
        self.display()

        # setup the main window
        self.root = gtk.Window(type=gtk.WINDOW_TOPLEVEL)
        self.sw = gtk.ScrolledWindow()
        self.root.set_title(title)
        self.root.set_size_request(600, 475)

        # Get the model and attach it to the view
        self.view = self.make_view( self.store )

        b = gtk.Button(stock=gtk.STOCK_APPLY)
        b.connect('clicked', self.commit_changes)
        self.hbbox = gtk.HButtonBox()
        self.hbbox.pack_start(b, False, False, 1)

        # Add our view into the main window
        self.sw.add(self.view)

        self.vbox = gtk.VBox(False,0)
        self.vbox.pack_start(self.sw,True,True,2)
        self.vbox.pack_end(self.hbbox,False,False,2)
        self.root.add(self.vbox)
        self.root.show_all()
        return

    def SetPrinter(self,printer):
        self.printer = printer

    def SetParent(self,parent):
        self.parent = parent

    def Print(self,txt):
        if self.printer:
			self.printer(txt)
        else:
			print txt


    def display(self):
        """ Sets up and populates gtk.TreeStore """
        self.store = gtk.TreeStore( int, str, str, str, gobject.TYPE_BOOLEAN )
        row = [0,self.fooname,self.fooclass, self.fooproject,False]  
        self.store.append(None, row )

        if not self.pdb.cached and not self.pdb.connect(): return
        modules = self.pdb.GetIceProdModules()
        for m in modules:
            if not self.iconfig.GetIceProdPost(m.GetName()):
                  row = [m.GetId(),m.GetName(), m.GetClass(),'iceprod.modules',False]  
                  logger.debug('|'.join(map(str,row)))
                  self.store.append(None, row )

    def add_module(self, tv, path, viewcol):
		""" 
		 Add IceProdPre
		"""
		sel = tv.get_selection()
		sel.select_path(path)
		model, iter = sel.get_selected() 
		id     = model.get_value(iter, 0)
		name   = model.get_value(iter, 1)
		cname  = model.get_value(iter, 2)
		proj_name   = "iceprod.modules"

		newmod = IceProdPre()
		newmod.SetName(name)
		newmod.SetClass(cname)

		if not self.pdb.cached and not self.pdb.connect(): return
		parameters = self.pdb.GetParameters(int(id))
		for p in parameters:
			param = Parameter()
			param.SetName(p.GetName())
			param.SetType(p.GetType())
			param.SetDescription(p.GetDescription())
			param.SetValue(p.GetValue())
			newmod.AddParameter(param)

		self.iconfig.AddIceProdPre(newmod)
		self.gtkmod.showmodules()
		self.root.destroy()

    def commit_changes(self, widget, data=None):
        model = self.store
        self.mdict = {}
        if not self.pdb.cached and not self.pdb.connect(): return
        model.foreach(self.do_change)
        return

    def do_change(self, model, path, iter,connect=True):
		""" 
		 Add IceProdPre
		"""
		if model[path][4]:
			id     = model[path][0]
			name   = model[path][1]
			cname  = model[path][2]

			proj_name  = "iceprod.modules"
			proj = self.iconfig.GetProject(proj_name)

			newmod = IceProdPre()
			newmod.SetName(name)
			newmod.SetClass(cname)

			parameters = self.pdb.GetParameters(int(id))
			for p in parameters:
				param = Parameter()
				param.SetName(p.GetName())
				param.SetType(p.GetType())
				param.SetDescription(p.GetDescription())
				param.SetValue(p.GetValue())
				newmod.AddParameter(param)

			self.iconfig.AddIceProdPre(newmod)
			self.gtkmod.showmodules()
			self.root.destroy()

    def col_edited( self, cell, path, new_text, model , column):
        """
        Canges the name of the module
		@todo: add type checking for input values
		@todo: add descriptions
        """
        row = model[path]
        row[column] = u'%s' % new_text


    def row_toggled( self,cell, path):
        """
        Sets the toggled state on the toggle button to true or false.
        """
        model = self.store
        model[path][4] =  not model[path][4] 

        if not model[path][4]:
        	self.renderer[1].set_property('editable', False)
        	self.renderer[2].set_property('editable', False)
        	self.renderer[3].set_property('editable', False)
        	return

        if model[path][3] == self.fooproject:
        	self.renderer[3].set_property('editable', True)
        	self.renderer[3].connect( 'edited', self.col_edited,self.store,3)
        if model[path][2] == self.fooclass:
        	self.renderer[2].set_property('editable', True)
        	self.renderer[2].connect( 'edited', self.col_edited,self.store,2)
        if model[path][1] == self.fooname:
        	self.renderer[1].set_property('editable', True)
        	self.renderer[1].connect( 'edited', self.col_edited,self.store,1)


    def make_view( self, model ):
        """ Form a view for the Tree Model """
        self.view = gtk.TreeView( model )
        self.view.connect('row-activated', self.add_module)

        # setup the text cell renderer and allows these
        # cells to be edited.
        self.renderer = [None]*5
        for i in range(4):
        	self.renderer[i] = gtk.CellRendererText()
        self.renderer[4]  = gtk.CellRendererToggle()
        self.renderer[4].set_property('activatable', True)
        self.renderer[4].connect( 'toggled', self.row_toggled)
		
        self.column =  [None]*5
        self.column[0] = gtk.TreeViewColumn("DbID",self.renderer[0], text=0)
        self.column[0].set_sort_column_id(0)
        self.column[1] = gtk.TreeViewColumn("Default Name",self.renderer[1], text=1)
        self.column[1].set_sort_column_id(1)
        self.column[2] = gtk.TreeViewColumn("Class",self.renderer[2], text=2)
        self.column[2].set_sort_column_id(2)
        self.column[3] = gtk.TreeViewColumn("Project",self.renderer[3],text=3 )
        self.column[4] = gtk.TreeViewColumn('Selected')
        self.column[4].pack_start(self.renderer[4], True)
        self.column[4].add_attribute( self.renderer[4], "active", 4)

        for i in range(5):
        	self.view.append_column( self.column[i] )
        return self.view
