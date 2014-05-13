#!/bin/env python
#
"""
  IceTray connections frame for GtkIcetraConfig application

  copyright  (c) 2005 the icecube collaboration

  @version: $Revision: $
  @date: $Date:  $
  @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import pygtk
pygtk.require("2.0")
import gtk, gobject
from iceprod.core.dataclasses import *
import logging

logger = logging.getLogger('GtkVector')

class GtkVector:
    """ The GUI class is the controller for our application """
    def __init__(self,cobj,parent):

        self.cobj = cobj
        self.parent = parent
        self.store = InfoModel(cobj)	
        self.display = DisplayModel(cobj)

        # setup the main window
        self.root = gtk.Window(type=gtk.WINDOW_TOPLEVEL)
        self.root.connect("delete_event", self.parent.redraw)
        self.root.set_title("Vector Contents")
        self.root.set_size_request(400, 500)

        # Add scrolled window
        self.sw   = gtk.ScrolledWindow()

        # Get the model and attach it to the view
        self.mdl = self.store.get_model()
        self.view = self.display.make_view( self.mdl )
        self.vbox = gtk.VBox()
        self.sw.add(self.view)
        self.vbox.pack_start(self.sw)

        self.hbbox = gtk.HButtonBox()
        self.b0 = gtk.Button('Add Entry')
        self.b1 = gtk.Button('Delete Entry')
        self.b0.connect('clicked', self.display.add_entry,self.store)
        self.b1.connect('clicked', self.display.delete_entry,self.mdl)
        self.hbbox.pack_start(self.b0, False, False, 1)
        self.hbbox.pack_start(self.b1, False, False, 1)

        self.vbox.pack_start(self.hbbox, False)
        # Add our view into the main window
        self.root.add(self.vbox)
        self.root.show_all()
        return


class InfoModel:
    """ The model class holds the information we want to display """

    def __init__(self,cobj):
        """ Sets up and populates our gtk.TreeStore """
        self.tree_store = gtk.ListStore( int, str, str , str)
        self.cobj = cobj
        self.redraw()

    def redraw(self):

        self.tree_store.clear()
        for i in range(len(self.cobj.GetValue())):
            p = self.cobj.GetValue()[i]
            ptype = self.cobj.GetType().strip('v')
            if ptype == 'OMKey':
            	value = "OMKey(%s,%s)" % (p.stringid,p.omid)
            	row = [i, ptype, value,  '' ]  
            else:
            	row = [i, ptype,p.GetValue(), p.GetUnit() or '' ]  
            self.tree_store.append( row )
            logger.debug(str(row))


    def add_entry(self, val,index):
        row = [index,self.cobj.GetType().strip('v'),val.GetValue(),val.GetUnit() or '' ]  
        self.tree_store.append( row )
        logger.debug(str(row))

    def get_model(self):
        """ Returns the model """
        if self.tree_store:
            return self.tree_store 
        else:
            return None

class DisplayModel:
    """ Displays the Info_Model model in a view """
    def __init__(self,cobj):
        self.cobj = cobj
        self.tips = gtk.Tooltips()
        self.tips.enable()


    def on_selection_changed(self,selection): 
		pname  = None

		model, iter = selection.get_selected() 
		if iter:
			try:
				pindex = model.get_value(iter, 0)
				ptype = model.get_value(iter, 1)
				param = self.cobj.GetValue()[pindex]
				logger.debug("selected: %s %s" % (pindex,ptype))

				self.tips.disable()
				self.tips.set_tip(self.view, param.GetDescription())
				self.tips.enable()
			except: pass


    def make_view( self, model ):
        """ Form a view for the Tree Model """
        self.view = gtk.TreeView( model )
        self.selection = self.view.get_selection()
        self.selection.connect('changed', self.on_selection_changed)

        # setup the text cell renderer and allows these
        # cells to be edited.
        self.renderer0 = gtk.CellRendererText()
        self.renderer0.set_property( 'editable', False)

        self.renderer1 = gtk.CellRendererText()
        self.renderer1.set_property( 'editable', False)

        self.renderer2 = gtk.CellRendererText()
        self.renderer2.set_property( 'editable', True )
        self.renderer2.connect( 'edited', self.col2_edited_cb, model )

        self.renderer3 = gtk.CellRendererText()
        self.renderer3.set_property( 'editable', True )
        self.renderer3.connect( 'edited', self.col3_edited_cb, model )
		
        self.column0 = gtk.TreeViewColumn("Index",self.renderer0, text=0)
        self.column1 = gtk.TreeViewColumn("Type",self.renderer1, text=1)
        self.column2 = gtk.TreeViewColumn("Value",self.renderer2,text=2 )
        self.column3 = gtk.TreeViewColumn("Unit",self.renderer3,text=3 )

        self.view.append_column( self.column0 )
        self.view.append_column( self.column1 )
        self.view.append_column( self.column2 )
        self.view.append_column( self.column3 )
        return self.view

		
    def col2_edited_cb( self, cell, path, new_text, model ):
        """
        Canges the value of the entry
		@todo: add type checking for input values
		@todo: add descriptions
        """
        logger.debug("Change '%s' to '%s'" % (model[path][2], new_text))
        row = model[path][0]

        if row < len(self.cobj.GetValue()):
			# Some kind of type checking should happen here
			entry = self.cobj.GetValue()[row]
			try:
			    unit = model[path][3]
			    if self.cobj.GetType().startswith('OMKey'):
			        if new_text.startswith("OMKey"):
			            val = new_text.replace("OMKey",'').replace("(",'')
			            val = val.replace(")",'').split(",")
			            self.cobj.GetValue()[row] = pyOMKey(val[0],val[1])
			    else:
			        entry.SetValue(new_text)
			        if unit:
				        entry.SetUnit(unit)
			    model[path][2] = u'%s' % new_text
			except Exception,e:
			    logger.error(str(e)+": unable to parse value '%s'"% new_text)
        else:
		    logger.error("index %d out of bounds: %d" % ( row , len(self.cobj.GetValue())))

    def col3_edited_cb( self, cell, path, new_text, model ):
        """
        Canges the value of the entry
		@todo: add type checking for input values
		@todo: add descriptions
        """
        logger.debug("Change '%s' to '%s'" % (model[path][3], new_text))
        row = model[path][0]
        if row < len(self.cobj.GetValue()) and new_text:
			param = self.cobj.GetValue()[row]
			if param.GetType().startswith('OMKey'):
				logger.error("type '%s' - does not accept I3Units"%param.GetType())
				return
			elif param.GetType() in VectorTypes:
				map(lambda x:x.SetUnit(u'%s' % new_text),param.GetValue())
			else: 
				param.GetValue().SetUnit(u'%s' % new_text)
			model[path][3] = u'%s' % new_text

    def add_entry(self, b, model):
	  	"""
		Manually add a entry
		"""
		val = Value('NULL')
		index = len(self.cobj.GetValue())
		self.cobj.GetValue().append(val)
		model.add_entry(val,index)

		
    def delete_entry(self, b, mymodel):
		sel = self.view.get_selection()
		model, iter = sel.get_selected() 
		index = mymodel.get_value(iter,0)
		logger.debug("deleting %d" %  index)
		del self.cobj.GetValue()[index]
		model.remove(iter)
