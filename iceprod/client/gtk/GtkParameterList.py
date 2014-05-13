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
from GtkVector import GtkVector
import logging

logger = logging.getLogger('GtkParameterList')

class GtkParameterList:
    """ The GUI class is the controller for our application """
    def __init__(self,cobj,width=750,height=600):

        self.cobj = cobj
        self.store = InfoModel(self.cobj)	
        self.display = DisplayModel(self)

        # setup the main window
        self.root = gtk.Window(type=gtk.WINDOW_TOPLEVEL)
        self.root.set_title("Parameter Table")
        self.root.set_size_request(width, height)
        self.root.set_position(gtk.WIN_POS_CENTER_ALWAYS)

        # Add scrolled window
        self.sw   = gtk.ScrolledWindow()

        # Get the model and attach it to the view
        self.mdl = self.store.get_model()
        self.view = self.display.make_view( self.mdl )
        self.vbox = gtk.VBox()
        self.sw.add(self.view)
        self.vbox.pack_start(self.sw)

        self.hbbox = gtk.HButtonBox()
        self.b0 = gtk.Button('Add Parameter')
        self.b1 = gtk.Button('Delete Parameter')
        self.b0.connect('clicked', self.display.add_parameter,self.store)
        self.b1.connect('clicked', self.display.delete_parameter,self.mdl)
        self.hbbox.pack_start(self.b0, False, False, 1)
        self.hbbox.pack_start(self.b1, False, False, 1)

        self.vbox.pack_start(self.hbbox, False)
        # Add our view into the main window
        self.root.add(self.vbox)
        self.root.show_all()
        return

    def redraw(self, widget, event=None):
        self.store.redraw()

class InfoModel:
    """ The model class holds the information we want to display """

    def __init__(self,cobj):
        """ Sets up and populates our gtk.TreeStore """
        self.tree_store = gtk.ListStore( str, str, str , str)
        self.cobj = cobj
        self.redraw()

    def redraw(self):
        self.tree_store.clear()
        params = self.cobj.GetParameters()
        for p in params:
            if p.GetType() == 'OMKey':
            	value = "OMKey(%s,%s)" % (p.GetValue().stringid,p.GetValue().omid)
            	row = [p.GetType(),p.GetName(), value,  '' ]  
            elif p.GetType() == 'OMKeyv':
            	getvalue = lambda x: "OMKey(%s,%s)" % (x.stringid,x.omid)
            	if len(p.GetValue()) > 0:
            		value = "[%s]" % (','.join(map(getvalue,p.GetValue())))
            	else:
            		value = "[]" % (','.join(map(getvalue,p.GetValue())))
            	row = [p.GetType(),p.GetName(), value,  '' ]  
            elif p.GetType() in VectorTypes:
            	getvalue = lambda x: x.unit and "%s*I3Units::%s" % (x.value,x.unit) \
						   or x.value
            	if len(p.GetValue()) > 0:
            		value = "[%s]" % (','.join(map(getvalue,p.GetValue())))
            	else:
            		value = "[]" 
            	row = [p.GetType(),p.GetName(), value,  '' ]  
            else:
            	row = [p.GetType(),p.GetName(), p.GetValue().value, p.GetValue().unit or '' ]  
            self.tree_store.append( row )
            logger.debug('|'.join(row))


    def add_parameter(self, param):
        row = [param.GetType(),param.GetName(),param.GetValue().value,param.GetValue().unit or '' ]  
        self.tree_store.append( row )
        logger.debug('|'.join(row))

    def get_model(self):
        """ Returns the model """
        if self.tree_store:
            return self.tree_store 
        else:
            return None

class DisplayModel:
    """ Displays the Info_Model model in a view """
    def __init__(self,parent):
        self.parent = parent
        self.cobj = self.parent.cobj
        self.tips = gtk.Tooltips()
        self.tips.enable()
        self.popup = gtk.Menu()
        viewconfig = gtk.MenuItem("view in browser...")
        viewconfig.show()
        viewconfig.connect("activate",self.showpage)
        self.popup.append(viewconfig)
        self.popup.show()
        self.url = None

    def showpage(self, widget):
        try:
		   import webbrowser
		   print self.url
		   webbrowser.open(self.url)
        except Exception,e:
		   logger.debug(e)
		   pass

    def edit_vector(self, tv, path, viewcol):
        sel = self.view.get_selection()
        sel.select_path(path)
        model, iter = sel.get_selected() 
        row = model[path]
        pname = row[1]
        param = self.cobj.GetParameter(pname)
        if param:
			if param.GetType() in VectorTypes:
			    self.mlist = GtkVector(param,self.parent)

    def showtip( self, widget, event):
		if widget == self.view and self.view.get_path_at_pos(int(event.x),int(event.y)):
		    path,w,x,y = self.view.get_path_at_pos(int(event.x),int(event.y))
		    model = self.view.get_model()
		    row = model[path]
		    pname = row[1]
		    param = self.cobj.GetParameter(pname)

		    if not isinstance(param.GetValue(),list) and param.GetValue().value.startswith('http'):
				   self.tips.set_tip(self.view, "right-click to view in browser")
		    else:
				   self.tips.set_tip(self.view, param.GetDescription())
		    self.tips.enable()
 
    def on_selection_changed(self,selection): 
		pname  = None

		model, iter = selection.get_selected() 
		if iter:
			try:
				ptype = model.get_value(iter, 0)
				pname = model.get_value(iter, 1)
				param = self.cobj.GetParameter(pname)
				logger.debug("selected: %s %s" % (ptype,pname))

				self.tips.set_tip(self.view, param.GetDescription())

				if ptype in VectorTypes: 
				    self.renderer2.set_property( 'editable', False)
				else:
				    self.renderer2.set_property( 'editable', True)
			except: pass


    def make_view( self, model ):
        """ Form a view for the Tree Model """
        self.view = gtk.TreeView( model )
        self.selection = self.view.get_selection()
        self.selection.connect('changed', self.on_selection_changed)
        self.view.connect( 'row-activated', self.edit_vector )
        self.view.connect( 'motion-notify-event', self.showtip )

        # setup the text cell renderer and allows these
        # cells to be edited.
        self.renderer0 = gtk.CellRendererText()
        self.renderer0.set_property( 'editable', True)
        self.renderer0.connect( 'edited', self.col0_edited_cb, model )

        self.renderer1 = gtk.CellRendererText()
        self.renderer1.set_property( 'editable', True)
        self.renderer1.connect( 'edited', self.col1_edited_cb, model )

        self.renderer2 = gtk.CellRendererText()
        self.renderer2.set_property( 'editable', True )
        self.renderer2.connect( 'edited', self.col2_edited_cb, model )

        self.renderer3 = gtk.CellRendererText()
        self.renderer3.set_property( 'editable', True )
        self.renderer3.connect( 'edited', self.col3_edited_cb, model )
		
        self.column0 = gtk.TreeViewColumn("Type",self.renderer0, text=0)
        self.column1 = gtk.TreeViewColumn("Name",self.renderer1, text=1)
        self.column2 = gtk.TreeViewColumn("Value",self.renderer2,text=2 )
        self.column3 = gtk.TreeViewColumn("Unit",self.renderer3,text=3 )

        self.view.append_column( self.column0 )
        self.view.append_column( self.column1 )
        self.view.append_column( self.column2 )
        self.view.append_column( self.column3 )

        self.view.connect('button-press-event', self.clicked)

        return self.view

    def col0_edited_cb( self, cell, path, new_text, model ):
        """
        Canges the type of the parameter (for manually added params)
		@todo: add type checking for input values
        """
        logger.debug("Change '%s' to '%s'" % (model[path][0], new_text))
        row = model[path]
        pname = row[1]
        param = self.cobj.GetParameter(pname)
        if param:
			# Some kind of type checking should happen here
			param.SetType(u'%s' % new_text)
			if new_text in VectorTypes and not isinstance(param.GetValue(),list):
			    param.SetValue([param.GetValue()])

			row[0] = u'%s' % new_text

        self.parent.redraw(None,None)

    def col1_edited_cb( self, cell, path, new_text, model ):
        """
        Canges the name of the parameter  (for manually added params)
		@todo: add type checking for input values
        """
        logger.debug("Change '%s' to '%s'" % (model[path][2], new_text))
        row = model[path]
        pname = row[1]
        param = self.cobj.GetParameter(pname)
        if param:
        	self.cobj.RemoveParameter(pname)
        	param.SetName(u'%s' % new_text)
        	self.cobj.AddParameter(param)
        	row[1] = u'%s' % new_text

    def clicked(self, view, event):
          if event.button == 3 and view.get_path_at_pos(int(event.x), int(event.y)):
            path, col, cellx, celly = view.get_path_at_pos(int(event.x), int(event.y))
            self.view.grab_focus()
            selection = self.view.get_selection()
            if not selection.path_is_selected(path):
               self.view.set_cursor( path, col, 0)

            model, iter = selection.get_selected() 
            pname  = model.get_value(iter, 1)
            pvalue = self.cobj.GetParameter(pname).GetValue().value
            if pvalue.startswith('http'):
               self.url = pvalue
               self.popup.popup( None, None, None, event.button, event.time)

            return True

    def parse_val(self,ptype,value):
		if ptype in VectorTypes:
			if ptype.startswith("OMKey"):
				valvect = re.findall(r'OMKey\(\s*[0-9]+\s*,\s*[0-9]+\s*\)',value)
				valvect = map(lambda x: self.parse_val("OMKey",x),valvect)
				return valvect
			else: 
				val = value.strip("[").strip("]").strip().split(",")
				val = map(lambda x: x.strip("\""),val)

				if ptype.startswith("string") or ptype.startswith("bool"):
					return map(Value,val)
				else:
					vals = map(lambda x: x.split("*I3Units::")[0],val)
					units = map(lambda x: (len(x.split("*I3Units::")) > 1 and \
							x.split("*I3Units::") or [None,None])[1] ,val)
					return map(lambda x,y: Value(x,y),vals,units)
		elif ptype == 'OMKey' and value.startswith("OMKey"):
			val = value.replace("OMKey",'').replace("(",'').replace(")",'').split(",")
			return pyOMKey(val[0],val[1])
		else: return Value(value)
		
    def col2_edited_cb( self, cell, path, new_text, model ):
        """
        Canges the value of the parameter
		@todo: add type checking for input values
		@todo: add descriptions
        """
        logger.debug("Change '%s' to '%s'" % (model[path][2], new_text))
        row = model[path]
        pname = row[1]
        param = self.cobj.GetParameter(pname)
        if param:
			if param.GetType() in VectorTypes:
			    return
			else:
			    try:
				    unit = model[path][3]
				    param.SetValue(self.parse_val(param.GetType(),new_text))
				    if unit:
					    param.GetValue().SetUnit(unit)
				    row[2] = u'%s' % new_text
			    except Exception,e:
				    logger.error(str(e)+": unable to parse value '%s'"% new_text)

    def col3_edited_cb( self, cell, path, new_text, model ):
        """
        Canges the value of the parameter
		@todo: add type checking for input values
		@todo: add descriptions
        """
        logger.debug("Change '%s' to '%s'" % (model[path][3], new_text))
        row = model[path]
        pname = row[1]
        param = self.cobj.GetParameter(pname)
        if param and new_text:
			if param.GetType().startswith('OMKey'):
				logger.error("type '%s' - does not accept I3Units"%param.GetType())
				return
			elif param.GetType() in VectorTypes:
				map(lambda x:x.SetUnit(u'%s' % new_text),param.GetValue())
			else: 
				param.GetValue().SetUnit(u'%s' % new_text)
			row[3] = u'%s' % new_text

    def add_parameter(self, b, model):
	  	"""
		Manually add a parameter
		"""
		param = Parameter()
		param.SetName('new_parameter')
		param.SetType('string')
		param.SetValue(Value('NULL'))
		self.cobj.AddParameter(param)
		model.add_parameter(param)

		
    def delete_parameter(self, b, mymodel):
		sel = self.view.get_selection()
		model, iter = sel.get_selected() 
		pname = mymodel.get_value(iter,1)
		logger.debug("deleting %s" %  pname)
		self.cobj.RemoveParameter(pname)
		model.remove(iter)
