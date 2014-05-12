"""
  Gtk from for displaying and configuring IceTray Modules in 
  GtkIcetraConfig application

  copyright  (c) 2005 the icecube collaboration

  @version: $Revision: $
  @date: $Date:  $
  @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""
import pygtk
import pygtk
pygtk.require('2.0')
import gtk
from iceprod.core.dataclasses import *

class GtkIPModule(gtk.VBox):

    dragndrop = False

    def SetDragNDrop(self,value): 
        self.dragndrop = value

