#!/bin/env python
#
"""
  Add Simulation Category Window
  copyright  (c) 2005 the icecube collaboration

  @version: $Revision: $
  @date: $Date:  $
  @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import pygtk
pygtk.require("2.0")
import gtk, gobject
from iceprod.core.dataclasses import Parameter
import logging

logger = logging.getLogger('GtkAddSimCat')

class GtkAddSimCat:
    """ The GUI class is the controller for our application """
    def __init__(self,parent):

        # setup the main window
        self.parent = parent
        self.root = gtk.Window(type=gtk.WINDOW_TOPLEVEL)
        self.root.set_title("Add Simulation Category")
        self.root.set_size_request(210, 100)

        self.vbox = gtk.VBox()

        self.bok = gtk.Button(stock=gtk.STOCK_APPLY)
        self.bcancel = gtk.Button(stock=gtk.STOCK_CANCEL)
        self.bok.connect('clicked', self.commit)
        self.bcancel.connect('clicked', self.cancel)
        self.hbok = gtk.HButtonBox()
        self.hbok.pack_start(self.bok)
        self.hbok.pack_start(self.bcancel)

        self.simulation_category_entry = gtk.Entry()
        self.simulation_category_entry.connect("activate", self.commit)
        self.simcatframe = gtk.Frame("Simulation Category")
        self.simcatframe.add(self.simulation_category_entry)
        self.vbox.pack_start(self.simcatframe,False,False,1)
        self.vbox.pack_start(self.hbok,False,False,1)

        # Add our view into the main window
        self.root.add(self.vbox)
        self.root.show_all()
        return


    def commit( self, widget ):
        cat = self.simulation_category_entry.get_text()
        if cat:
       		self.parent.add_simulation_category(cat)
       		self.root.destroy()

    def cancel( self, widget ):
        self.root.destroy()
