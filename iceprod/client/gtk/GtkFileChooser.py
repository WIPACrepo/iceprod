#!/bin/env python
#   copyright  (c) 2005
#   the icecube collaboration
#   $Id: $
#
#   @version $Revision: $
#   @date $Date: $
#   @author Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
#	@brief File chooser for saving and opening files in  GtkIcetraConfig
#	application
#########################################################################
import pygtk
pygtk.require('2.0')
import gtk
from os.path import expandvars

class GtkSaveFileChooser:
    # Get the selected filename and print it to the console
    def file_ok_sel(self, w):
        filename = self.filew.get_filename()
        self.parent.configfile = filename
        self.parent.savefile(None,filename)
        self.filew.destroy()

    def destroy(self, widget):
        self.filew.destroy()

    def __init__(self,parent):
        # Create a new file selection widget
        self.parent = parent
        self.filew   = gtk.FileSelection("File selection")

        self.filew.connect("destroy", self.destroy)
        # Connect the ok_button to file_ok_sel method
        self.filew.ok_button.connect("clicked", self.file_ok_sel)
    
        # Connect the cancel_button to destroy the widget
        self.filew.cancel_button.connect("clicked",
                                         lambda w: self.filew.destroy())
    
        # Lets set the filename, as if this were a save dialog,
        # and we are giving a default filename
        self.filew.set_filename(self.parent.GetConfigFile() or "myconfig.xml")
    
        self.filew.show()

class GtkOpenFileChooser:
    # Get the selected filename and print it to the console
    def file_ok_sel(self, w):
        self.filename = self.filew.get_filename()
        self.parent.getconfig(self.filename)
        self.filew.destroy()

    def destroy(self, widget):
        self.filew.destroy()

    def __init__(self,parent,filename):
        # Create a new file selection widget
        self.parent = parent
        self.filew   = gtk.FileSelection("File selection")
        self.filename = filename

        self.filew.connect("destroy", self.destroy)
        # Connect the ok_button to file_ok_sel method
        self.filew.ok_button.connect("clicked", self.file_ok_sel)
    
        # Connect the cancel_button to destroy the widget
        self.filew.cancel_button.connect("clicked",
                                         lambda w: self.filew.destroy())
    
        self.filew.show()


class GtkOpenDBFileChooser:
    # Get the selected filename and print it to the console
    def file_ok_sel(self, w):
        filename = self.filew.get_filename()
        self.parent.pdb.loadfile(filename)
        self.filew.destroy()

    def destroy(self, widget):
        self.filew.destroy()

    def __init__(self,parent):
        # Create a new file selection widget
        self.parent = parent
        self.filew   = gtk.FileSelection("File selection")

        self.filew.connect("destroy", self.destroy)
        # Connect the ok_button to file_ok_sel method
        self.filew.ok_button.connect("clicked", self.file_ok_sel)
    
        # Connect the cancel_button to destroy the widget
        self.filew.cancel_button.connect("clicked",
                                         lambda w: self.filew.destroy())
    
        self.filew.show()

class GtkSaveDBFileChooser:
    # Get the selected filename and print it to the console
    def file_ok_sel(self, w):
        filename = self.filew.get_filename()
        self.parent.pdb.savefile(filename)
        self.filew.destroy()

    def destroy(self, widget):
        self.filew.destroy()

    def __init__(self,parent):
        # Create a new file selection widget
        self.parent = parent
        self.filew   = gtk.FileSelection("File selection")

        self.filew.connect("destroy", self.destroy)
        # Connect the ok_button to file_ok_sel method
        self.filew.ok_button.connect("clicked", self.file_ok_sel)
    
        # Connect the cancel_button to destroy the widget
        self.filew.cancel_button.connect("clicked",
                                         lambda w: self.filew.destroy())
    
        # Lets set the filename, as if this were a save dialog,
        # and we are giving a default filename
        self.filew.set_filename(expandvars("$HOME/paramdb.xml"))
    
        self.filew.show()

def main():
    gtk.main()
    return 0

if __name__ == "__main__":
    GtkFileChooser(None)
    main()
