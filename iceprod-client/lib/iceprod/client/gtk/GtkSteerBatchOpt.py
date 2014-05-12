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
from GtkSteering import GtkSteering
import logging

logger = logging.getLogger('GtkSteerBatchOpt')

class GtkSteerBatchOpt(GtkSteering):

    def delete_steering(self, b):
		sel = self.tv.get_selection()
		model, iter = sel.get_selected() 
		path = model.get_path(iter)
		otype = model.get_value(iter, 0)
		oname = model.get_value(iter, 1)
		self.steering.RemoveBatchOpt((otype,oname))
		self.showsteerings()

    def add_steering(self, b):
	  	"""
		add steering batch option
		"""
	  	row = []
		batchopt = BatchOpt()
		batchopt.SetType('*')
		batchopt.SetName('new_batchoption')
		batchopt.SetValue('NULL')
		self.steering.AddBatchOpt(batchopt)

	  	row.append(batchopt.GetType())
	  	row.append(batchopt.GetName())
	  	row.append(batchopt.GetValue())
	  	i0 = self.liststore.append(row)
		sel = self.tv.get_selection()
		sel.select_iter(i0)
		

    def __init__(self,steering):
        GtkSteering.__init__(self,steering)

        self.b0.set_label('Add Option')
        self.b1.set_label('Delete Option')
        self.tv.column[0].set_title('I3Queue Name')
        self.tv.column[1].set_title('Option')
        self.tv.cell[0].set_property( 'editable', True)

        self.show_all()
        
    def reload(self,steering):
	  self.steering = steering
	  self.showsteerings()

    def showsteerings(self):
	  self.liststore.clear()
	  for b in self.steering.GetBatchOpts():

	  	row = [b.GetType(),b.GetName(),b.GetValue()]
	  	print row
	  	self.liststore.append(row)


    def cell_edit( self, cell, path, new_text,model,col ):
        """
        Canges the value of the batch option
        """
        logger.debug("Change '%s' to '%s'" % (model[path][col], new_text))
        logger.debug("%s,%s" % (path,col) )

        row = model[path]
        otype = row[0]
        oname = row[1]
        batchopt =  self.steering.GetBatchOpt((otype,oname))
        if not batchopt:
			logger.error("batch option %s,%s not found" % (otype,oname))
			return
        if (col == 0):
			self.steering.RemoveBatchOpt((otype,oname))
			batchopt.SetType(new_text)
			self.steering.AddBatchOpt(batchopt)
        elif (col == 1):
			self.steering.RemoveBatchOpt((otype,oname))
			batchopt.SetName(new_text)
			self.steering.AddBatchOpt(batchopt)
        elif (col == 2):
			batchopt.SetValue(new_text)
        else:
			logger.error("unknown column %s" % col)
        row[col] = u'%s' % new_text


