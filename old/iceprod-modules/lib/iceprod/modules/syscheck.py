
#!/bin/env python
#
"""
 Interface for configuring pre/post icetray scripts

 copyright  (c) 2005 the icecube collaboration

 @version: $Revision: $
 @date: $Date: $
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import os
import re
import sys
import math
import dircache
import time
import string
import shutil
import cPickle
from ipmodule import IPBaseClass
from iceprod.core import functions
import logging

class FindJava(IPBaseClass):
	"""
	SETUP JAVA ENVIRONMENT
    find any java installed on system
	"""

	def __init__(self):
	    IPBaseClass.__init__(self)
	    self.AddParameter(
                        'SearchPath',
                        'Colon separated list of directories in which to search', 
                        '$JAVA_HOME:/usr/java:/usr/local/java')
	    self.logger = logging.getLogger('iceprod::RenameFile')


	def Execute(self,stats):
	    if not IPBaseClass.Execute(self,stats): return 0
	    javasearchpath = self.GetParameter('SearchPath')
	    java = functions.findjava(javasearchpath)
	    if not java: 
	       raise Exception,"java not found in "+ javasearchpath 
	    functions.myputenv('JAVA_HOME',java[0])
	    print expandvars("using java in $JAVA_HOME")
	    return 0
