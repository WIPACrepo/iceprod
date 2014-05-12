#!/bin/env python
# -*- coding: utf-8 -*-
"""
 Interface for configuring IceProd 1.x modules

 copyright  (c) 2011 the icecube collaboration

 @version: $Revision: $
 @date: $Date: $
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
 @author: David Schultz <david.schultz@icecube.wisc.edu>
"""

import logging
from collections import OrderedDict


class IPBaseClass:
    """
    This class provides an interface for preprocessing files in iceprod
    """

    def __init__(self):
        if not hasattr(self,'parameters'):
           self.parameters    = OrderedDict()
           self.description   = OrderedDict()
           self.AddParameter("execute","boolean condition to execute", True)

        self.status        = 0
        self.parser        = None

        # Aggregate CPU times
        self.realtime        = 0.0
        self.usertime        = 0.0
        self.systime        = 0.0

        self.logger = logging.getLogger('IPBaseClass')

    def SetParser(self, parser):
        """
        Set the ExpParser object
        """
        self.parser        = parser

    def AddParameter(self, param,description, value):
        """
        Add parameter value for plugin
        """
        self.parameters[param.lower()]  = value
        self.description[param.lower()] = description

    def GetParameter(self, param):
        """
        Get parameter value for plugin
        """
        if not self.parameters.has_key(param.lower()):
            raise Exception, "Attemting to get parameter %s not added by %s" % (param,self)
        return self.parameters[param.lower()]

    def SetParameter(self, param, value):
        """
        Set parameter value for plugin
        """
        if not self.parameters.has_key(param.lower()):
            print self.ShowParameters()
            raise Exception, "param %s was configured but not added by %s" % (param,self)
        self.parameters[param.lower()] = value
        self.logger.info("%s:%s" %(param,value))
        return self


    def Execute(self,stats):
        self.logger.info("execute %s: %s" % (self.__class__.__name__,self.GetParameter("execute")))
        return self.GetParameter("execute")

    def ShowParameters(self):
        return zip(
            self.parameters.keys(),
            map(self.parameters.get,self.parameters.keys()),
            map(self.description.get,self.parameters.keys())
            )

    def Finish(self,stats={}):
        self.logger.info("finish %s: %s" % (self.__class__.__name__,self.GetParameter("execute")))
        return 0

class Hello(IPBaseClass):

    def __init__(self):
        IPBaseClass.__init__(self)
        self.AddParameter("greeting","String to write to screen","Hello World")

    def Execute(self,stats):
        if not IPBaseClass.Execute(self,stats): return 0
        print self.GetParameter("greeting")
        return 0

class GoodBye(IPBaseClass):

    def __init__(self):
        IPBaseClass.__init__(self)
        self.AddParameter("greeting","String to write to screen","Good bye World")

    def Execute(self,stats):
        if not IPBaseClass.Execute(self,stats): return 0
        print self.GetParameter("greeting")
        return 0

class Test_old(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    
    def Execute(self,stats):
        if not IPBaseClass.Execute(self,stats): return 0
        return 'Test_old IPBaseClass'




