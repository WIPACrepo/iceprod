#!/bin/env python
#   copyright  (c) 2005
#   the icecube collaboration
#   $Id: $
#
#   @version $Revision: $
#   @date $Date: $
#   @author Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
#    @brief icetray connections frame for GtkIcetraConfig application
#########################################################################
from iceprod.core.xmlparser import IceTrayXMLParser
from iceprod.core.xmlwriter import IceTrayXMLWriter
from iceprod.core.dataclasses import *
from cPickle import loads,dumps
import sys,os,traceback
import getpass
import getopt
import xmlrpclib

class i3SOAPClient:

    def __init__(self,url=None,geturl=lambda:'https://condor.icecube.wisc.edu:9080'):
        self.url = url
        self.geturl = geturl
        self.printtext = lambda x: sys.stdout.write(x+"\n")
        self.connected = False
        self.cached = True 

    def SetPrinter(self,printer):
        self.printtext = printer

    def _print(self,text):
        if self.printtext and isinstance(text,str):
            self.printtext(text)
        else:
            print text

    def checkURL(self,url):
        return url.startswith('http://') or url.startswith('https://')

    def connect(self):
        if not self.connected:
            if not self.url:
               self.url = self.geturl()
            if self.checkURL(self.url):
               self.server = xmlrpclib.ServerProxy(self.url)
               self.connected = True
            else:
               self.printtext("Please specify a valid url: %s" % self.url)
        return self.connected

    def submit(self,i3steering,username,passwd,prodflag=False,submitter="%s@%s" % (getpass.getuser(),os.uname()[1])):

        if not self.connect(): return None

        try:
            if prodflag:
               status,i3q_pkl,ex = self.server.submit(
                    dumps(i3steering), 
                    username,passwd,submitter,prodflag)
               self._print(status)
               if (i3q_pkl) and loads(i3q_pkl):
                  return loads(i3q_pkl)
               elif ex:
                  ex = loads(ex)
                  self._print("Remote Exception: %s" % str(ex))
                  return None

            # non-production
            pmaxjobs = i3steering.GetParameter('MAXJOBS')
            maxjobs  = int(pmaxjobs.GetValue())
            stepsize = 20

            cookie = I3Cookie()
            for i in range(0,maxjobs,stepsize):
               status,i3q_pkl,ex = self.server.submit(
                    dumps(i3steering), 
                    username,
                    passwd,
                    submitter,
                    prodflag,
                    i, min(i+stepsize,maxjobs),
                    cookie.dataset_id)
               self._print(status)

               if (i3q_pkl) and loads(i3q_pkl):
                  i3q = loads(i3q_pkl)
                  cookie.dataset_id = i3q.dataset_id
                  for job_id in i3q.GetJobIds():
                      cookie.AddJobId(job_id)
               elif ex:
                  ex = loads(ex)
                  self._print("Remote Exception: %s" % str(ex))
                  return None
            return cookie

        except Exception, e:
            self._print("Caught exception: %s " % str(e))
            traceback.print_exc(file=sys.stdout)
            return None

    def enqueue(self,i3steering,username,passwd,submitter=getpass.getuser()):

        if not self.connect(): return None
        return self.server.enqueue(
                    dumps(i3steering), username,passwd,submitter)


    def check_q(self,i3q,username,password):

        if not self.connect(): return None
        try:
            self._print(self.server.checkjobs(dumps(i3q),username,password))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_remove(self,i3q,username,password,job=-1):

        if not self.connect(): return None
        try:
            self._print(self.server.queue_remove(dumps(i3q),username,password))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_suspend(self,username,password,dataset,job=-1):
        if not self.connect(): return None
        try:
            self._print(self.server.queue_suspend(username,password,dataset,job))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_resume(self,username,password,dataset,job=-1):
        if not self.connect(): return None
        try:
            self._print(self.server.queue_resume(username,password,dataset,job))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_reset(self,username,password,dataset,job=-1):
        if not self.connect(): return None
        try:
            self._print(self.server.queue_reset(username,password,dataset,job))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_setstatus(self,username,password,dataset,job,status):
        if not self.connect(): return None
        try:
            self._print(self.server.queue_setstatus(username,password,dataset,job,status))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_dataset_getstatus(self,dataset):
        if not self.connect(): return None
        try:
            for item in loads(self.server.getdatasetstatus(dataset)):
                #for key,value in item.items():
                self._print( "Dataset %s status: %s" % (item['dataset_id'],item['status']))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_dataset_setstatus(self,username,password,dataset,status):
        if not self.connect(): return None
        try:
            self._print(self.server.queue_dataset_setstatus(username,password,dataset,status))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_retire(self,username,password,dataset):
        """
        set status of dataset to 'OBSOLETE' and change the subcategory
        in DIF_Plus
        """
        if not self.connect(): return None
        try:
            self._print(self.server.queue_retire(username,password,dataset))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))


    def q_clean(self,username,password,dataset):
        if not self.connect(): return None
        try:
            self._print(self.server.queue_clean(username,password,dataset))
        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_delete(self,username,password,dataset):
        if not self.connect(): return None
        try:
            self._print(self.server.queue_delete(username,password,dataset))
        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_finish(self,username,password,dataset):
        if not self.connect(): return None
        try:
            self._print(self.server.queue_dataset_finish(username,password,dataset))
        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_daemon_suspend(self,username,password,grid,daemon):
        if not self.connect(): return None
        try:
            self._print(self.server.daemon_suspend(username,password,grid,daemon))
        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_daemon_resume(self,username,password,grid,daemon):
        if not self.connect(): return None
        try:
            self._print(self.server.daemon_resume(username,password,grid,daemon))
        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_grid_suspend_dataset(self,username,password,grid,dataset):
        if not self.connect(): return None
        try:
            self._print(self.server.grid_suspend_dataset(username,password,grid,dataset,1))
        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_grid_resume_dataset(self,username,password,grid,dataset):
        if not self.connect(): return None
        try:
            self._print(self.server.grid_suspend_dataset(username,password,grid,dataset,0))
        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_grid_add_dataset(self,username,password,grid,dataset):
        if not self.connect(): return None
        try:
            self._print(self.server.grid_add(username,password,grid,dataset))
        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def q_status(self,dataset,job=-1):
        if not self.connect(): return None
        num = 0
        for item in loads(self.server.getstatus(dataset,job)):
            for key,value in item.items():
                if value and value != "0" and value != 0:
                    self._print( "%s: %s" % (key,value))
                    num += 1
        if num == 0:
            self._print("No jobs for dataset %d" % dataset)

    def q_validate(self,username,password,dataset,valid=True):
        if not self.connect(): return None
        try:
            self._print(self.server.queue_validate(username,password,dataset,valid))

        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def loaddict(self,odict,username,password,dataset_id):
        if not self.connect(): return None
        try:
            self._print(self.server.loaddict(
                                dumps(odict),
                                username,password,dataset_id))
        except Exception, e:
            self._print("Caught exception: %s " % str(e))

    def authenticate(self,username,password):

        if not self.connect(): return None
        try:
            return self.server.authenticate(username,password)

        except Exception, e:
            self._print("Caught exception: %s " % str(e))
            return False

    def printsummary(self,days): 
        if not self.connect(): return None
        return self.server.printsummary(days)

    def show_dataset_table(self,search_string=""): 
        if not self.connect(): return None
        try:
            return loads(self.server.showrunlist(search_string))
        except Exception,e:
            self._print("Caught exception: %s " % str(e))
        return []

    def download_config(self,dataset,defaults=False,descriptions=False): 
        if not self.connect(): return None
        try:
          return loads(self.server.download_config(dataset,defaults,descriptions))
        except Exception,e:
            self._print("Caught exception: %s " % str(e))

    def check_server(self): 
        if not self.connect(): return None
        return loads(self.server.check_connection())

    def GetSimCategories(self): 
        if not self.connect(): return None
        return loads(self.server.get_simcat_categories())

    # ParamDB wrappers
    def SwitchMetaProject(self,iconfig,id,name,version): 
        if not self.connect(): return None
        return loads(self.server.SwitchMetaProject(dumps(iconfig),id,name,loads(version)))

    def GetMetaProjects(self): 
        if not self.connect(): return None
        return loads(self.server.GetMetaProjects())

    def GetProjectsSM(self,module,metaproj):
        if not self.connect(): return None
        return loads(self.server.GetProjectsSM(dumps(module),dumps(metaproj)))

    def GetProjectsMM(self,module,metaproj):
        if not self.connect(): return None
        return loads(self.server.GetProjectsMM(dumps(module),dumps(metaproj)))

    def GetProjects(self,metaproject_id):
        if not self.connect(): return None
        return loads(self.server.GetProjects(metaproject_id))

    def GetProjectDependencies(self,project_id,metaproject_id): 
        if not self.connect(): return None
        return loads(self.server.GetProjectDependencies(project_id,metaproject_id))

    def GetServices(self,project_id): 
        if not self.connect(): return None
        return loads(self.server.GetServices(project_id))

    def GetServicesP(self,name,version): 
        if not self.connect(): return None
        return loads(self.server.GetServicesP(name,dumps(version)))

    def GetModules(self,project_id): 
        if not self.connect(): return None
        return loads(self.server.GetModules(project_id))

    def GetModulesP(self,name,version): 
        if not self.connect(): return None
        return loads(self.server.GetModulesP(name,dumps(version)))

    def GetIceProdModules(self): 
        if not self.connect(): return None
        return loads(self.server.GetIceProdModules())

    def GetParameters(self,module_id): 
        if not self.connect(): return None
        return loads(self.server.GetParameters(module_id))

    def fetch_metaproject_list(self): 
        if not self.connect(): return None
        return loads(self.server.fetch_metaproject_list())

    def fetch_project_list(self,metaproject_id): 
        if not self.connect(): return None
        return loads(self.server.fetch_project_list(metaproject_id))

    def fetch_project(self,id):
        if not self.connect(): return None
        return loads(self.server.fetch_project(id))

    def fetch_project_id(self,pname,pversion):
        if not self.connect(): return None
        return loads(self.server.fetch_project_id(name,dumps(pversion)))

    def fetch_service_id(self,service,pid): 
        if not self.connect(): return None
        return self.server.fetch_service_id(dumps(service),pid)

    def fetch_module_id(self,module,mid): 
        if not self.connect(): return None
        return self.server.fetch_module_id(dumps(module),mid)

    def fetch_project_dependencies(self,project_id,metaproject_id): 
        if not self.connect(): return None
        return loads(self.server.fetch_project_dependencies(project_id,metaproject_id))

    def fetch_modules_from_project_id(self,project_id):
        if not self.connect(): return None
        return loads(self.server.fetch_modules_from_project_id(project_id))

    def fetch_modules_for_project(self,name,version): 
        if not self.connect(): return None
        return loads(self.server.fetch_modules_for_project(name,dumps(version)))

    def fetch_services_for_project(self,name,version): 
        if not self.connect(): return None
        return loads(self.server.fetch_services_for_project(name,dumps(version)))

    def fetch_module_parameters(self,module_id): 
        if not self.connect(): return None
        return loads(self.server.fetch_module_parameters(module_id))

    def fetch_service_parameters(self,module_id): 
        if not self.connect(): return None
        return loads(self.server.fetch_service_parameters(module_id))

