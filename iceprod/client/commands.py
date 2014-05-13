"""
 Command library for iceprodsh
 
 copyright  (c) 2011 the icecube collaboration
 
 @version: $Revision: $
 @date: $Date: $
 @author: David Schultz <dschultz@icecube.wisc.edu>
"""
#from os.path import expandvars
#from ConfigParser import ConfigParser,SafeConfigParser

#from iceprod.core.xmlparser import IceTrayXMLParser
#from iceprod.core.xmlwriter import IceTrayXMLWriter
#from iceprod.core.dataclasses import Steering
#from iceprod.client.soaptrayclient import i3SOAPClient


class Command(object):
    """Command: prototype command"""
    shortdoc = None
    def Execute(self,shell):
        pass
        
    numArgs = 0
    def CheckArgs(self,args):
        # fail if numArgs not present
        if len(args) < self.numArgs:
            return "Please specify all arguments"
        self.args = args
        return None
        
    def _getDatasetJob(self):
        """Convert arguments to dataset,job pairs"""
        dataset_job = []
        for a in self.args:
            dj = a.split('.')
            datasets = []
            jobs = []
            if dj[0].find('-') != -1:
                # dataset range
                d = dj[0].split('-')
                if len(d) < 2 or int(d[0]) > int(d[1]):
                    print "Invalid range for dataset"
                    return []
                datasets = range(int(d[0]),int(d[1])+1)
            elif dj[0].find(',') != -1:
                # dataset list
                datasets = map(int,dj[0].split(','))
            else:
                # single dataset
                datasets.append(int(dj[0]))
            if len(dj) > 1:
                # get job ids
                if dj[1].find('-') != -1:
                    # job range
                    d = dj[1].split('-')
                    if len(d) < 2 or int(d[0]) > int(d[1]):
                        print "Invalid range for job"
                        return []
                    jobs = range(int(d[0]),int(d[1])+1)
                elif dj[1].find(',') != -1:
                    # job list
                    jobs = map(int,dj[1].split(','))
                else:
                    # single job
                    jobs.append(int(dj[1]))
            else:
                jobs.append(-1)
            for d in datasets:
                for j in jobs:
                    dataset_job.append((d,j))            
        return dataset_job
        
    def _getDatasetJobStatus(self):
        """Convert arguments to dataset,job,status tuples"""
        args2 = []
        name = None
        for a in self.args:
            if name is None:
                name = a
            else:
                args2.append((name,a))
                name = None
        dataset_job = []
        for a,status in args2:
            dj = a.split('.')
            datasets = []
            jobs = []
            if dj[0].find('-') != -1:
                # dataset range
                d = dj[0].split('-')
                if len(d) < 2 or int(d[0]) > int(d[1]):
                    print "Invalid range for dataset"
                    return []
                datasets = range(int(d[0]),int(d[1])+1)
            elif dj[0].find(',') != -1:
                # dataset list
                datasets = map(int,dj[0].split(','))
            else:
                # single dataset
                datasets.append(int(dj[0]))
            if len(dj) > 1:
                # get job ids
                if dj[1].find('-') != -1:
                    # job range
                    d = dj[1].split('-')
                    if len(d) < 2 or int(d[0]) > int(d[1]):
                        print "Invalid range for job"
                        return []
                    jobs = range(int(d[0]),int(d[1])+1)
                elif dj[1].find(',') != -1:
                    # job list
                    jobs = map(int,dj[1].split(','))
                else:
                    # single job
                    jobs.append(int(dj[1]))
            else:
                jobs.append(-1)
            for d in datasets:
                for j in jobs:
                    dataset_job.append((d,j,status))            
        return dataset_job
        
    def _getGridDaemon(self):
        """Convert arguments to grid,daemon pairs"""
        grid_daemon = []
        for a in self.args:
            gd = a.rsplit('.',1)
            if gd[0].isdigit():
                gd[0] = int(gd[0])
            if len(gd) > 1:
                grid_daemon.append((gd[0],gd[1]))
            else:
                grid_daemon.append((gd[0],'all'))
        return grid_daemon
        
    def _getGridDataset(self):
        """Convert arguments to grid,dataset pairs"""
        args2 = []
        grid = None
        for a in self.args:
            if grid is None:
                grid = a
            else:
                args2.append((grid,a))
                grid = None
        grid_dataset = []
        for grid,dataset in args2:
            grids = []
            datasets = []
            if grid.find(',') != -1:
                # grid list
                grids = grid.split(',')
            else:
                # single grid
                grids.append(grid)
            # get dataset ids
            if dataset.find('-') != -1:
                # dataset range
                d = dataset.split('-')
                if len(d) < 2 or int(d[0]) > int(d[1]):
                    print "Invalid range for dataset"
                    return []
                datasets = range(int(d[0]),int(d[1])+1)
            elif dataset.find(',') != -1:
                # dataset list
                datasets = map(int,dataset.split(','))
            else:
                # single dataset
                datasets.append(int(dataset))
            for g in grids:
                if g.isdigit():
                    g = int(g)
                for d in datasets:
                    grid_dataset.append((g,d))
        return grid_dataset

class set(Command):
    """Command: set <variable> <value> [<variable> <value>]
    
    Set local variable(s) in iceprodsh.
    Can set multiple variables at once.
    
    Arguments:
      <variable>  The variable to set
                  (username, url, production, test, editor)
      <value>     The value to set the variable to
      
    Returns:
      Echo variables and new values out to shell.
      Print error on unknown variable.
    """
    numArgs = 2
    shortdoc = "set <variable> <value> : Set a local variable"
    def Execute(self,shell):
        # format options from self.args list
        options = {}
        name = None
        for a in self.args:
            if name is None:
                name = a
            else:
                options[name] = a
                name = None
        # evaluate options
        ret = False
        for opt in options.iterkeys():
            if opt == 'username': 
                shell.username = options[opt]
                shell.password = None
                shell.cfg.set('iceprodsh','username',options[opt])
            elif opt == 'url': 
                from iceprod.client.soaptrayclient import i3SOAPClient
                shell.url = options[opt]
                shell.cfg.set('iceprodsh','url',options[opt])
                print "now connecting to %s" % shell.url
                shell.client = i3SOAPClient(url=shell.url)
            elif opt == 'production': 
                shell.production = int(options[opt])
            elif opt == 'test': 
                shell.test = int(options[opt])
            elif opt == 'editor': 
                shell.editor = options[opt]
            elif opt == 'prefix': 
                shell.prefix = options[opt]
            else:
                print "unknown option '%s'" % opt
                continue
            print opt,'set to',options[opt]
            ret = True
        return ret
        # return True on success, False on failure
    
    def CheckArgs(self,args):
        # must have pairs of arguments
        if len(args)%2 == 1:
            return "Invalid arguments.  Must be name value pairs."
        return super(set,self).CheckArgs(args)

class get(Command):
    """Command: get <variable> [<variable>]
    
    Get local variable(s) in iceprodsh.
    Can get multiple variables at once.
    
    Arguments:
      <variable>  The variable to get
                  (username, url, production, test, editor)
      
    Returns:
      Print variables and values out to shell.
      Print error on unknown variables.
    """
    shortdoc = "get <variable> : Get a local variable"
    numArgs = 1
    def Execute(self,shell):        
        # format options from args list
        print "get():",self.args
        options = {}
        for a in self.args:
            options[a] = None
        # evaluate options
        for opt in options.iterkeys():
            if opt == 'username': 
                options[opt] = shell.username
            elif opt == 'url': 
                options[opt] = shell.url
            elif opt == 'production': 
                options[opt] = str(shell.production)
            elif opt == 'test': 
                options[opt] = str(shell.test)
            elif opt == 'editor': 
                options[opt] = shell.editor
            else:
                print "unknown option '%s'" % opt
                continue
            print opt,'=',options[opt]
        return options
        # return dict of <variable>:<value> on success, empty dict on failure

class suspend(Command):
    """Command: suspend <dataset_id>[.<job>]
   
   Suspend a whole dataset or a specific job from a dataset.
   Can suspend multiple jobs or datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset to suspend.
     [.<job>]      (Optional) Specify the job within the dataset to suspend.
          
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset or job id is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Suspend whole dataset  (dataset 1234, all jobs)
        suspend 1234
     
     Suspend individual jobs from different datasets
     (dataset 1234, job 10 and dataset 4321, job 20)
        suspend 1234.10 4321.20
     
     Suspend multiple datasets and jobs using commas
     (datasets 1234, 1243 and jobs 1, 3, and 5)
        suspend 1234,1243.1,3,5
     
     Suspend multile datasets and jobs using ranges
     (datasets 1234 - 1235, jobs 1 - 5)
        suspend 1234-1235.1-5
    """
    shortdoc = "suspend <dataset_id>[.<job>] : Suspend jobs."
    numArgs = 1
    def Execute(self,shell):        
        # get dataset job pairs
        dataset_job = self._getDatasetJob()
    
        # for each dataset and job, suspend
        ret = True
        for dataset,job in dataset_job:
            self._suspend(shell,dataset,job)
        return ret
    
    def _suspend(self,shell,dataset,job):
        #print "suspend dataset",dataset,"job",job
        shell.auth()
        shell.client.q_suspend(shell.username, shell.password, dataset,job)

class resume(Command):
    """Command: resume <dataset_id>[.<job>]
   
   Resume a dataset or a specific job from a dataset.
   Can resume multiple jobs or datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset to resume.
     [.<job>]      (Optional) Specify the job within the dataset to resume.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset or job id is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Resume whole dataset  (dataset 1234, all jobs)
        resume 1234
     
     Resume individual jobs from different datasets
     (dataset 1234, job 10 and dataset 4321, job 20)
        resume 1234.10 4321.20
     
     Resume multiple datasets and jobs using commas
     (datasets 1234, 1243 and jobs 1, 3, and 5)
        resume 1234,1243.1,3,5
     
     Resume multile datasets and jobs using ranges
     (datasets 1234 - 1235, jobs 1 - 5)
        resume 1234-1235.1-5
    """
    shortdoc = "resume <dataset_id>[.<job>] : Resume jobs."
    numArgs = 1
    def Execute(self,shell):        
        # get dataset job pairs
        dataset_job = self._getDatasetJob()
    
        # for each dataset and job, resume
        ret = True
        for dataset,job in dataset_job:
            self._resume(shell,dataset,job)
        return ret
    
    def _resume(self,shell,dataset,job):
        #print "resume dataset",dataset,"job",job
        shell.auth()
        shell.client.q_resume(shell.username, shell.password, dataset,job)

class reset(Command):
    """Command: reset <dataset_id>[.<job>]
   
   Reset a dataset or a specific job from a dataset.
   Can reset multiple jobs or datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset to reset.
     [.<job>]      (Optional) Specify the job within the dataset to reset.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset or job id is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Reset whole dataset  (dataset 1234, all jobs)
        reset 1234
     
     Reset individual jobs from different datasets
     (dataset 1234, job 10 and dataset 4321, job 20)
        reset 1234.10 4321.20
     
     Reset multiple datasets and jobs using commas
     (datasets 1234, 1243 and jobs 1, 3, and 5)
        reset 1234,1243.1,3,5
     
     Reset multile datasets and jobs using ranges
     (datasets 1234 - 1235, jobs 1 - 5)
        reset 1234-1235.1-5
    """
    shortdoc = "reset <dataset_id>[.<job>] : Reset jobs."
    numArgs = 1
    def Execute(self,shell):        
        # get dataset job pairs
        dataset_job = self._getDatasetJob()
    
        # for each dataset and job, reset
        ret = True
        for dataset,job in dataset_job:
            self._reset(shell,dataset,job)
        return ret
    
    def _reset(self,shell,dataset,job):
        #print "reset dataset",dataset,"job",job
        shell.auth()
        shell.client.q_reset(shell.username, shell.password, dataset,job)

class status(Command):
    """Command: status <dataset_id>[.<job>]
            or
         getstatus <dataset_id>[.<job>]
   
   Get the status of all jobs or a specific job from a dataset.
   Can get the status of multiple jobs over multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset.
     [.<job>]      (Optional) Specify the job within the dataset.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset or job id is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Get status of whole dataset  (dataset 1234, all jobs)
        status 1234
     
     Get status of individual jobs from different datasets
     (dataset 1234, job 10 and dataset 4321, job 20)
        status 1234.10 4321.20
     
     Get status of multiple datasets and jobs using commas
     (datasets 1234, 1243 and jobs 1, 3, and 5)
        status 1234,1243.1,3,5
     
     Get status of multile datasets and jobs using ranges
     (datasets 1234 - 1235, jobs 1 - 5)
        status 1234-1235.1-5
    """
    shortdoc = "status <dataset_id>[.<job>] : Get status of jobs. AKA: getstatus"
    numArgs = 1
    def Execute(self,shell):
        # get dataset job pairs
        dataset_job = self._getDatasetJob()
    
        # for each dataset and job, get status
        ret = True
        for dataset,job in dataset_job:
            self._status(shell,dataset,job)
        return ret
    
    def _status(self,shell,dataset,job):
        #print "status dataset",dataset,"job",job
        shell.client.q_status(dataset,job)

class getstatus(status):
    __doc__ = status.__doc__
    shortdoc = "getstatus <dataset_id>[.<job>] : Get status of jobs."
    
class datasetstatus(Command):
    """Command: datasetstatus <dataset_id>
            or
         getdatasetstatus <dataset_id>
   
   Get the status of the dataset.
   Can get the status of multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Get status of single dataset  (dataset 1234)
        datasetstatus 1234
     
     Get status of multiple datasets using spaces
     (dataset 1234 and dataset 4321)
        datasetstatus 1234 4321
     
     Get status of multiple datasets using commas
     (datasets 1234, 1243)
        datasetstatus 1234,1243
     
     Get status of multile datasets using ranges
     (datasets 1234 - 1235)
        datasetstatus 1234-1235
    """
    shortdoc = "datasetstatus <dataset_id> : Get status of dataset. AKA: getdatasetstatus"
    numArgs = 1
    def Execute(self,shell):
        # get dataset job pairs
        dataset_job = self._getDatasetJob()
    
        # for each dataset and job, get status
        ret = True
        for dataset,job in dataset_job:
            self._datasetstatus(shell,dataset)
        return ret
    
    def _datasetstatus(self,shell,dataset):
        #print "status dataset",dataset
        shell.client.q_dataset_getstatus(dataset)

class getdatasetstatus(datasetstatus):
    __doc__ = datasetstatus.__doc__
    shortdoc = "getdatasetstatus <dataset_id> : Get status of dataset."
    
class setstatus(Command):
    """Command: setstatus <dataset_id>[.<job>] <status>
   
   Set the status of all jobs or a specific job from a dataset.
   Can set the status of multiple jobs over multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset.
     [.<job>]      (Optional) Specify the job within the dataset.
     <status>      Specify the status
                   (WAITING, QUEUEING, QUEUED, PROCESSING, OK, ERROR,
                    READYTOCOPY, COPYING, SUSPENDED, RESET, FAILED,
                    COPIED, EVICTED, CLEANING, IDLE)
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset or job id is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Set status of all jobs in dataset to RESET  (dataset 1234, all jobs)
        status 1234 RESET
     
     Set status of individual jobs from different datasets
     (dataset 1234, job 10 to waiting and dataset 4321, job 20 to RESET)
        status 1234.10 WAITING 4321.20 RESET
     
     Set status of multiple datasets and jobs using commas
     (jobs 1, 3, and 5 from datasets 1234 and 1243 to OK)
        status 1234,1243.1,3,5 OK
     
     Set status of multile datasets and jobs using ranges
     (datasets 1234 - 1235, jobs 1 - 5 to SUSPENDED)
        status 1234-1235.1-5 SUSPENDED
    """
    shortdoc = "setstatus <dataset_id>[.<job>] <status> : Set status of jobs."
    numArgs = 2
    def Execute(self,shell):
        # get dataset job pairs
        dataset_job = self._getDatasetJobStatus()
    
        # for each dataset and job, set status
        ret = True
        for dataset,job,status in dataset_job:
            self._status(shell,dataset,job,status)
        return ret
    
    def _status(self,shell,dataset,job,status):
        #print "status dataset",dataset,"job",job,"status",status
        shell.auth()
        shell.client.q_setstatus(shell.username,shell.password,dataset,job,status)
        
    def CheckArgs(self,args):
        # must have pairs of arguments
        if len(args)%2 == 1:
            return "Invalid arguments.  Must be grid dataset pairs."
        return super(setstatus,self).CheckArgs(args)

class setdatasetstatus(Command):
    """Command: setstatus <dataset_id> <status>
   
   Set the status of a dataset.
   Can set the status of multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset.
     <status>      Specify the status
                   (PROCESSING, COMPLETE, ERRORS, READYTOPUBLISH,
                   MOVING, OBSOLETE, READYTODELETE, DELETED, TEMPLATE)
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Set status of dataset to PROCESSING
        status 1234 PROCESSING
     
     Set status of multiple datasets to different states
     (dataset 1234 to PROCESSING and dataset 4321 to ERRORS)
        status 1234 PROCESSING 4321 ERRORS
     
     Set status of multiple datasets using commas
     (datasets 1234, 1243 to COMPLETE)
        status 1234,1243 COMPLETE
     
     Set status of multile datasets using ranges
     (datasets 1234 - 1235 to READYTOPUBLISH)
        status 1234-1235 READYTOPUBLISH
    """
    shortdoc = "setdatasetstatus <dataset_id> <status> : Set status of dataset."
    numArgs = 2
    def Execute(self,shell):
        # get dataset status pairs
        dataset_job = self._getDatasetJobStatus()
        
        # for each dataset, set status
        ret = True
        for dataset,job,status in dataset_job:
            self._status(shell,dataset,status)
        return ret
    
    def _status(self,shell,dataset,status):
        #print "status dataset",dataset,"status",status
        shell.auth()
        shell.client.q_dataset_setstatus(shell.username,shell.password,dataset,status)
        
    def CheckArgs(self,args):
        # must have pairs of arguments
        if len(args)%2 == 1:
            return "Invalid arguments.  Must be grid dataset pairs."
        return super(setdatasetstatus,self).CheckArgs(args)

class clean(Command):
    """Command: clean <dataset_id>
   
   Clean a dataset.
   Can clean multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset to clean.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Clean single dataset  (dataset 1234)
        clean 1234
     
     Clean multiple datasets using commas
     (datasets 1234, 1243)
        clean 1234,1243
     
     Clean multile datasets using ranges
     (datasets 1234 - 1235)
        clean 1234-1235
    """
    shortdoc = "clean <dataset_id> : Clean a dataset."
    numArgs = 1
    def Execute(self,shell):        
        # get datasets
        dataset_job = self._getDatasetJob()
    
        # for each dataset, clean
        ret = True
        for dataset,job in dataset_job:
            self._clean(shell,dataset)
        return ret
    
    def _clean(self,shell,dataset):
        #print "clean dataset",dataset
        shell.auth()
        shell.client.q_clean(shell.username, shell.password, dataset)

class finish(Command):
    """Command: finish <dataset_id>
   
   Finish a dataset.
   Can finish multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset to finish.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Finish single dataset  (dataset 1234)
        finish 1234
     
     Finish multiple datasets using commas
     (datasets 1234, 1243 and jobs 1, 3, and 5)
        finish 1234,1243
     
     Finish multile datasets using ranges
     (datasets 1234 - 1235, jobs 1 - 5)
        finish 1234-1235
    """
    shortdoc = "finish <dataset_id> : Finish a dataset."
    numArgs = 1
    def Execute(self,shell):        
        # get datasets
        dataset_job = self._getDatasetJob()
    
        # for each dataset, finish
        ret = True
        for dataset,job in dataset_job:
            self._finish(shell,dataset)
        return ret
    
    def _finish(self,shell,dataset):
        #print "finish dataset",dataset
        shell.auth()
        shell.client.q_finish(shell.username, shell.password, dataset)

class retire(Command):
    """Command: retire <dataset_id>
   
   Retire a dataset.  Send it to the archives.
   Can retire multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset to retire.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Retire single dataset  (dataset 1234)
        retire 1234
     
     Retire multiple datasets using commas
     (datasets 1234, 1243 and jobs 1, 3, and 5)
        retire 1234,1243
     
     Retire multile datasets using ranges
     (datasets 1234 - 1235, jobs 1 - 5)
        retire 1234-1235
    """
    shortdoc = "retire <dataset_id> : Retire a dataset."
    numArgs = 1
    def Execute(self,shell):        
        # get datasets
        dataset_job = self._getDatasetJob()
    
        # for each dataset, retire
        ret = True
        for dataset,job in dataset_job:
            self._retire(shell,dataset)
        return ret
    
    def _retire(self,shell,dataset):
        #print "retire dataset",dataset
        shell.auth()
        shell.client.q_retire(shell.username, shell.password, dataset)

class nuke(Command):
    """Command: nuke <dataset_id>
   
   Nuke (delete) a dataset.  Similar to 'hide'+'clean', but more
   destructive.  Can nuke multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset to nuke.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Nuke single dataset  (dataset 1234)
        nuke 1234
     
     Nuke multiple datasets using commas
     (datasets 1234, 1243 and jobs 1, 3, and 5)
        nuke 1234,1243
     
     Nuke multile datasets using ranges
     (datasets 1234 - 1235, jobs 1 - 5)
        nuke 1234-1235
    """
    shortdoc = "nuke <dataset_id> : Nuke (delete) a dataset."
    numArgs = 1
    def Execute(self,shell):        
        # get datasets
        dataset_job = self._getDatasetJob()
    
        # for each dataset, nuke
        ret = True
        for dataset,job in dataset_job:
            self._nuke(shell,dataset)
        return ret
    
    def _nuke(self,shell,dataset):
        #print "nuke dataset",dataset
        shell.auth()
        shell.client.q_delete(shell.username, shell.password, dataset)

class valid(Command):
    """Command: valid <dataset_id>
             or
         show <dataset_id>
   
   Validate a dataset and let it show up in iceprod.
   Can validate multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset to validate.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset id is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Validate single dataset  (dataset 1234)
        valid 1234
     
     Validate multiple datasets using commas
     (datasets 1234, 1243 and jobs 1, 3, and 5)
        valid 1234,1243
     
     Validate multile datasets using ranges
     (datasets 1234 - 1235, jobs 1 - 5)
        valid 1234-1235
    """
    shortdoc = "valid <dataset_id> : Validate a dataset. AKA: show"
    numArgs = 1
    def Execute(self,shell):        
        # get datasets
        dataset_job = self._getDatasetJob()
    
        # for each dataset, validate
        ret = True
        for dataset,job in dataset_job:
            self._valid(shell,dataset,True)
        return ret
    
    def _valid(self,shell,dataset,state):
        #print "validate dataset",dataset,"state",state
        shell.auth()
        shell.client.q_validate(shell.username, shell.password, dataset,state)

class show(valid):
    __doc__ = valid.__doc__
    shortdoc = "show <dataset_id> : Validate a dataset. AKA: validate"

class invalid(valid):
    """Command: invalid <dataset_id>
             or
         nvalid <dataset_id>
             or
         hide <dataset_id>
   
   Invalidate a dataset and hide it from iceprod.
   Can invalidate multiple datasets at once.
   
   Arguments:
     <dataset_id>  Specify the dataset to invalidate.
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the dataset id is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Invalidate single dataset  (dataset 1234)
        invalid 1234
     
     Invalidate multiple datasets using commas
     (datasets 1234, 1243 and jobs 1, 3, and 5)
        invalid 1234,1243
     
     Invalidate multile datasets using ranges
     (datasets 1234 - 1235, jobs 1 - 5)
        invalid 1234-1235
    """
    shortdoc = "invalid <dataset_id> : Invalidate a dataset. AKA: nvalid,hide"
    numArgs = 1
    def Execute(self,shell):        
        # get datasets
        dataset_job = self._getDatasetJob()
    
        # for each dataset, invalidate
        ret = True
        for dataset,job in dataset_job:
            self._valid(shell,dataset,False)
        return ret

class nvalid(invalid):
    __doc__ = invalid.__doc__
    shortdoc = "nvalid <dataset_id> : Invalidate a dataset. AKA: invalid,hide"
    
class hide(invalid):
    __doc__ = invalid.__doc__
    shortdoc = "hide <dataset_id> : Invalidate a dataset. AKA: invalid,nvalid"

class startserver(Command):
    """Command: startserver <grid>[.<daemon>]
   
   Start a server or a specific daemon on the server.
   Can start multiple servers and daemons at once.
   
   Only works if main iceprodd daemon is running on the server,
   otherwise the change is only in the database.
    
   Arguments:
     <grid>      Specify the grid to start, either by name or id
     [.<daemon>] (Optional) Specify the daemon to start.
                  (all,soapdh,soapqueue,soaphist,soapmon)
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the grid or daemon is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Start GLOW
        startserver GLOW
           or
        startserver 1
     
     Start soapqueue on GLOW and soapdh on glow-test
        startserver GLOW.soapqueue glow-test.soapdh
        
     Start EGEE.Madison, or others with a dot in the name
     (must specify daemon to get grid name correct)
        startserver EGEE.Madison.all
    """
    shortdoc = "startserver <grid>[.<daemon>] : Start an iceprod server."
    numArgs = 1
    def Execute(self,shell):        
        # get grid, daemon pairs
        grid_daemon = self._getGridDaemon()
    
        # for each grid and daemon, start
        ret = True
        for grid,daemon in grid_daemon:
            self._start(shell,grid,daemon)
        return ret
    
    def _start(self,shell,grid,daemon):
        print "start grid",grid,"daemon",daemon
        shell.auth()
        shell.client.q_daemon_resume(shell.username, shell.password, grid, daemon)

class stopserver(Command):
    """Command: stopserver <grid>[.<daemon>]
   
   Stop a server or a specific daemon on the server.
   Can stop multiple servers and daemons at once.
   
   Only works if main iceprodd daemon is running on the server,
   otherwise the change is only in the database.
    
   Arguments:
     <grid>      Specify the grid to stop, either by name or id
     [.<daemon>] (Optional) Specify the daemon to stop.
                  (soapdh,soapqueue,soaphist,soapmon)
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the grid or daemon is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Stop GLOW
        stopserver GLOW
           or
        stopserver 1
     
     Stop soapqueue on GLOW and soapdh on glow-test
        stopserver GLOW.soapqueue glow-test.soapdh
    """
    shortdoc = "stopserver <grid>[.<daemon>] : Stop an iceprod server."
    numArgs = 1
    def Execute(self,shell):        
        # get grid, daemon pairs
        grid_daemon = self._getGridDaemon()
    
        # for each grid and daemon, stop
        ret = True
        for grid,daemon in grid_daemon:
            self._stop(shell,grid,daemon)
        return ret
    
    def _stop(self,shell,grid,daemon):
        print "stop grid",grid,"daemon",daemon
        shell.auth()
        shell.client.q_daemon_suspend(shell.username, shell.password, grid, daemon)


class includegrid(Command):
    """Command: includegrid <grid> <dataset_id>
   
   Resume a dataset on a grid.
   Can resume multiple datasets and grids at once.
    
   Arguments:
     <grid>        Specify the grid, either by name or id
     <dataset_id>  Specify dataset to act on
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the grid or dataset is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Resume 1234 on GLOW
        includegrid GLOW 1234
           or
        includegrid 1 1234
     
     Resume 1234 and 1243 on GLOW and glow-test
        includegrid GLOW,glow-test 1234,1243
    """
    shortdoc = "includegrid <grid> <dataset_id> : Resume a dataset on a grid."
    numArgs = 2
    def Execute(self,shell):        
        # get grid, dataset pairs
        grid_dataset = self._getGridDataset()
    
        # for each grid and dataset, resume
        ret = True
        for grid,dataset in grid_dataset:
            self._include(shell,grid,dataset)
        return ret
    
    def _include(self,shell,grid,dataset):
        print "include grid",grid,"dataset",dataset
        shell.auth()
        shell.client.q_grid_resume_dataset(shell.username, shell.password, grid, dataset)
    
    def CheckArgs(self,args):
        # must have pairs of arguments
        if len(args)%2 == 1:
            return "Invalid arguments.  Must be grid dataset pairs."
        return super(includegrid,self).CheckArgs(args)

class excludegrid(Command):
    """Command: excludegrid <grid> <dataset_id>
   
   Suspend a dataset on a grid
   Can suspend multiple datasets and grids at once.
    
   Arguments:
     <grid>        Specify the grid, either by name or id
     <dataset_id>  Specify dataset to act on
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the grid or dataset is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Suspend 1234 on GLOW
        excludegrid GLOW 1234
           or
        excludegrid 1 1234
     
     Suspend 1234 and 1243 on GLOW and glow-test
        excludegrid GLOW,glow-test 1234,1243
    """
    shortdoc = "excludegrid <grid> <dataset_id> : Suspend a dataset on a grid."
    numArgs = 2
    def Execute(self,shell):        
        # get grid, dataset pairs
        grid_dataset = self._getGridDataset()
    
        # for each grid and dataset, suspend
        ret = True
        for grid,dataset in grid_dataset:
            self._exclude(shell,grid,dataset)
        return ret
    
    def _exclude(self,shell,grid,dataset):
        print "exclude grid",grid,"dataset",dataset
        shell.auth()
        shell.client.q_grid_suspend_dataset(shell.username, shell.password, grid, dataset)
    
    def CheckArgs(self,args):
        # must have pairs of arguments
        if len(args)%2 == 1:
            return "Invalid arguments.  Must be grid dataset pairs."
        return super(excludegrid,self).CheckArgs(args)

class addgrid(Command):
    """Command: addgrid <grid> <dataset_id>
   
   Add a grid to a dataset
   Can add multiple grids to multiple datasets at once.
    
   Arguments:
     <grid>        Specify the grid, either by name or id
     <dataset_id>  Specify dataset to act on
     
   Returns:
     Returns the result of the mysql query (success or failure).
     Warning that if the grid or dataset is wrong it will likely
     print success because there was no mysql error.
     
   Examples:
     Add GLOW to 1234
        addgrid GLOW 1234
           or
        addgrid 1 1234
     
     Add GLOW to 1234 and glow-test to 4321
        addgrid GLOW 1234 glow-test 4321
        
     Add GLOW and glow-test to 1234 and 4321
        addgrid GLOW,glow-test 1234,4321
    """
    shortdoc = "addgrid <grid> <dataset_id> : Add a grid to a dataset."
    numArgs = 2
    def Execute(self,shell):        
        # get grid, dataset pairs
        grid_dataset = self._getGridDataset()
    
        # for each grid and dataset, add
        ret = True
        for grid,dataset in grid_dataset:
            self._add(shell,grid,dataset)
        return ret
    
    def _add(self,shell,grid,dataset):
        print "add grid",grid,"dataset",dataset
        shell.auth()
        shell.client.q_grid_add_dataset(shell.username, shell.password, grid, dataset)
    
    def CheckArgs(self,args):
        # must have pairs of arguments
        if len(args)%2 == 1:
            return "Invalid arguments.  Must be grid dataset pairs."
        return super(addgrid,self).CheckArgs(args)

class open(Command):
    """Command: open <filename>
   
   Open a steering file.
    
   Arguments:
     <filename>  File name to open
     
   Returns:
     No return if successful, error on failure.
     
   Examples:
     Open file dataset_1234.xml
        open dataset_1234.xml
    """
    shortdoc = "open <filename> : Open a steering file."
    numArgs = 1
    def Execute(self,shell):   
        from iceprod.core.xmlparser import IceTrayXMLParser
        from iceprod.core.dataclasses import Steering     
        shell.steering = Steering()
        shell.filename = self.args[0]
        myparser = IceTrayXMLParser(shell.steering)
        myparser.ParseFile(shell.filename,validate=shell.xmlvalidate)

class edit(Command):
    """Command: edit
   
   Edit the currently open steering file.
   (Must have previously opened or downloaded a steering file)
    
   Arguments: none
     
   Returns:
     Opens the steering file in the editor set by the
     environment variable EDITOR.  Default is vi.
    """
    shortdoc = "edit : Edit the currently open steering file."
    def Execute(self,shell):    
        import sys,os,time
        from iceprod.core.xmlparser import IceTrayXMLParser
        from iceprod.core.xmlwriter import IceTrayXMLWriter
        from iceprod.core.dataclasses import Steering
        if not shell.steering:
            print "No configuration loaded"
            return
        writer = IceTrayXMLWriter(shell.steering)
        tmpfile = ".iceprod.%u.tmp.xml" % time.time()
        writer.write_to_file(tmpfile)
        newsteering = None
        while not newsteering:
            os.system("%s %s" % (shell.editor,tmpfile))
            myparser = IceTrayXMLParser(Steering())
            newsteering = myparser.ParseFile(tmpfile,validate=shell.xmlvalidate)
            if not newsteering:
                print "Hit any key to continue."
                sys.stdin.readline()
        shell.steering = newsteering
        os.remove(tmpfile)

    def CheckArgs(self,args):
        if len(args) > 0:
            print "Ignoring arguments"
        return None

class close(Command):
    """Command: close
   
   Close the currently open steering file.
   (Does nothing if no file is open)
    
   Arguments: none
     
   Returns: none
    """
    shortdoc = "close : Close the currently open steering file."
    def Execute(self,shell):        
        shell.steering = None

    def CheckArgs(self,args):
        if len(args) > 0:
            print "Ignoring arguments"
        return None

class save(Command):
    """Command: save [<filename>]
   
   Save the currently open steering file.
   (Must have previously opened or downloaded a steering file)
    
   Arguments:
     <filename>  (Optional) Name of file to save to.  
                 If not specified, filename is either the
                 file it was opened from, the file it was
                 last saved to, or the dataset id if it
                 was downloaded.
     
   Returns:
     Saves steering to file.  No visible return.     
    """
    shortdoc = "save [<filename>] : Save the currently open steering file."
    def Execute(self,shell): 
        from iceprod.core.xmlwriter import IceTrayXMLWriter
        if not shell.steering:
            print "No configuration loaded"
            return
        if len(self.args) > 0:
            shell.filename = self.args[0]
        writer = IceTrayXMLWriter(shell.steering)
        writer.write_to_file(shell.filename)

class download(Command):
    """Command: download <dataset>
   
   Download a dataset (replaces currently open steering file)
    
   Arguments:
     <dataset>  Specify dataset id to download.
     
   Returns:
     Downloads steering from database and opens it.
    """
    shortdoc = "download <dataset> : Download a dataset."
    numArgs = 1
    def Execute(self,shell): 
        dataset = int(self.args[0])
        shell.steering = shell.client.download_config(dataset)
        shell.filename = 'dataset_%u.xml' % dataset

    def CheckArgs(self,args):
        # must have single integer argument
        s = super(download,self).CheckArgs(args)
        if s is not None:
            return s
        if not self.args[0].isdigit():
            return "Invalid argument. Must be integer."
        if len(args) > 1:
            print "Ignoring additional arguments.  Using only ",arg[0]
        return None

class summary(Command):
    """Command: summary [<days>]
   
   Print a summary of the production sites for the last few days.
    
   Arguments:
     <days>  (Optional) Specify the number of days to look back.
              Defaults to 7 days.
     
   Returns:
     Prints summary to terminal.
    """
    shortdoc = "summary [<days>] : Print a summary of the production sites."
    def Execute(self,shell): 
        if len(self.args) < 1:
            days = 7
        else:
            days = int(self.args[0])
        try:
            print shell.client.printsummary(days)
        except Exception,e:
            print e

    def CheckArgs(self,args):
        # can have single integer argument
        s = super(summary,self).CheckArgs(args)
        if s is not None:
            return s
        if len(self.args) > 0 and not self.args[0].isdigit():
            return "Invalid argument. Must be integer."
        if len(args) > 1:
            print "Ignoring additional arguments.  Using only ",arg[0]
        return None

class loadfiles(Command):
    """Command: loadfiles <rootdir> [<rootdir>] [regex=<regex>] [dataset=<dataset_id>]
   
   Get list of files in rootdir(s) and add them to database
   dictionary table if they match the regex.
    
   Arguments:
     <rootdir>     Directory of files
     regex=<regex> (Optional) A regex to match against
     regex=<regex> (Optional) A regex to match against
     
   Returns:
     Error message on failure.
    """
    shortdoc = "loadfiles <rootdir> [<rootdir>] [regex=<regex>] [dataset=<dataset_id>]"
    numArgs = 1 # must have at least one argument
    def Execute(self,shell): 
        import os, re
        from iceprod.core import odict
        odict = OrderedDict();
        if self.regex: cregex = re.compile(self.regex)
        filecount = 0
        for root in self.args:
            for path, dirs, files in os.walk(root):
                for file in files:
                    if not self.regex or cregex.match(file):
                        truncated_path = path.replace(shell.prefix,'')
                        filename = os.path.join(truncated_path,file)
                        key = filecount
                        odict[key] = filename
                        print key, odict[key]
                        filecount += 1
        shell.client.loaddict(odict, shell.username, shell.password,self.dataset)
        return len(odict)

    def CheckArgs(self,args):
        s = super(summary,self).CheckArgs(args)
        if s is not None:
            return s
        # find a regex or dataset in the args
        self.regex = None
        self.dataset = 0
        for a in args:
            if a.startswith('regex='):
                self.regex = a.split('=',1)[1]
                self.args.remove(a)
            elif a.startswith('dataset='):
                self.dataset = a.split('=',1)[1]
                self.args.remove(a)
        return None

class exit(Command):
    """Command: exit
     or  quit
    
   Exit iceprodsh.
   Also saves the default configuration settings 
   and history to the home directory.
    """
    shortdoc = "exit : Exit iceprodsh. AKA: quit"
    def Execute(self,shell):
        import os,readline,__builtin__
        cfgfile = __builtin__.open(os.path.join(os.getenv('HOME'),".iceprodshrc"),'w')
        shell.cfg.write(cfgfile)
        cfgfile.close()
        print "adios."
        try:
            readline.write_history_file(os.path.expandvars('$HOME/.iceprodsh_history'))
        except:pass
        os._exit(0)

class quit(exit):
    __doc__ = exit.__doc__
    shortdoc = "quit : Exit iceprodsh. AKA: exit"

class usage(Command):
    """Command: usage
     or  help
    
   Print usage information.
   This is the basic help system.
    """
    shortdoc = "usage : Print usage information. AKA: help"
    def Execute(self,shell):
        self._usage()
    
    usage = """Usage: iceprodsh [option] <url> <command> <args>
        
 options:
 
   -i,
   --interactive         : Interactive shell (default).
   
   -h,
   --help                : This screen.

   -r <url>, 
   --url=<url>           : Specify a url of soaptray server.

   -u <username>, 
   --username=<username> : Specify a username.

   --production          : Authenticate, update production database
                           and create metadata. For production by
                           authorized users.
   --test                : Specify submissions as tests.

   --meta=<file>         : Use .ini style meta-data file instead of
                           interactive form.
   -m <key:value>        : Key value pair to override metadata file.
                           Can use multiple times.
   
   --prefix=<value>      : Specify prefix for files.
    
   -v <value>,
   --validate=<value>    : Turn validation on or off.
    """
    def _usage(self):
        command_list = self._listcommands()
        print self.usage
        print " "
        print "commands:"
        for cmd in command_list:
            print "   "
            if cmd.shortdoc is None:
                print "  "+cmd.__doc__.split("\n",1)[0].split(':',1)[1]
            else:
                print "   "+cmd.shortdoc
        print "   "
    
    def _listcommands(self):
        import inspect
        command_list = []
        for name, obj in inspect.getmembers(inspect.getmodule(Command)):
            if inspect.isclass(obj) and name != "Command":
                command_list.append(obj)
        return command_list

class help(usage):
    """Command: help [<command>]
    
   Lists the help, either for general usage or for specific commands.
    
   Arguments:
     <command>  (Optional)  If omitted, prints usage.
                If specified, prints help for specific command.
    """
    shortdoc = "help [<command>] : Print usage or detailed help"
    def Execute(self,shell):
        if len(self.args) < 1:
            self._usage()
            return
        
        item = self.args[0]
        command_list = self._listcommands()
        modules = {'iceprod':'iceprod',
                    'iceprod.core':'iceprod.core',
                    'iceprod.server':'iceprod.server',
                    'iceprod.client':'iceprod.client',
                    'iceprod.modules':'iceprod.modules',
                    'core':'iceprod.core',
                    'server':'iceprod.server',
                    'client':'iceprod.client',
                    'modules':'iceprod.modules'}
        modules.update(self._listmodules('iceprod'))
        modules.update(self._listmodules('iceprod.core'))
        modules.update(self._listmodules('iceprod.server'))
        modules.update(self._listmodules('iceprod.client'))
        modules.update(self._listmodules('iceprod.modules'))
        for m in modules:
            if item == m:
                self._pythonhelp(modules[item])
                return
            elif item.startswith(m):
                self._pythonhelp(item)
                return
        for cmd in command_list:
            if cmd.__name__ == item:
                print cmd.__doc__
                return
        
        print "Item not found in help"
    
    def _listmodules(self,package_name=''):
        import os,imp
        package_name_os = package_name.replace('.','/')
        file, pathname, description = imp.find_module(package_name_os)
        if file:
            # Not a package
            return {}
        # Use a set because some may be both source and compiled.
        ret = {}
        for module in os.listdir(pathname):
            if module.endswith('.py') and module != '__init__.py':
                tmp = os.path.splitext(module)[0] 
                ret[tmp] = package_name+'.'+tmp
        return ret
    
    def _pythonhelp(self,item):
        import iceprod
        import iceprod.core
        import iceprod.server
        import iceprod.client
        import iceprod.modules
        import __builtin__
        __builtin__.help(item)

class submit(Command):
    """Command: submit [<filename>]
   
   Submit a new dataset.  If in production mode, submits to iceprod.
   Otherwise, the dataset is run locally.
    
   Arguments:
     <filename>  (Optional) Specify the filename to submit.
                 Defaults to the currently open file.
     
   Returns:
     Prints success or failure, as well as other details.
    """
    shortdoc = "submit [<filename>] : Submit a new dataset."
    def Execute(self,shell):         
        try:
            import signal
            from iceprod.core.xmlparser import IceTrayXMLParser
            from iceprod.client.soaptrayclient import i3SOAPClient
            from iceprod.core.dataclasses import Steering

            default_handler = { 
                signal.SIGQUIT: signal.getsignal(signal.SIGQUIT),
                signal.SIGINT:  signal.getsignal(signal.SIGINT) ,
                signal.SIGCHLD: signal.getsignal(signal.SIGCHLD),
            }


            def handler(signum,frame):
                shell.logger.warn("caught signal %s" % signum)
                raise Exception, "Operation cancelled"

            signal.signal(signal.SIGQUIT, handler)
            signal.signal(signal.SIGINT, handler)

            print "production flag is set to %s" % shell.production
            print "test flag is set to %s" % shell.test

            if len(self.args) >= 1:
                shell.steering = Steering()
                myparser = IceTrayXMLParser(shell.steering)
                if not myparser.ParseFile(self.args[0],validate=shell.xmlvalidate):
                    raise "unable to parse configuration file"

            if not shell.steering:
                shell.logger.fatal("no steering object loaded:Cannot submit") 
                return False

            # Prompt for description and add to steering object
            shell.descmap['geometry'] = '%(geometry)s'
            shell.descmap['simcat'] = '%(simcat)s'
            shell.descmap['composition'] = '%(composition)s'
            shell.descmap['weighted'] = '%(weighted)s'
            shell.descmap['spectrum'] = '%(spectrum)s'
            shell.descmap['icemodel'] = '%(icemodel)s'
            shell.descmap['angularrange'] = '%(angularrange)s'
            shell.descmap['energyrange'] = '%(energyrange)s'
            
            # do submit based on type
            if shell.meta:
                self._meta(shell)
                return
            elif shell.production:
                steering = self._production(shell)     
            else:
                steering = shell.steering

            shell.auth()
            # Instantiate SOAP client and submit job to cluster.
            i3q = shell.client.submit( steering, shell.username, shell.password, shell.production)
            if i3q:
                shell.client.check_q(i3q, shell.username, shell.password)

            # Reset signal handers back to original
            signal.signal(signal.SIGQUIT, default_handler[signal.SIGQUIT])
            signal.signal(signal.SIGINT, default_handler[signal.SIGINT])
        except Exception,e:
            print e
            
    def _meta(self,shell):
        import time
        from ConfigParser import ConfigParser,SafeConfigParser
        from iceprod.core import metadata, lex
        # read meta-data from file
        metafile = ConfigParser()
        steering = shell.steering
        print "reading parameters from", shell.meta
        try:
            metafile.read(shell.meta)
        except Exception,e:
            shell.logger.fatal("%s:Cannot read %s" % (e,shell.meta))
            raise
        for key,val in shell.metadict.items():
            try:
                metafile.set('iceprod-meta',key,val)
            except Exception,e:
                shell.logger.fatal(key + ":"+ e)

        grids = metafile.get('iceprod-meta','grid')
        steering.AddExtra("Grid", grids)

        maxjobs = metafile.getint('iceprod-meta','maxjobs')
        steering.AddExtra("Maxjobs", maxjobs)

        ticket = metafile.getint('iceprod-meta','ticket')
        steering.AddExtra("Ticket",ticket)

        simcat = metafile.get('iceprod-meta','sim-category')
        steering.SetCategory(simcat)

        dtype = metafile.get('iceprod-meta','dataset-type')
        steering.SetDatasetType(dtype)

        # Initialize metafile
        difplus  = metadata.DIF_Plus()
        dif      = difplus.GetDIF()
        plus     = difplus.GetPlus()

        dtext = metafile.get('iceprod-meta','title')
        dif.SetEntryTitle(dtext)

        cat = metafile.get('iceprod-meta','category')
        plus.SetCategory(cat)
        plus.SetSubCategory(self._get_subcat())
        datetime = time.strftime('%Y-%m-%dT%H:%M:%S',time.localtime())
        plus.SetStartDatetime(datetime)
        plus.SetEndDatetime(datetime)

        dtext = dif.GetValidParameters()[0]
        dif.SetParameters(dtext)

        steering.AddExtra("Metadata",difplus)

        dtext = metafile.get('iceprod-meta','description',raw=True)

        # iterate over steering parameters and store them parsed in dict
        shell.descmap['simcat']      = steering.GetCategory()
        shell.descmap['category']    = plus.GetCategory()
        shell.descmap['subcategory'] = plus.GetSubCategory()

        #instantiate a parser to evaluate expressions
        expparser = lex.ExpParser( {
                'extern':0,
                'procnum':0,
                'tray':0,
                'iter':0,
                'dataset':0,
                'nproc': int(steering.GetParameter('MAXJOBS').GetValue()),
                },
                steering)

        # iterate over steering parameters and store them parsed in dict
        for p in steering.GetParameters():
          try:
             shell.descmap[p.GetName()] = expparser.parse(p.GetValue())
          except Exception,e: pass

        try:
          dtext = dtext % shell.descmap
        except Exception,e:
          shell.logger.fatal(e)
        steering.SetDescription(dtext)
        dif.SetSummary(dtext)

        username = metafile.get('iceprod-meta','username')
        passwd   = metafile.get('iceprod-meta','passwd')
        i3q = shell.client.submit( steering, username, passwd, 1)
        if i3q:
           shell.client.check_q(i3q, username, passwd)

    def _production(self,shell):
        import time
        from iceprod.core import metadata, lex
        from iceprod.core.dataclasses import SimulationCategories, DatasetTypes
        # submit to iceprod
        steering = shell.steering
        
        # Prompt for grid
        msg = "Grid: enter the name(s) of grids to run this dataset on:"
        grids = self._get_text(msg)
        steering.AddExtra("Grid", grids)

        # Prompt for number of jobs
        msg = "Maxjobs: enter the number of jobs in this dataset:"
        maxjobs = self._get_int(msg)
        steering.AddExtra("Maxjobs", maxjobs)

        # Initialize metadata
        difplus  = metadata.DIF_Plus()
        dif      = difplus.GetDIF()
        plus     = difplus.GetPlus()

        # Prompt for title description and add to metadata object
        if shell.test:
            dtext = 'Test Dataset' 
        else:
            dtext = 'Production Dataset' 
            #dtext = self._get_title()
        dif.SetEntryTitle(dtext)

        # Prompt for ticket number
        ticket = 0
        if not shell.test:
            ticket = self._get_ticket()
        steering.AddExtra("Ticket",ticket)
    
        # Prompt for simulation category choice
        msg = "Simulation Category: Enter a generator from the choices below:"
        if shell.test:
            cat = 'Test'
        else:
            cat = self._get_choice(SimulationCategories, msg)
        steering.SetCategory(cat)

        # Prompt for category choice
        msg = "Category: enter a number from the choices below:"
        if shell.test:
            dtext = 'unclassified'
        else:
            dtext = self._get_choice(plus.GetValidCategories(),msg)
        plus.SetCategory(dtext)

        plus.SetSubCategory(self._get_subcat())

        #if self.test:
        #    startdatetime = time.strftime('%Y-%m-%dT%H:%M:%S',time.localtime())
        #else:
        #    startdatetime = get_date('Please enter a start validity datetime: ')
        startdatetime = time.strftime('%Y-%m-%dT%H:%M:%S',time.localtime())
        plus.SetStartDatetime(startdatetime)

        #if self.test:
        #    enddatetime = time.strftime('%Y-%m-%dT%H:%M:%S',time.localtime())
        #else:
        #    enddatetime = get_date('Please enter an end validity datetime: ')
        enddatetime = time.strftime('%Y-%m-%dT%H:%M:%S',time.localtime())
        plus.SetEndDatetime(enddatetime)

        for iconfig in steering.GetTrays():
            # Add projects from any included metaprojects to DIFPlus
            for metaproject in iconfig.GetMetaProjectList():
                for project in metaproject.GetProjectList():
                    plus.AddProject(project)

            # Add projects not included in any metaprojects to DIFPlus
            for project in iconfig.GetProjectList():
                plus.AddProject(project)

        msg = "Parameters: enter a number from the choices below:"
        #if self.test:
        #    dtext = dif.GetValidParameters()[0]
        #else:
        #    dtext = get_choice(dif.GetValidParameters(),msg)
        dtext = dif.GetValidParameters()[0]
        dif.SetParameters(dtext)

        # Prompt for dataset category
        msg = "Dataset type: enter a number from the choices below:"
        if shell.test:
            dtext = 'TEST'
        else:
            dtext = self._get_choice(DatasetTypes, msg)
        steering.SetDatasetType(dtext)

        #instantiate a parser to evaluate expressions
        expparser = lex.ExpParser( {
                'extern':0,
                'procnum':0,
                'tray':0,
                'iter':0,
                'dataset':0,
                'nproc': int(steering.GetParameter('MAXJOBS').GetValue()),
                },
                steering)

        # iterate over steering parameters and store them parsed in dict
        for p in steering.GetParameters():
          try:
             shell.descmap[p.GetName()] = expparser.parse(p.GetValue())
          except Exception,e: pass
        shell.descmap['simcat'] = steering.GetCategory()

        #initialize descripton
        descriptionstr = "%(geometry)s %(simcat)s %(composition)s "
        descriptionstr += "with %(weighted)s "
        descriptionstr += "spectrum of %(spectrum)s, using %(icemodel)s "
        descriptionstr += "photon tables. "
        descriptionstr += "Angular range of %(angularrange)s "
        descriptionstr += "and energy range of %(energyrange)s "
        description = descriptionstr % shell.descmap
        dtext = self._get_description(shell.completer,description)

        if shell.test:
            dtext = 'Test: ' + dtext
        steering.SetDescription(dtext)
        dif.SetSummary(dtext)

        steering.AddExtra("Metadata",difplus)
        return steering

    def _get_description(self,completer,initial=''):
        """
         Prompt for and read a description for the simulation run.
         @return: the text entered by the user as a single string 
        """
        import sys
        
        print 'Please type a brief description of this run.'
        print 'You may write multiple lines. To finish just enter a blank line:'

        text = []
        try:
            import readline
            import rlcompleter
        except:
            print 'e.g. "%s"' % initial
            line = sys.stdin.readline().strip()
            while line:
                text.append(line)
                line = sys.stdin.readline().strip()
        else:
            readline.parse_and_bind("tab: complete")
            readline.set_completer(completer)
            #readline.insert_text(initial)
            readline.add_history(initial)
            readline.set_startup_hook(None)
            while True:
                try:
                    line = raw_input('> ')
                except EOFError:
                    break
                if not line: break
                text.append(line)
        
        return ' '.join(text)

    def _get_text(self,prompt):
        """
         Prompt for input
         @return: the text entered by the user
        """
        import sys
        print prompt
        ret = sys.stdin.readline().strip()
        if not ret:
            return self._get_text(prompt)
        else:
            return ret

    def _get_int(self,prompt):
        return int(self._get_text(prompt))

    def _get_title(self):
        """
         Prompt for and read a title for the simulation run.
         @return: the text entered by the user
        """
        import sys
        print 'Please type title for this run:'
        ret = sys.stdin.readline().strip()
        if not ret:
            return self._get_title()
        else:
            return ret

    def _get_ticket(self):
        """
         Prompt for and read a ticket number associated with dataset.
         @return: the number entered
        """
        import sys
        print "Is there a ticket number associated with this dataset?"
        print "If so, enter it. Otherwise enter 0."
        val = sys.stdin.readline().strip()
        if val.isdigit():
            return int(val)
        else:
            print "  Not a number."
            print " "
            return self._get_ticket()

    def _get_subcat(self):
        """
         Prompt for and read a subcategrory for the DIF_Plus
         @return: the text entered by the user
        """
        print "Sub-category will be automatically filled by server"
        return "subcat"

    def _get_date(self,prompt):
        """
         Prompt for and read a title for the simulation run.
         @return: the text entered by the user
        """
        import sys, time, re
        date_regex = r'^[0-9]{4,4}(-[0-9]{2,2}){2,2}T([0-9]{2,2}:){2,2}[0-9]{2,2}'
        print 'Format: (yyyy-mm-ddThh:mm:ss)'
        print 'e.g. current local datetime:  %s ' % time.strftime('%Y-%m-%dT%H:%M:%S',time.localtime())
        print prompt
        ret = sys.stdin.readline().strip()
        if not ret or not re.search(date_regex,ret):
            return self._get_date(prompt)
        else:
            return ret

    def _get_choice(self,choices,prompt=''):
        import sys
        print prompt 
        for i in range(len(choices)):
            print "(%d): %s" % (i,choices[i])
        sys.stdout.write('choice: ')
        line = sys.stdin.readline().strip()
        try: 
            item = int(line)
            print 'You selected (%d): %s.' % (item,choices[item])
            sys.stdout.write('is this correct? (Y/N): ')
            val = sys.stdin.readline().strip().upper()
            if val == 'Y' or val == 'YES':
                return choices[item]
            else:
                return self._get_choice(choices,prompt)
        except Exception,e: 
            print 'Invalid choice: %s' % str(e)
            return self._get_choice(choices,prompt)

    def CheckArgs(self,args):
        # can have single string argument
        s = super(submit,self).CheckArgs(args)
        if s is not None:
            return s
        if len(args) > 1:
            print "Ignoring additional arguments.  Using only ",arg[0]
        return None
