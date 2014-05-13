#!/bin/env python
#

"""
 Main window for GtkIcetraConfig application

 copyright (c) 2005 the icecube collaboration

 @version: $Revision: $
 @date: 2006-02-07T13:17:44
 @author: Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
"""

import sys,os
import pygtk
pygtk.require('2.0')
import gtk
import gtk.gdk
import getpass
import getopt
import logging
import cPickle
import threading
import logo
from os.path import expandvars
from GtkProjects import GtkProjects
from GtkServices import GtkServices 
from GtkModules  import GtkModules
from GtkIceProdPre import GtkIceProdPre
from GtkIceProdPost import GtkIceProdPost
from GtkFileChooser import *
from GtkSteering import GtkSteering
from GtkOFilter import GtkOFilter
from GtkDAGRel import GtkDAGRel
from GtkExtern import GtkExtern
from GtkSteerBatchOpt import GtkSteerBatchOpt
from GtkSteerDepend import GtkSteerDepend
from GtkInput import GtkInput
from gtkrunlist import GtkRunList
from gtkmetaprojectlist import GtkMetaProjectList
from iceprod.core.xmlwriter import IceTrayXMLWriter
from iceprod.core.xmlparser import IceTrayXMLParser
from iceprod.core.dataclasses import *
from iceprod.core.paramdb import *
from iceprod.core.configuration import Config
from iceprod.client.soaptrayclient import i3SOAPClient

logger = logging.getLogger('gtkicetray')

#counter to keep track of howmany windows are open
instances = 0


class GtkConfig:

    _paramuser = None
    _parampasswd = None
    _produser = None
    _prodpasswd = None
    resume_action = None
    _windowminwidth = 750
    _windowminheight = 600
    _windowmaxwidth = 1150
    _windowmaxheight = 1000
    _trayminwidth = 400
    _trayminheight = 250
    _traymaxwidth = 800
    _traymaxheight = 650

    def RunTest(self):
		self.test = True

    def _getprodurl(self):

        if not self.url:
           if self.cfg.GetValue('URL'):
              self.url = self.cfg.GetValue('URL').split(',')
           else:
              self.url = ['https://condor.icecube.wisc.edu:9080']
        return self.url

    def _setprodurl(self,url):
        while len(self.url) > 10: self.url.pop()
        if url in self.url: 
            self.url.pop(self.url.index(url))
            self.url.insert(0,url)
            self.cfg.SetValue('URL',','.join(self.url))
        return url

    def _getproduser(self):
		if not self._produser:
			self._produser = self.cfg.GetValue('ConfigDatabaseUser')
		if not self._produser:
			self._produser = getpass.getuser()
		return self._produser

    def _setproduser(self,user):
		self._produser = user
		self.cfg.SetValue('ConfigDatabaseUser',user)
		return self._produser

    def _getprodpass(self,reset=False):
		if reset or not self._prodpasswd:
			self._prodpasswd = getpass.getpass("Password for '%s': " % self._getproduser() )
		return self._prodpasswd

    def _hasprodpass(self):
		return self._prodpasswd

    def _setprodpass(self,passwd):
		self._prodpasswd = passwd
		return self._prodpasswd

    def _getparamserver(self):
		if not self.pdb_server:
			self.pdb_server = self.cfg.GetValue('ParameterDatabaseServer')
		return self.pdb_server

    def _setparamserver(self,server):
        self.pdb_server = server
        self.cfg.SetValue('ParameterDatabaseServer',server)
        return self.pdb_server

    def _getprodserver(self):
		if not self.cdb_server:
			self.cdb_server = self.parent.cfg.GetValue('ConfigDatabaseServer')
		return self.cdb_server


    def _setprodserver(self,server):
        self.cdb_server = server
        self.cfg.SetValue('ConfigDatabaseServer',server)
        return self.cdb_server


    def _getparamdb(self):
        if not self.pdb_name:
			self.pdb_name = self.cfg.GetValue('ParameterDatabase')
        return self.pdb_name

    def _setparamdb(self,db):
        self.pdb_name=db
        self.cfg.SetValue('ParameterDatabase',db)
        return self.pdb_name

    def _getproddb(self):
        if not self.cdb_name:
			self.cdb_name = self.cfg.GetValue('ConfigDatabase')
        return self.cdb_name

    def _setproddb(self,db):
        self.cdb_name=db
        self.cfg.SetValue('ConfigDatabase',db)
        return self.cdb_name

			
    def _getparamuser(self):
		if not self._paramuser:
			self._paramuser = self.cfg.GetValue('ParameterDatabaseUser')
		if not self._paramuser:
			self._paramuser = getpass.getuser()
		return self._paramuser

    def _setparamuser(self,user):
		self._paramuser = user
		self.cfg.SetValue('ParameterDatabaseUser',user)
		return self._paramuser

    def _getparampass(self,reset=False):
		if reset or not self._parampasswd:
			self._parampasswd = getpass.getpass("Password for '%s': " % self._getparamuser() )
		return self._parampasswd

    def _setparampass(self,passwd):
		self._parampasswd = passwd
		return self._parampasswd

    def set_resume_action(self,action):
        self.resume_action = action

    def resume_action_callback(self):
        if self.resume_action:
        	self.resume_action()
        self.resume_action = None

    def rotate_book(self, button, notebook):
        """ 
		This method rotates the position of the tabs
        """

        notebook.set_tab_pos((notebook.get_tab_pos()+1) %4)
        return

    def tabsborder_book(self, button, notebook):
        """ 
		Add/Remove the page tabs and the borders
        """

        tval = False
        bval = False
        if self.show_tabs == False:
	    tval = True 
        if self.show_border == False:
	    bval = True

        notebook.set_show_tabs(tval)
        self.show_tabs = tval
        notebook.set_show_border(bval)
        self.show_border = bval
        return

    def remove_book(self, button, notebook):
        """ 
		Remove a page from the notebook
        """ 
        page = notebook.get_current_page()
        notebook.remove_page(page)
        # Need to refresh the widget -- 
        # This forces the widget to redraw itself.
        notebook.queue_draw_area(0,0,-1,-1)

    def delete(self, widget, event=None):
        self.SaveCookies()
        self.cfg.Write()
        self.config = None
        global instances
        instances -= 1
        if instances < 1: gtk.main_quit()
        return False

    def GetPathPrefix(self):
        """
		Get prefix path from entry
        """
        if self.steering.GetOF():
			self.steering.GetOF().SetPrefix(self.prefixentry.get_text())
			return self.steering.GetOF().GetPrefix()


    def saveas(self,widget):
        """
		menu item - Save configuration to file and prompt for filename
        """
        self.savefilechooser = GtkSaveFileChooser(self)


    def savefile(self,widget,configfile=None):
        """
		menu item - Save configuration to file
		@param widget: gtk widget
		@param configfile: current name of file
        """
        if not configfile: 
            configfile = self.configfile
        for tray in self.trays:
        	tray.GetEvents()
        	tray.GetIterations()
        self.GetPathPrefix()
        writer = IceTrayXMLWriter(self.steering,add_descriptions=self.showdefaults)
        writer.write_to_file(configfile)

    def addtray(self,widget):
        """
		menu item - Add new IceTrayConfig object
        """
        self.steering.AddTray(IceTrayConfig())
        self.ReloadWidgets()


    def deltray(self,widget):
        """
		menu item - delete IceTrayConfig object
        """
#        self.steering.AddTray(IceTrayConfig())
        page = self.notebook.get_current_page()
        print page
        if page > 0:
			self.steering.RemoveTray(page-1)
			del self.trays[page-1]
			self.notebook.remove_page(page)
			self.ReloadWidgets()
        else:
			self.PrintText("No IceTray selected.")

    def addof(self,widget):
        """
		menu item - Add new OfflineFilter object
        """
        if not self.steering.GetOF():
			self.steering.SetOF(OfflineFilter())
			self.ReloadWidgets()

    def addextern(self,widget):
        """
		menu item - Add new External object
        """
        if not self.steering.GetExterns():
			self.steering.AddExtern(Extern())
			self.ReloadWidgets()

    def delof(self,widget):
        """
		menu item - delete OfflineFilter object
        """
        if self.steering.GetOF():
			self.steering.SetOF(None)
			self.ReloadWidgets()
			page = self.steernote.get_current_page()
			page = self.steernote.get_nth_page(4)
			self.steernote.remove_page(4)
        else:
			self.PrintText("Nothing to remove.")

    def enable_dragndrop(self,widget):
        self.dragndrop = not self.dragndrop
        for tray in self.trays:
          for widget in tray.mywidgets:
              print widget, 'dragndrop'
              widget.SetDragNDrop(self.dragndrop)

    def enable_defaults(self,widget):
        self.showdefaults = not self.showdefaults


    def switchversion(self,widget):
       """
       menu item - globally switch metaprojects
       """
       try:
        	self.ReloadWidgets()
        	mlist = GtkMetaProjectList(self.pdb,self)
       except Exception,e:
        	logger.error(e)
        	pass

    def open(self,widget):
        """
		menu item - Display Filechooser dialog to load configuration file
        """
        self.openfilechooser = GtkOpenFileChooser(self,self.configfile)

    def close(self,widget):
        """
		menu item - remove config file 
        """
        self.CloseConfig()

    def new(self,widget):
        """
		menu item - start new config
        """
        if not self.steering:
           steering = Steering()
           steering.AddTray(IceTrayConfig())
           self.LoadConfig(steering)
        else:
           newconfig = GtkConfig(self._produser,self.prodflag,True)
           newconfig.LoadConfig(Steering())


    def PrintText(self,text,append=True):
        """
		Write text to console window and automatically scroll to end
		@param text: Text to append to message area
        """

        buffer = self.consoletv.get_buffer()
        if append:
            try:
         	    buffer.insert(buffer.get_end_iter(),text+'\n')
         	    self.consoletv.scroll_mark_onscreen(buffer.get_insert())
            except Exception,e :
			    logger.error(str(e))
        else:
            try:
         	    buffer.set_text(text+'\n')
         	    self.consoletv.scroll_mark_onscreen(buffer.get_insert())
            except Exception,e :
			    logger.error(str(e))

        return True


    def submit_form(self,widget):
        for tray in self.trays:
        	tray.GetEvents()
        	tray.GetIterations()
        self.GetPathPrefix()
        check,str = self.steering.CheckConfig()
        if not check:
        	self.PrintText('Configuration error: %s' % str)
        	return

        if self.prodflag: # Get DIF_Plus metadata info
        	gtkinputwindow = GtkInput(self,self.test)
        else:
			self.submit_auth(self.submit)
        return

    def check_server(self,widget):
        client = i3SOAPClient(geturl=self.geturl)
        client.SetPrinter(self.PrintText)
        try:
           self.PrintText(client.check_server(),append=False)
        except Exception,e:
           self.PrintText("failed to connect to server: %s" % e)


    def submit(self):
        client = i3SOAPClient(geturl=self.geturl)
        client.SetPrinter(self.PrintText)
        self.PrintText("submitting request...")

        passwd = self._getprodpass()

        i3q = client.submit(
				self.steering,
				self._getproduser(),
				passwd,
				self.prodflag)

        if i3q:
        	self.add_job_to_menu(i3q)
        del client


    def submit_auth(self,func):

		auth_func = lambda x: (
			(i3SOAPClient(geturl=self.geturl).authenticate(
				self._setproduser(username_entry.get_text()), 
				self._setprodpass(password_entry.get_text())) \
			and (func(), auth_dialog.destroy() )
			or error_label.set_text("Failed..")))

		auth_dialog = gtk.Dialog(title='authentication', parent=None, flags=0 );
		error_label = gtk.Label()
		error_label.show()

		username_label = gtk.Label("Username:")
		username_label.show()

		username_entry = gtk.Entry()
		username_entry.set_text(self._getproduser()) 
		username_entry.show()

		password_label = gtk.Label("Password:")
		password_label.show()

		password_entry = gtk.Entry()
		password_entry.set_visibility(False)
		if self._hasprodpass():
			password_entry.set_text(self._getprodpass()) 
		password_entry.show()
		password_entry.connect("activate", auth_func)

		cancel_button = gtk.Button('Cancel')
		cancel_button.show()
		cancel_button.connect("clicked", lambda widget: \
				self.PrintText("cancelled.") and auth_dialog.destroy())

		submit_button = gtk.Button('Submit')
		submit_button.show()
		submit_button.connect("clicked", auth_func)

		auth_dialog.vbox.pack_start(error_label, True, True, 0)
		auth_dialog.vbox.pack_start(username_label, True, True, 0)
		auth_dialog.vbox.pack_start(username_entry, True, True, 0)
		auth_dialog.vbox.pack_start(password_label, True, True, 0)
		auth_dialog.vbox.pack_start(password_entry, True, True, 0)
		auth_dialog.action_area.pack_start(cancel_button, True, True, 0)
		auth_dialog.action_area.pack_start(submit_button, True, True, 0)
		auth_dialog.show()


    def pdbauth(self,db,onfailure=None,args=()):

		auth_dialog = gtk.Dialog(title='authentication', parent=None, flags=0 );

		username_label = gtk.Label("Username:")
		username_label.show()

		username_entry = gtk.Entry()
		username_entry.set_text(self._getparamuser()) 
		username_entry.show()

		password_label = gtk.Label("Password:")
		password_label.show()

		password_entry = gtk.Entry()
		password_entry.set_visibility(False)
		password_entry.show()

		server_label = gtk.Label("Server:")
		server_entry = gtk.Entry()
		if self._getparamserver(): 
			server_entry.set_text(self._getparamserver()) 
		server_label.show()
		server_entry.show()

		database_label = gtk.Label("Database:")
		database_entry = gtk.Entry()
		if self._getparamdb():
			database_entry.set_text(self._getparamdb()) 
		database_label.show()
		database_entry.show()

		cancel_button = gtk.Button('Cancel')
		cancel_button.show()
		cancel_button.connect("clicked", lambda widget: auth_dialog.destroy())

		submit_button = gtk.Button('OK')
		submit_button.show()

		authfunc = lambda x: (db.authenticate(
						self._setparamserver(server_entry.get_text()),
						self._setparamuser(username_entry.get_text()), 
						self._setparampass(password_entry.get_text()),
						self._setparamdb(database_entry.get_text()),
						True) ,  
						auth_dialog.destroy(), 
						self.resume_action_callback(),
						onfailure and apply(onfailure,args))

		username_entry.connect("activate", authfunc)
		password_entry.connect("activate", authfunc)
		server_entry.connect("activate", authfunc)
		database_entry.connect("activate", authfunc)
		submit_button.connect("clicked", authfunc)


		auth_dialog.vbox.pack_start(username_label, True, True, 0)
		auth_dialog.vbox.pack_start(username_entry, True, True, 0)
		auth_dialog.vbox.pack_start(password_label, True, True, 0)
		auth_dialog.vbox.pack_start(password_entry, True, True, 0)
		auth_dialog.vbox.pack_start(server_label, True, True, 0)
		auth_dialog.vbox.pack_start(server_entry, True, True, 0)
		auth_dialog.vbox.pack_start(database_label, True, True, 0)
		auth_dialog.vbox.pack_start(database_entry, True, True, 0)
		auth_dialog.action_area.pack_start(cancel_button, True, True, 0)
		auth_dialog.action_area.pack_start(submit_button, True, True, 0)
		auth_dialog.show()

    def add_job_to_menu(self,job):
        jobs_item = gtk.MenuItem("Run " + str(job.GetClusterId()))
        new_job_menu = gtk.Menu()
        jobs_item.set_submenu(new_job_menu)

        check_job_item = gtk.MenuItem("Check status")
        check_job_item.connect("activate",self.check_job,job)
        new_job_menu.append(check_job_item)
        check_job_item.show()

        terminate_job_item = gtk.MenuItem("Terminate")
        terminate_job_item.connect("activate",self.remove_job,job)
        new_job_menu.append(terminate_job_item)
        terminate_job_item.show()

        clear_job_item = gtk.MenuItem("Clear job")
        clear_job_item.connect("activate",self.clear_job,job)
        new_job_menu.append(clear_job_item)
        clear_job_item.show()

        jobs_item.show()
        self.jobs_menu.prepend(jobs_item)
        self.job_q_list.append((job,jobs_item))


    def run(self,widget,data=None):
        """
		Run job on local computer (where client is running). This
		requires that your I3_WORK environment is set.
        """
        self.PrintText('Running IceTray on local computer in %s' % os.getenv('I3_WORK') )
        self.PrintText('Output is printed to stdout (for now)')
        self.PrintText('-'*40)
        threading.Thread(target=self.run_thread).start()

    def run_thread(self):
        IceTrayXMLWriter(self.GetSteering()).write_to_file('.tmp.xml')
        path = expandvars('$I3_BUILD/bin/runconfig.py')
        cmd = '%s .tmp.xml 2>&1' % path
        print cmd 
        cout = os.popen(cmd,'r')
        eline = cout.readline()
        line = 0
        buff = ""
        while eline:
           if line%30 == 0:  
              self.PrintText(buff)
              buff = ""
           buff += "\n" + eline.strip()
           eline = cout.readline()
           line += 1
        cout.close()
        return


    def clear_jobs(self,widget):
        for job in self.job_q_list:
        	self.jobs_menu.remove(job[1])
        	del job
        self.job_q_list = []

    def check_job(self,widget,job):
        """
		Request status of job from server 
		@todo: deal with proxy option
		Note: This might seem like a security risk. One might hack the local
		version of an I3Queue object to run a different command on the
		server. However, the the server uses the class definitions from it's
		own libraries and does not care about the client implementation of
		this class.
        """
        logger.debug("checking status of job from on host %s" % \
					   	job.url or job.GetHost() )
        
        client = i3SOAPClient(geturl=self.geturl)
        client.SetPrinter(self.PrintText)
        check_q = lambda:client.check_q(job,self._getproduser(),self._getprodpass())
        self.submit_auth(check_q)

    def remove_job(self,widget,job):
        """
		Request removal of job from queue
		@todo: deal with proxy option
        """
        logger.info("requesting removal of job from queue on host %s" % \
					   	job.url or job.GetHost() )
        
        client = i3SOAPClient(geturl=self.geturl)
        client.SetPrinter(self.PrintText)
        q_remove = lambda:client.q_remove(job,self._getproduser(),self._getprodpass())
        self.submit_auth(q_remove)

    def clear_job(self,widget,job):

        for j in range(len(self.job_q_list)):
			if self.job_q_list[j][0].GetClusterId() == job.GetClusterId():
				self.jobs_menu.remove(self.job_q_list[j][1])
				del self.job_q_list[j]
				return
        		
        
        

    def getconfig(self,filename):
       try:
        	steering = Steering()
        	self.configfile = filename
        	self.parser = IceTrayXMLParser(steering)
        	self.parser.ParseFile(filename,validate=self.validatexml)
        	self.LoadConfig(steering)
        	self.ReloadWidgets()
       except Exception,e:
        	sys.excepthook(sys.exc_type,sys.exc_value,sys.exc_traceback) 
        	logger.error(e)

    def loadpdbfile(self,widget):
        try:
        	opendbfilechooser = GtkOpenDBFileChooser(self)
        except Exception,e: logger.error(e)

    def updatedb(self,widget):
       try:
       		self.pdb.download()
        	savedbfilechooser = GtkSaveDBFileChooser(self)
       except Exception,e:
        	logger.debug(e)
        	pass

    def docupage(self,widget):
       try:
        	import webbrowser
        	webbrowser.open("http://wiki.icecube.wisc.edu/index.php/IceProd")
       except Exception,e:
        	logger.debug(e)
        	pass

    def download(self,widget):
       try:
        	client = i3SOAPClient(geturl=self.geturl)
        	client.SetPrinter(self.PrintText)
        	self.runlist = GtkRunList(client,self)
       except Exception,e:
        	logger.error(e)
        	sys.excepthook(sys.exc_type,sys.exc_value,sys.exc_traceback) 
        	pass

    def SetConfigFile(self,cfile):
        self.configfile = cfile

    def GetConfigFile(self):
        return self.configfile

    def SetSteering(self,steering):
        self.steering = steering


    def GetIceTrayConfig(self,index=0):
        return self.steering.GetTrays()[index]

    def GetSteering(self):
        return self.steering

    def reload_widget(self,tab,event,widget):
        widget.reload(self.steering)

    def ReloadWidgets(self):
		for w in self.mywidgets:
			w.reload(self.steering)

		for t in range(max(0,len(self.steering.GetTrays())), 
						max(0,len(self.trays))):
			self.notebook.remove_page(t+1)

		for t in range(len(self.steering.GetTrays())):
			if t < len(self.trays):
				self.trays[t].reload(self.steering.GetTray(t))
				if len(self.trays) > 1:
					self.notebook.set_tab_label_text(self.trays[t], 
							"IceTray[%d]" % t )
				else:
					self.notebook.set_tab_label_text(self.trays[t], "IceTray")
			else:
				self.AddTrayTab(self.steering.GetTray(t))

		if self.steering.GetOF():
			self.AddFilterTab(self.steering)



    def LoadCookies(self,cfg=os.path.join(os.getenv('HOME'),".gtkicetray_cookies")):
		try:
			cfile = open(cfg); 
			joblist = cPickle.load(cfile)

			for job in joblist:
				self.add_job_to_menu(job)
		except Exception, e:
			self.PrintText(str(e))
			pass # don't care if there aren't any cookies

    def SaveCookies(self,cfg=os.path.join(os.getenv('HOME'),".gtkicetray_cookies")):
		try:
			jobs = [job for job,job_entry in self.job_q_list]
			cfile = open(cfg,'w'); 
			cPickle.dump(jobs,cfile)
		except Exception,e:
			logger.warn("could not save cookies: %s" % e)

    def SaveXMLpdb(self):
		try:
			self.pdb.savefile(expandvars("$HOME/.gtktray-paramdb"))
		except Exception,e:
			logger.warn("could not save parameter db: %s" % e)

    def MakeToolbar(self):
        toolbar = gtk.Toolbar()
        toolbar.set_orientation(gtk.ORIENTATION_HORIZONTAL)
        toolbar.set_style(gtk.TOOLBAR_ICONS)

        run_icon = gtk.Image()
        run_icon.set_from_stock(gtk.STOCK_EXECUTE,gtk.ICON_SIZE_SMALL_TOOLBAR)
        toolbar.append_element(
            gtk.TOOLBAR_CHILD_RADIOBUTTON, # type of element
            None,                          # widget
            "Run","Run","",run_icon,
            self.run,                      # signal
            toolbar)                       # data for signal

        stop_icon = gtk.Image()
        stop_icon.set_from_stock(gtk.STOCK_STOP,gtk.ICON_SIZE_SMALL_TOOLBAR)
        toolbar.append_element(
            gtk.TOOLBAR_CHILD_RADIOBUTTON, # type of element
            None,                          # widget
            "Stop","Stop","",stop_icon,
            None,                          # signal
            toolbar)                       # data for signal

        toolbar.show()
        handlebox = gtk.HandleBox()
        handlebox.add(toolbar)

        return handlebox

    def MakeMenu(self):
        """
        Create menubar and add menu items
        """

        menu_bar = gtk.MenuBar()

        file_menu = gtk.Menu()
        file_menu.show()

        file_menu_item = gtk.MenuItem("_File")
        file_menu_item.show()
        file_menu_item.set_submenu(file_menu)
        menu_bar.append(file_menu_item)

        edit_menu = gtk.Menu()
        edit_menu.show()
        self.edit_menu = edit_menu

        edit_menu_item = gtk.MenuItem("_Edit")
        edit_menu_item.show()
        edit_menu_item.set_submenu(edit_menu)
        menu_bar.append(edit_menu_item)

        addtray_item = gtk.MenuItem("add tray")
        addtray_item.connect("activate",self.addtray)
        addtray_item.show()
        edit_menu.append(addtray_item)

        deltray_item = gtk.MenuItem("remove tray")
        deltray_item.connect("activate",self.deltray)
        deltray_item.show()
        edit_menu.append(deltray_item)

        addof_item = gtk.MenuItem("add filter description")
        addof_item.connect("activate",self.addof)
        addof_item.show()
        edit_menu.append(addof_item)

        delof_item = gtk.MenuItem("remove filter description")
        delof_item.connect("activate",self.delof)
        delof_item.show()
        edit_menu.append(delof_item)

        jobs_menu = gtk.Menu()
        jobs_menu.show()
        self.jobs_menu = jobs_menu

        jobs_menu_item = gtk.MenuItem("_Jobs")
        jobs_menu_item.show()
        jobs_menu_item.set_submenu(jobs_menu)
        menu_bar.append(jobs_menu_item)
        self.LoadCookies()

        tools_menu = gtk.Menu()
        tools_menu.show()
        self.tools_menu = tools_menu

        tools_menu_item = gtk.MenuItem("_Tools")
        tools_menu_item.show()
        tools_menu_item.set_submenu(tools_menu)
        menu_bar.append(tools_menu_item)
		
        help_menu = gtk.Menu()
        help_menu.show()
        self.help_menu = help_menu

        help_menu_item = gtk.MenuItem("_Help")
        help_menu_item.show()
        help_menu_item.set_submenu(help_menu)
        menu_bar.append(help_menu_item)

        documentation_item = gtk.MenuItem("IceProd _Documentation")
        documentation_item.connect("activate",self.docupage)
        documentation_item.show()
        help_menu.append(documentation_item)

        open_item = gtk.MenuItem("_Open...")
        open_item.connect("activate",self.open)
        open_item.show()
        file_menu.append(open_item)

        new_item = gtk.MenuItem("_New")
        new_item.connect("activate",self.new)
        new_item.show()
        file_menu.append(new_item)

        close_item = gtk.MenuItem("_Close")
        close_item.connect("activate",self.close)
        close_item.show()
        file_menu.append(close_item)

        download_item = gtk.MenuItem("_Download...")
        download_item.connect("activate",self.download)
        download_item.show()
        file_menu.append(download_item)

        save_item = gtk.MenuItem("_Save")
        save_item.connect("activate",self.savefile)
        save_item.show()
        file_menu.append(save_item)

        saveas_item = gtk.MenuItem("Save _As...")
        saveas_item.connect("activate",self.saveas)
        saveas_item.show()
        file_menu.append(saveas_item)

        submit_item = gtk.MenuItem("Submit _Dataset")
        submit_item.connect("activate",self.submit_form)
        submit_item.show()
        file_menu.append(submit_item)

        run_item = gtk.MenuItem("_Run (local)")
        run_item.connect("activate",self.run)
        run_item.show()
        file_menu.append(run_item)

        quit_item = gtk.MenuItem("_Quit")
        quit_item.connect("activate",self.delete)
        quit_item.show()
        file_menu.append(quit_item)

        clear_jobs_item = gtk.MenuItem("_Clear jobs")
        clear_jobs_item.connect("activate",self.clear_jobs)
        clear_jobs_item.show()
        jobs_menu.append(clear_jobs_item)

        updatedb_item = gtk.MenuItem("_Download Parameter DB...")
        updatedb_item.connect("activate",self.updatedb)
        updatedb_item.show()
        tools_menu.append(updatedb_item)

        loadpdb_item = gtk.MenuItem("_Load Parameter DB File...")
        loadpdb_item.connect("activate",self.loadpdbfile)
        loadpdb_item.show()
        tools_menu.append(loadpdb_item)

        upgrade_item = gtk.MenuItem("_Upgrade Metaprojects...")
        upgrade_item.connect("activate",self.switchversion)
        upgrade_item.show()
        tools_menu.append(upgrade_item)

        drag_item = gtk.CheckMenuItem("_Enable drag & drop")
        drag_item.connect("activate",self.enable_dragndrop)
        drag_item.show()
        tools_menu.append(drag_item)

        desc_item = gtk.CheckMenuItem("_Enable Defaults")
        desc_item.connect("activate",self.enable_defaults)
        desc_item.show()
        tools_menu.append(desc_item)

        return menu_bar

    def get_i3config(self):
        return self.steering

    def geturl(self):
        return self._setprodurl(self.url_entry.get_child().get_text())

    def __init__(self, user=None,prod=False,auth=True):
        self.steering    = None
        self.dragndrop   = False
        self.showdefaults  = False
        self.configfile  = "newconfig.xml"
        self.host        = None
        self.url         = []
        self.job_q_list  = []
        self._user       = user
        self.prodflag    = prod
        self.test        = False
        self.trays       = []
        self.child_pid   = 0
        self.validatexml = True
        gtk.gdk.threads_init()

        self.cfg = Config(os.path.join(os.getenv('HOME'),".gtkicetrayrc"))
        try:
            self.cfg.Read()
        except Exception,e:
            print >> sys.stderr, "Could not find configuration"

        if self.cfg.GetValue('URL'):
           self.url = self.cfg.GetValue('URL').split(',')
        else:
           self.url = []
        self.pdb = RPCParamDB(geturl=self.geturl)
        self.pdb.SetPrinter(self.PrintText)
        self.mywidgets = []

        self.consoletv = gtk.TextView()
        self.consoletv.set_cursor_visible(False)	
        self.consoletv.set_wrap_mode(gtk.WRAP_NONE)
        self.consoletv.set_editable(False)

        table = gtk.Table(3,6,False)
        consolesw = gtk.ScrolledWindow()
        self.vbox = gtk.VBox(False,0)
        self.vpane = gtk.VPaned()
        menu_bar = self.MakeMenu()
        menu_bar.show()
        self.vbox.pack_start(menu_bar,False,False,2)

        hbox = gtk.HBox(False,1)
        self.url_entry = gtk.combo_box_entry_new_text() 
        urlframe = gtk.Frame("URL")
        for url in self._getprodurl():
           self.url_entry.append_text(url)
        self.url_entry.set_active(0)

        urlframe.add(self.url_entry)
        urlframe.show()

        run_icon = gtk.Image()
        pixbuf = gtk.gdk.pixbuf_new_from_xpm_data(logo.xpm_image_data)
        run_icon.set_from_stock(gtk.STOCK_EXECUTE,gtk.ICON_SIZE_SMALL_TOOLBAR)
        #run_icon.set_from_pixbuf(pixbuf.scale_simple(30,30,gtk.gdk.INTERP_TILES))
        run_icon.show()
        run_button = gtk.Button('',stock=gtk.STOCK_NETWORK)
        #run_button.set_image(run_icon)
        run_button.show()
        run_button.connect("clicked", self.check_server)
        hbox.pack_start(urlframe,True,True,2)
        hbox.pack_start(run_button,False,False,2)
        hbox.show()

        self.url_entry.show()
        self.vbox.pack_start(hbox,False,False,2)

        self.vbox.pack_end(self.vpane,True,True,2)
        self.vpane.pack1(table,True,True)
        self.vpane.pack2(consolesw,True,True)
        self.vbox.show()
        self.vpane.show()

        #consolesw.set_size_request(100, 140)
        consolesw.add(self.consoletv)
        consolesw.show()
        self.consoletv.show()

        self.window = gtk.Window(gtk.WINDOW_TOPLEVEL)
        global instances
        instances += 1
        window = self.window

        # get screen size
        screen = window.get_screen()
        screenwidth = screen.get_width()
        screenheight = screen.get_height()
        if (screenwidth > self._windowmaxwidth+200):
            self.windowwidth = self._windowmaxwidth
        elif (screenwidth < self._windowminwidth+200):
            self.windowwidth = self._windowminwidth
        else:
            self.windowwidth = screenwidth-200
        if (screenheight > self._windowmaxheight+200):
            self.windowheight = self._windowmaxheight
        elif (screenheight < self._windowminheight+200):
            self.windowheight = self._windowminheight
        else:
            self.windowheight = screenheight-200
        self.traywidth = self.windowwidth-350
        self.trayheight = self.windowheight-350

        # make window
        window.connect("delete_event", self.delete)
        window.set_border_width(10)
        window.set_default_size(self.windowwidth, self.windowheight)
        window.add(self.vbox)
        window.set_resizable(True)
        window.set_position(gtk.WIN_POS_CENTER_ALWAYS)
        window.show()
        icon = window.render_icon(gtk.STOCK_DIALOG_INFO, gtk.ICON_SIZE_BUTTON)
        pixbuf = gtk.gdk.pixbuf_new_from_xpm_data(logo.xpm_image_data)
        window.set_icon(pixbuf)
        window.show()

        # Create a new notebook, place the position of the tabs
        self.notebook = gtk.Notebook()
        self.notebook.set_tab_pos(gtk.POS_TOP)
        table.attach(self.notebook, 0,6,0,1)
        self.notebook.show()
        self.show_tabs = True
        self.show_border = True
        table.show()

    def CloseConfig(self):
        self.steering = None
        for w in self.mywidgets: del w
        self.mywidgets = []
        self.trays     = []
        while  self.notebook.get_current_page() >= 0:
           self.notebook.remove_page(self.notebook.get_current_page())

    def LoadConfig(self,steering):
        # Steering
        self.CloseConfig()
        self.steering = steering
        label = gtk.Label("Steering")
        self.steernote = gtk.Notebook()
        self.steernote.show()
        self.notebook.append_page(self.steernote, label)

        frame = gtk.Frame("Steering")
        frame.set_border_width(10)
        #frame.set_size_request(self.traywidth, self.trayheight)
        frame.show()

        label = gtk.Label("Parameters")
        label.show()
        gtksteering = GtkSteering(self.steering)
        frame.add(gtksteering)
        self.mywidgets.append(gtksteering)
        self.steernote.append_page(frame, label)

        # Steering - batch options
        frame = gtk.Frame("Archive Dependencies")
        frame.set_border_width(10)
        #frame.set_size_request(self.traywidth, self.trayheight)
        frame.show()

        label = gtk.Label("Dependencies")
        label.show()
        gtksteerdepend = GtkSteerDepend(self.steering)
        frame.add(gtksteerdepend)
        self.mywidgets.append(gtksteerdepend)
        self.steernote.append_page(frame, label)

        # Steering - batch options
        frame = gtk.Frame("Batch System Options")
        frame.set_border_width(10)
        #frame.set_size_request(self.traywidth, self.trayheight)
        frame.show()

        label = gtk.Label("BatchOpts")
        label.show()
        gtkbatchopts = GtkSteerBatchOpt(self.steering)
        frame.add(gtkbatchopts)
        self.mywidgets.append(gtkbatchopts)
        self.steernote.append_page(frame, label)

        # Steering - Externs
        frame = gtk.Frame("Externals")
        frame.set_border_width(10)
        #frame.set_size_request(self.traywidth, self.trayheight)
        frame.show()
        label = gtk.Label("Externals")
        label.show()
        gtkextern = GtkExtern(self.steering)
        frame.add(gtkextern)
        self.mywidgets.append(gtkextern)
        self.steernote.append_page(frame, label)
        
        self.AddTaskTab(steering)
        # Steering - DAG
        #frame = gtk.Frame("DAG Graph")
        #frame.set_border_width(10)
        ##frame.set_size_request(self.traywidth, self.trayheight)
        #frame.show()
        #label = gtk.Label("DAG Graph")
        #label.show()
        #dag = GtkDAGRel(self.steering)
        #frame.add(dag)
        #self.mywidgets.append(dag)
        #self.steernote.append_page(frame, label)
        
        if self.steering.GetOF():
			self.AddFilterTab(self.steering)

        for tray in self.steering.GetTrays():
        	self.AddTrayTab(tray)

        #if steering.GetTaskDefinitions():
		#	self.AddTaskTab(self.steering)

        # Set what page to start at (page 0)
        self.notebook.set_current_page(0)

        # Create a bunch of buttons
        button = gtk.Button("close")
        button.connect("clicked", self.delete)
        button.show()


    def AddTrayTab(self,tray):
		"""
		Add a new icetray configuration tab
		@param tray: the new IceTrayConfig object
		"""
		# Tray
		gtktray = GtkIceTray(tray,self.pdb,self.traywidth,
                                     self.trayheight,self.windowwidth-100,
                                     self.windowheight-100)
		gtktray.SetPrinter(self.PrintText)
		gtktray.show()
		label = gtk.Label()
		self.notebook.append_page(gtktray, label)
		label.set_text("IceTray[%d]" % len(self.trays) )
		label.show()
		self.trays.append(gtktray)

    def AddFilterTab(self,steering):
        hbox = gtk.HBox()
        fbox = gtk.VBox()
        self.prefixentry = EventEntry("Prefix")
        label = gtk.Label("Prefix")
        self.prefixentry.set_reload_function(steering.GetOF().GetPrefix)
        self.prefixentry.set_text(str(steering.GetOF().GetPrefix()))
        self.prefixentry.show()
        hbox.pack_start(self.prefixentry,False,False,2)
        fbox.pack_start(hbox,False,False,2)
        hbox.show()

        frame = gtk.Frame("OfflineFilter")
        frame.set_border_width(10)
        #frame.set_size_request(self.traywidth, self.trayheight)
        frame.show()
        fbox.pack_start(frame)
        fbox.show()

        if not steering.GetOF(): steering.SetOF(OfflineFilter())
        label = gtk.Label("OfflineFilter")
        label.show()
        gtkof = GtkOFilter(steering)
        frame.add(gtkof)
        self.mywidgets.append(gtkof)
        self.steernote.append_page(fbox, label)

    def AddTaskTab(self,steering):
        fbox = gtk.VBox()
        frame = gtk.Frame("TaskRel")
        frame.set_border_width(10)
        #frame.set_size_request(self.traywidth, self.trayheight)
        frame.show()
        fbox.pack_start(frame)
        fbox.show()

        label = gtk.Label("TaskRel")
        label.show()
        gtk_tasks = GtkDAGRel(steering)
        frame.add(gtk_tasks)
        self.mywidgets.append(gtk_tasks)
        self.steernote.append_page(fbox, label)



class GtkIceTray(gtk.VBox):

    _traywidth = 400
    _trayheight = 250

    def reload(self,tray):
		self.tray = tray
		self.evententry.set_text(self.tray.GetEvents())
		self.iterentry.set_text(self.tray.GetIterations())

		for widget in self.mywidgets: 
			widget.reload(tray)

    def reload_widget(self,tab,event,widget):
        widget.reload(self.tray)

    def SetPrinter(self,printer):
		for widget in self.mywidgets:
			widget.SetPrinter(printer)

    def GetEvents(self):
		self.tray.SetEvents(int(self.evententry.get_text()))
		return self.tray.GetEvents()

    def GetIterations(self):
		self.tray.SetIterations(int(self.iterentry.get_text()))
		return self.tray.GetIterations()

    def __init__(self,tray,database,traywidth=_traywidth,
                 trayheight=_trayheight,windowwidth=750,windowheight=600):
		gtk.VBox.__init__(self)
		self.tray = tray
		self.pdb = database
		self.mywidgets  = []
		hbox = gtk.HBox()
		self.evententry=EventEntry()
		self.evententry.set_text(str(self.tray.GetEvents()))
		self.evententry.show()
		self.iterentry=EventEntry("Iterations")
		self.iterentry.set_reload_function(IceTrayConfig.GetIterations)
		self.iterentry.set_text(str(self.tray.GetIterations()))
		self.iterentry.show()
		hbox.pack_start(self.evententry,False,False,2)
		hbox.pack_start(self.iterentry,False,False,2)
		hbox.show()

		self.pack_start(hbox,False,False,2)

		self.notebook = gtk.Notebook()

		# IceProdPre
		frame = gtk.Frame("IceProdPre")
		frame.set_border_width(10)
		#frame.set_size_request(traywidth, trayheight)
		frame.show()

		label = gtk.Label("IceProdPre")
		label.show()
		gtkpres = GtkIceProdPre(self.tray,self.pdb,
                                        windowwidth,windowheight)
		frame.add(gtkpres)
		self.mywidgets.append(gtkpres)
		self.notebook.append_page(frame, label)

		# Modules
		frame = gtk.Frame("Modules")
		frame.set_border_width(10)
		#frame.set_size_request(traywidth, trayheight)
		frame.show()

		label = gtk.Label("Modules")
		label.show()
		gtkmodules = GtkModules(self.tray,self.pdb)
		frame.add(gtkmodules)
		self.mywidgets.append(gtkmodules)
		self.notebook.append_page(frame, label)
	  
		# Services
		frame = gtk.Frame("Services")
		frame.set_border_width(10)
		#frame.set_size_request(traywidth, trayheight)
		frame.show()

		label = gtk.Label("Services")
		label.show()
		gtkservices = GtkServices(self.tray,self.pdb)
		frame.add(gtkservices)
		self.mywidgets.append(gtkservices)
		self.notebook.append_page(frame, label)

		# IceProdPost
		frame = gtk.Frame("IceProdPost")
		frame.set_border_width(10)
		#frame.set_size_request(traywidth, trayheight)
		frame.show()

		label = gtk.Label("IceProdPost")
		label.show()
		gtkposts = GtkIceProdPost(self.tray,self.pdb)
		frame.add(gtkposts)
		self.mywidgets.append(gtkposts)
		self.notebook.append_page(frame, label)


		# Projects
		vbox = gtk.VBox()
		frame = gtk.Frame()
		frame.set_border_width(10)
		#frame.set_size_request(traywidth+100, trayheight)
		frame.show()

		gtkprojects = GtkProjects(tray,self.pdb)
		self.mywidgets.append(gtkprojects)

		tab = gtkTab("Projects")
		tab.connect('button_press_event',self.reload_widget,gtkprojects)
		tab.show()
		self.notebook.append_page(gtkprojects, tab)
		self.pack_start(self.notebook,True,True,2)
		self.notebook.show()

		self.notebook.set_current_page(1)


  
		# Connections
#        frame = gtk.Frame("Connections")
#        frame.set_border_width(10)
#        frame.set_default_size(traywidth, trayheight)
#        frame.show()

#        label = gtk.Label("Connections")
#        label.show()
#        gtkconnections = GtkConnections(self.tray)
#        frame.add(gtkconnections)
#        self.mywidgets.append(gtkconnections)
#        self.notebook.append_page(frame, label)

																									
class gtkTab(gtk.EventBox):

    def __init__(self,label_text):
    	gtk.EventBox.__init__(self)
    	self.set_events(gtk.gdk.BUTTON_PRESS_MASK)
    	self.label = gtk.Label(label_text)
    	self.add(self.label)

    def show(self):
    	gtk.EventBox.show(self)
    	self.label.show()


class EventEntry(gtk.Frame):

    def __init__(self,title="Events"):
        gtk.Frame.__init__(self,title)
        self.evententry = gtk.Entry()
        self.add(self.evententry)
        self.reload_func = IceTrayConfig.GetEvents

    def set_reload_function(self,func):
        self.reload_func = func

    def reload(self,tray):
        self.evententry.set_text(str(self.reload_func(tray)))

    def show(self):
        gtk.Frame.show(self)
        self.evententry.show()

    def get_text(self):
        return self.evententry.get_text()

    def set_text(self,txt):
        self.evententry.set_text(str(txt))

def main():
    GtkConfig()
    return 0

if __name__ == "__main__":
    main()
