#!/bin/env python
#   copyright  (c) 2005
#   the icecube collaboration
#   $Id: $
#
#   @version $Revision: $
#   @date $Date: $
#   @author Juan Carlos Diaz Velez <juancarlos@icecube.wisc.edu>
#	@brief Setup script for installation of iceprod modules
#########################################################################
import sys,os
import os.path
import getpass
import getopt
import cPickle
from optparse import OptionParser
from os.path import abspath,join

_xml     = True
_mysql   = True
_ssl     = True
_soapy   = True

def write(str,fill=''):
	os.write(sys.stdout.fileno(),str + fill*(50-len(str)))

def hello(): 
	return "world"


def checklibs():
   """
   Check for presence and functionality of python module dependencies

   """
   _xml     = False
   _mysql   = False
   _ssl     = False
   _soapy   = False

   write("Checking for python libraries\n")

   write("checking for XML support",'.')
   try:
        from xml import xpath
        from xml.dom.ext.reader import Sax2
        from xml.dom.NodeFilter import NodeFilter
        from xml.dom.ext import Print,PrettyPrint
        from xml.sax._exceptions import SAXParseException
        write("OK\n")
        _xml = True
   except Exception, e:
        write("Missing\n")
        print >> sys.stderr , e

   # MySQL
   write("checking for MySQLdb",'.')
   try:
        import MySQLdb
        write("OK\n")
        _mysql=True
   except Exception, e:
        write("Missing\n")
        print >> sys.stderr , e

   # SSL
   write("checking for OpenSSL",'.')
   try:
        from OpenSSL import SSL
        write("OK\n")
        _ssl = True
   except Exception, e:
        write("Missing\n")
        print >> sys.stderr , e


   if _mysql:
        write("checking functionality of MySQLdb module",'.')
        try:
            _conn = MySQLdb.connect(host="dbs2.icecube.wisc.edu",user="www",db="I3OmDb") 
            cursor = _conn.cursor (MySQLdb.cursors.DictCursor) 
            cursor.execute("SHOW TABLES")
            _conn.close()
            write("OK\n")
        except Exception, e:
            write("Failed\n")
            print >> sys.stderr , e

   # xmlrpc
   write("checking for xmlrpc",'.')
   try:
        import xmlrpclib
        import SimpleXMLRPCServer
        write("OK\n")
        _soapy   = True
   except Exception, e:
        write("Missing\n")
        print >> sys.stderr , e

   if _soapy:
        from threading import Thread

        write("checking functionality of XML-RPC:\n")
        write("XML-RPC server",'.')
        try:
             server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', 9090),logRequests=False) 
             server.register_function(hello)
             Thread(target=server.handle_request).start()
             write("OK\n")
        except Exception, e:
             write("Failed\n")
             print >> sys.stderr , e
             
        write("XML-RPC client",'.')
        try:
             url = "http://localhost:9090"
             server = xmlrpclib.ServerProxy(url)
             server.hello()
             write("OK\n")
        except Exception, e:
             write("Failed\n")
             print >> sys.stderr , e

   write("\n\n\n")
   # end test ########################################
   return _xml and _mysql and _soapy


libraries = [
   "iceprod",
   "iceprod-core",
   "iceprod-client",
   "iceprod-server",
   "iceprod-modules",
]
cgilibs= [
   "iceprod",
   "iceprod-core",
   "iceprod-server",
   "iceprod-modules",
]
dirs = [
   "etc",
   "log",
   "doc",
]
# Determine path to source directory based on path to this script
src_path = os.path.dirname(os.path.abspath(sys.argv[0]))
build_path = os.getcwd()

if __name__ == '__main__':
   # Retrieve arguments
   usage = "usage: %prog [options]"
   parser = OptionParser(usage)
   parser.add_option("-c", "--checkmodules", action="store_false", default=False, dest="checklibs", help="Run checks of python dependencies")
   parser.add_option("-b", "--install-base", default=build_path, dest="installbase", help="Base installation directory")
   parser.add_option("-i", "--install", default=True,action="store_true", dest="install", help="Install IceProd packages")
   parser.add_option("-n", "--no-install", action="store_false", dest="install", help="Install IceProd packages")
   parser.add_option("-O", "--optimize", default=2, dest="optimize", help="bytecode optimization level (0,1,2)")
   parser.add_option("-d", "--epydoc", action="store_true", default=False, dest="epydoc", help="Generate epydoc HTML documentation")
   parser.add_option("-g", "--cgi", action="store_true", default=False, dest="cgi", help="install cgi scripts")

   (options,args) = parser.parse_args()
   build_path = options.installbase
   if not args:
      args.append('install')

   if options.checklibs:
      if checklibs():
         write("Preparing IceProd installation\n")
      else:
         write("Skipping IceProd installation\n")
         os._exit(1)
   else:
      write("Preparing IceProd installation\n")

   
   # Create target directories
   for d in dirs:
       d = os.path.join(build_path,d)
       if not os.path.exists(d): os.makedirs(d)

   # Run setup for each package
   if options.cgi:
     for l in cgilibs:
       os.chdir(os.path.join(src_path,l))
       cmd  = "setup.py install_lib" 
       cmd += " -d %s" % os.path.join(build_path,'lib')
       os.system("python " + cmd)
       cmd  = "setup.py install_data" 
       cmd += " -d %s" % os.path.join(build_path)
       os.system(sys.executable + " " + cmd)
     os._exit(0)
   if options.install:
     for l in libraries:
       os.chdir(os.path.join(src_path,l))
       cmd  = "setup.py %s" % " ".join(args)
       if 'install' in args:
           cmd += " --install-lib %s" % os.path.join(build_path,'lib')
           cmd += " --install-scripts %s" % os.path.join(build_path,'bin')
           cmd += " --install-data %s" % build_path
           cmd += " -O%s" % options.optimize
       if 'install_lib' in args:
           cmd += " -d %s" % os.path.join(build_path,'lib')
       if 'install_bin' in args:
           cmd += " -d %s" % os.path.join(build_path,'bin')
       if 'install_data' in args:
           cmd += " -d %s" % build_path
       os.system(sys.executable + " " + cmd)

     sys.path.append(os.path.join(build_path,'lib'))
     import iceprod
     zipoutpath = abspath(join(build_path,"shared",iceprod.zipfile()))
     libdir  = abspath(join(build_path,'lib'))
     print "generating zipfile %s.zip" % zipoutpath 
     iceprod.mktar(libdir,'iceprod/__init__.py',zipoutpath)
     iceprod.mktar(libdir,'iceprod/core',zipoutpath,'a')
     iceprod.mktar(libdir,'iceprod/modules',zipoutpath,'a')



   meta = '???'
   version = '???'
   handle = os.popen("svn info " + src_path)
   for line in handle.readlines():
       if line.strip().startswith("URL"):
          url = line.replace('URL:','').strip()
          meta = url.split('/')[5]
          version = '.'.join(url.split('/')[6:])
   handle.close()

   envsh_template = open(os.path.join(src_path,'env-shell.sh'),'r')
   envsh          = open(os.path.join(build_path,'env-shell.sh'),'w')
   for line in envsh_template:
       line = line.replace('@I3PRODPATH@',build_path)
       line = line.replace('@META_PROJECT@',meta.upper())
       line = line.replace('@VERSION@',version)
       envsh.write(line)
   envsh_template.close()
   envsh.close()
   os.chmod(os.path.join(build_path,'env-shell.sh'),0755)

   if not _ssl:
      write('Server will not support SSL encrypted connections\n')

   if options.epydoc:
      cmd = "epydoc --html -o %s/doc %s/lib/iceprod" % (build_path,build_path)
      print cmd
      os.system(cmd)

write('Done.\n')
