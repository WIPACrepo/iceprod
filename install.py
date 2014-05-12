#!/bin/env python
# Install IceProd projects
import sys

# python version check to fail hard
major_ver,minor_ver = sys.version_info[:2] 
if (major_ver < 2 or (major_ver == 2 and minor_ver < 7) or
    (major_ver == 3 and minor_ver < 2)):
    raise Exception('Python is too old. IceProd requires 2.7+ or 3.2+')

import os
import subprocess
import tempfile
import shutil
from importlib import import_module

def print_tab(s,tabs=0):
    print ' '*tabs*2+s

def print_green(s,tabs=0):
    print ' '*tabs*2+'\033[32m'+s+'\033[0m'
    
def print_red(s,tabs=0):
    print ' '*tabs*2+'\033[31m'+s+'\033[0m'

def _subprocess_call(args,verbose=False,**kwargs):
    if verbose:
        print 'running command:',args,kwargs
        p = subprocess.Popen(args,**kwargs)
        p.wait()
    else:
        p = subprocess.Popen(args,stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,**kwargs)
        p.communicate()
    ret = p.returncode
    if ret:
        raise subprocess.CalledProcessError(ret,args,None)

def _check_bin(prog,args,fail=True):
    try:
        _subprocess_call(args)
    except Exception:
        print_red('cannot find '+prog,tabs=1)
        if fail:
            raise
    else:
        print_green('found '+prog,tabs=1)

def _check_python(mod,imp=None,fail=True):
    try:
        if imp:
            import_module(imp)
        else:
            import_module(mod)
    except ImportError:
        print_red('cannot find '+mod,tabs=1)
        if fail:
            raise
    else:
        print_green('found '+mod,tabs=1)

def _mkdir(d):
    p = os.path.abspath(d)
    if not os.path.isdir(p):
        os.makedirs(p)

def check_libs():
    """Check for presence of lib dependencies"""
    print 'Checking for required dependencies:'
    _check_bin('curl', ['curl','--version'])

def check_pylibs():
    """Check for presence of python module dependencies"""
    print 'Checking for required python dependencies:'
    _check_python('pycurl')
    _check_python('tornado')
    _check_python('jsonrpclib')
    _check_python('tornadorpc')
    _check_python('lxml')

def check_optional_libs(debug=False):
    """Check for presence of optional lib dependencies"""
    print 'Checking for optional dependencies:'
    _check_bin('nginx', ['nginx','-v'], fail=debug)
    _check_bin('p7zip', ['7za'], fail=debug)
    _check_bin('globus-gridftp-client', ['grid-proxy-init','-version'],
               fail=debug)
    _check_bin('git', ['git','--version'], fail=debug)
    _check_bin('squid', ['squid','-v'], fail=debug)

def check_optional_pylibs(debug=False):
    """Check for presence of optional python module dependencies"""
    print 'Checking for optional python dependencies:'
    _check_python('configobj', fail=debug)
    _check_python('pyasn1', fail=debug)
    _check_python('pyopenssl', imp='OpenSSL', fail=debug)
    _check_python('pyuv', fail=debug)
    _check_python('pyuv_tornado', fail=debug)
    _check_python('pygridftp', imp='gridftpClient', fail=debug)
    _check_python('sphinx', fail=debug)
    _check_python('coverage', fail=debug)
    _check_python('flexmock', fail=debug)

def check_globus():
    """Check for globus CA certificates"""
    print 'Checking for globus CA certificates'
    prefix = '/'
    if 'GLOBUS_LOCATION' in os.environ:
        prefix = os.environ['GLOBUS_LOCATION']
    cert_path = os.path.join(prefix,'etc','grid-security','certificates')
    if not os.path.isdir(cert_path):
        print_red('cannot find certificate path',tabs=1)
        print_tab('check that certificates are installed in '+cert_path,tabs=2)
    else:
        print_green('certificates found',tabs=1)

libraries = [
   "iceprod",
   "iceprod-core",
   "iceprod-client",
   "iceprod-server",
   "iceprod-modules",
]

def install(options):
    """Install IceProd"""
    src_path = os.getcwd()
    build_path = sys.prefix
    i3prod_path = sys.prefix
    if options.prefix:
        build_path = options.prefix
    if options.i3prod:
        i3prod_path = options.i3prod
    _mkdir(build_path)
    for d in ('bin','etc','lib','share'):
        _mkdir(os.path.join(build_path,d))
    _mkdir(i3prod_path)
    
    # Run setup for each package
    if options.install:
        for l in libraries:
            cmd = [sys.executable,'setup.py','install']
            if options.prefix:
                prefix = os.path.expandvars(os.path.expanduser(options.prefix))
                cmd.append('--prefix=%s'%prefix)
            _subprocess_call(cmd,cwd=os.path.join(src_path,l),
                             verbose=options.debug)
    
    # get the browser CA certificates from curl, which is a pem encoding
    # of the Mozilla CA bundle
    # TODO: find a better way to do this, that also keeps this up to date
    ca_path = os.path.join(build_path,'etc','cacerts.crt')
    _subprocess_call(['curl','-o'+ca_path,'http://curl.haxx.se/ca/cacert.pem'],
                     verbose=options.debug)
    
    meta = '???'
    version = '???'
    handle = subprocess.Popen(['svn','info','.'],stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    for line in handle.communicate()[0].split('\n'):
        if line.strip().startswith("URL"):
            url = line.replace('URL:','').strip()
            meta = url.split('/')[5]
            version = '.'.join(url.split('/')[6:])
    
    envsh_template = open(os.path.join(src_path,'env-shell.sh.in'),'r')
    envsh          = open(os.path.join(build_path,'env-shell.sh'),'w')
    for line in envsh_template:
        line = line.replace('@I3PRODPATH@',i3prod_path)
        line = line.replace('@META_PROJECT@',meta.upper())
        line = line.replace('@VERSION@',version)
        line = line.replace('@PREFIX@',build_path)
        envsh.write(line)
    envsh_template.close()
    envsh.close()
    os.chmod(os.path.join(build_path,'env-shell.sh'),0755)

def docs(options):
    """Build documentation"""
    print "Building documentation"
    build_path = sys.prefix
    if options.prefix:
        build_path = options.prefix
    
    _mkdir(os.path.join(build_path,'share/doc/iceprod/rst/projects'))
    _mkdir(os.path.join(build_path,'share/doc/iceprod/html'))
    
    cmd = ['sphinx-build',
           '-b html'
           '-N',
           '-a',
           '-E',
           '-d docs_cache',
           '-c iceprod/resources/docs_conf',
           os.path.join(build_path,'share/doc/iceprod/rst'),
           os.path.join(build_path,'share/doc/iceprod/html'),
          ]
    _subprocess_call(cmd,verbose=options.debug)

if __name__ == '__main__':
    # Retrieve arguments
    from optparse import OptionParser
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("-n", "--no-install", action="store_false", default=True,
                      dest="install", help="Do not install IceProd packages")
    parser.add_option("-d", "--doc", action="store_true", default=False,
                      dest="doc", help="Generate sphinx HTML documentation")
    parser.add_option("-p", "--prefix", action="store", default=None,
                      dest="prefix", help="Install prefix")
    parser.add_option("-i", "--i3prod", action="store", default=None,
                      dest="i3prod", help="IceProd working directory")
    parser.add_option("--debug", action="store_true", default=False,
                      dest="debug", help="Debugging - print errors")

    (options,args) = parser.parse_args()
    
    # check dependencies
    try:
        check_libs()
        check_pylibs()
    except:
        print "Fix required dependencies before installing."
        if options.debug:
            raise
    else:
        try:
            check_optional_libs(debug=options.debug)
            check_optional_pylibs(debug=options.debug)
            check_globus()
        except:
            pass
        if options.install:
            print "Preparing IceProd installation"
            install(options)
            if options.doc:
                docs(options)
            print "Done."
        else:
            print "Skipping IceProd installation"
