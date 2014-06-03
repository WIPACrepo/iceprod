"""
  Test script for core exe

  copyright (c) 2013 the icecube collaboration
"""

from __future__ import print_function
try:
    from core_tester import printer,glob_tests
    import logging
except:
    def printer(s,passed=True):
        if passed:
            s += ' passed'
        else:
            s += ' failed'
        print(s)
    def glob_tests(x):
        return x
    import logging
    logging.basicConfig()
logger = logging.getLogger('exe')

import os
import sys
import time
import shutil
import tempfile
import random
import string
import subprocess
from functools import partial

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest


from iceprod.core import to_log,constants
import iceprod.core.dataclasses
import iceprod.core.functions
import iceprod.core.exe
import iceprod.core.jsonRPCclient
from iceprod.core.jsonUtil import json_encode,json_decode,json_compressor

from flexmock import flexmock


class exe_test(unittest.TestCase):
    def setUp(self):
        super(exe_test,self).setUp()
        
        self.test_dir = os.path.join(os.getcwd(),'test')
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)
        
        # mock the iceprod.core.functions.download function
        self.download_called = False
        self.download_args = {}
        self.download_return = None
        download = flexmock(iceprod.core.functions)
        download.should_receive('download').replace_with(self.download)
        
        # mock the iceprod.core.functions.upload function
        self.upload_called = False
        self.upload_args = {}
        self.upload_return = None
        upload = flexmock(iceprod.core.functions)
        upload.should_receive('upload').replace_with(self.upload)
        
        # set offline mode
        iceprod.core.exe.config.options = {'offline':iceprod.core.dataclasses.Parameter('offline',True)}
    
    def tearDown(self):
        iceprod.core.exe.config.options = {}
        shutil.rmtree(self.test_dir,True)
        super(exe_test,self).tearDown()
        
    def download(self,url,local,cache=False,proxy=False,options={}):
        """mocked iceprod.functions.download"""
        self.download_called = True
        self.download_args = {'url':url,'local':local,'cache':cache,
                              'proxy':proxy,'options':options}
        if callable(self.download_return):
            data = self.download_return()
        elif self.download_return:
            data = self.download_return
        else:
            return False
        # remove tar or compress file extensions
        suffixes = ('.tar','.tgz','.gz','.tbz2','.tbz','.bz2','.bz','.rar',
                    '.lzma2','.lzma','.lz','.xz','.7z','.z','.Z')
        local2 = reduce(lambda a,b:a.replace(b,''),suffixes,local)
        if isinstance(data,dict):
            # make directory of things
            os.mkdir(local2)
            for k in data:
                with open(os.path.join(local2,k),'w') as f:
                    f.write(data[k])
        else:
            with open(local2,'w') as f:
                f.write(data)
        if iceprod.core.functions.istarred(local):
            # tar the file
            local3 = local2+'.tar'
            iceprod.core.functions.tar(local3,local2,
                                       workdir=os.path.dirname(local2))
            if '.tar' in local:
                local2 = local3
            else:
                os.rename(local3,local2)
        if iceprod.core.functions.iscompressed(local):
            iceprod.core.functions.compress(local2,local.rsplit('.',1)[-1])
        if os.path.exists(local):
            return True
        else:
            raise Exception('Something went wrong when mocking download')
        
    def upload(self,local,remote,proxy=False,options={}):
        """mocked iceprod.functions.upload"""
        self.upload_called = True
        self.upload_args = {'local':local,'remote':remote,
                            'proxy':proxy,'options':options}
        suffixes = ('.tar','.tgz','.gz','.tbz2','.tbz','.bz2','.bz','.rar',
                    '.lzma2','.lzma','.lz','.xz','.7z','.z','.Z')
        tmp_dir = tempfile.mkdtemp(dir=self.test_dir)
        try:
            if os.path.exists(local):
                # check if remote is a directory
                if os.path.splitext(local)[1] == os.path.splitext(remote)[1]:
                    newlocal = os.path.join(tmp_dir,os.path.basename(remote))
                else:
                    newlocal = os.path.join(tmp_dir,os.path.basename(local))
                # copy to temp directory
                shutil.copy(local,newlocal)
                # uncompress if necessary
                if (iceprod.core.functions.iscompressed(newlocal) or 
                    iceprod.core.functions.istarred(local)):
                    files = iceprod.core.functions.uncompress(newlocal)
                else:
                    files = newlocal
                # get data or a file listing
                if isinstance(files,basestring):
                    data = ''
                    with open(files,'r') as f:
                        data = f.read()
                else:
                    data = files
                # pass back to test
                if callable(self.upload_return):
                    self.upload_return(data)
                elif self.upload_return:
                    self.upload_return = data
                else:
                    return False
            else:
                raise Exception('uploaded local file does not exist')
            return True
        finally:
            shutil.rmtree(tmp_dir)
    
    def make_shared_lib(self):
        """Make a shared library file used for testing"""
        so_file = os.path.join(self.test_dir,'hello')[len(os.getcwd()):]
        if so_file[0] == '/':
            so_file = so_file[1:]
        
        with open(so_file+'.c','w') as f:
            f.write("""#include <Python.h>

static PyObject* say_hello(PyObject* self, PyObject* args)
{
    const char* name;
 
    if (!PyArg_ParseTuple(args, "s", &name))
        return NULL;
 
    return Py_BuildValue("s", name);
}
 
static PyMethodDef HelloMethods[] =
{
     {"say_hello", say_hello, METH_VARARGS, "Greet somebody."},
     {NULL, NULL, 0, NULL}
};
 
PyMODINIT_FUNC
 
inithello(void)
{
     (void) Py_InitModule("hello", HelloMethods);
}
""")
        from distutils.ccompiler import new_compiler
        c = new_compiler()
        pythondir = os.path.expandvars('$I3PREFIX/include/python2.7')
        logger.info('pwd: %s',os.path.expandvars('$PWD'))
        with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
            try:
                ret = c.compile([so_file+'.c'],output_dir='.',include_dirs=[pythondir])
                logger.info('ret1: %r',ret)
                ret = c.link_shared_object([so_file+'.o'],so_file+'.so')
                logger.info('ret2: %r',ret)
            except:
                ret = c.compile([so_file+'.c'],output_dir='.',include_dirs=[pythondir],
                          extra_preargs=['-fPIC'])
                logger.info('ret3: %r',ret)
                ret = c.link_shared_object([so_file+'.o'],so_file+'.so')
                logger.info('ret4: %r',ret)
        
        so = open(so_file+'.so','rb').read()
        return so
    
    
    def test_01_downloadResource(self): 
        """Test downloading a resource"""
        try:
            # set download() return value
            self.download_return = 'the data'
            
            # create an environment
            resource_url = iceprod.core.dataclasses.Parameter()
            resource_url.name = 'resource_url'
            resource_url.value = 'http://x2100.icecube.wisc.edu/downloads'
            resource_directory = iceprod.core.dataclasses.Parameter()
            resource_directory.name = 'resource_directory'
            resource_directory.value = self.test_dir
            parameters = {'resource_url': resource_url,
                          'resource_directory': resource_directory}
            env = {'parameters':parameters}
            
            # create a resource object
            r = iceprod.core.dataclasses.Resource()
            r.remote = 'stuff'
            r.local = 'localstuff'
            
            # try downloading the resource
            iceprod.core.exe.downloadResource(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('downloadResource did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('downloadResource did not write to the '
                                'expected filename of %s'%r.local)
            
            # try a compressed object
            r.remote = 'stuff2.gz'
            r.local = 'localstuff2.gz'
            r.compression = True
            
            # try downloading the resource
            iceprod.core.exe.downloadResource(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-3])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-3])
            
            # try a tarred object
            r.remote = 'stuff3.tar'
            r.local = 'localstuff3.tar'
            r.compression = True
            
            # try downloading the resource
            iceprod.core.exe.downloadResource(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-4])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-4])
            
            # try a tarred compressed object
            r.remote = 'stuff4.tar.bz2'
            r.local = 'localstuff4.tar.bz2'
            r.compression = True
            
            # try downloading the resource
            iceprod.core.exe.downloadResource(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-8])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-8])
            
            # try a tarred compressed object
            r.remote = 'stuff5.tgz'
            r.local = 'localstuff5.tgz'
            r.compression = True
            
            # try downloading the resource
            iceprod.core.exe.downloadResource(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-4])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-4])
            
            # try supplying invalid env
            r.compression = None
            try:
                iceprod.core.exe.downloadResource({},r)
            except Exception as e:
                pass
            else:
                # was supposed to throw an exception
                raise Exception,'failed to throw exception on blank env'
            
        except Exception as e:
            logger.error('Error running downloadResource test: %r',e)
            printer('Test exe.downloadResource()',False)
            raise
        else:
            printer('Test exe.downloadResource()')
    
    def test_02_downloadData(self): 
        """Test downloading a data file"""
        try:
            # set download() return value
            self.download_return = 'the data'
            
            # create an environment
            data_url = iceprod.core.dataclasses.Parameter()
            data_url.name = 'data_url'
            data_url.value = 'http://x2100.icecube.wisc.edu/downloads'
            data_directory = iceprod.core.dataclasses.Parameter()
            data_directory.name = 'data_directory'
            data_directory.value = self.test_dir
            parameters = {'data_url': data_url,
                          'data_directory': data_directory}
            env = {'parameters':parameters}
            
            # create a resource object
            r = iceprod.core.dataclasses.Data()
            r.remote = 'stuff'
            r.local = 'localstuff'
            r.type = 'permanent'
            r.movement = 'input'
            
            # try downloading the resource
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('downloadResource did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('downloadResource did not write to the '
                                'expected filename of %s'%r.local)
            
            # try a compressed object
            r.remote = 'stuff2.gz'
            r.local = 'localstuff2.gz'
            r.compression = True
            
            # try downloading the resource
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-3])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-3])
            
            # try a tarred object
            r.remote = 'stuff3.tar'
            r.local = 'localstuff3.tar'
            r.compression = True
            
            # try downloading the resource
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-4])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-4])
            
            # try a tarred compressed object
            r.remote = 'stuff4.tar.bz2'
            r.local = 'localstuff4.tar.bz2'
            r.compression = True
            
            # try downloading the resource
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-8])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-8])
            
            # try a tarred compressed object
            r.remote = 'stuff5.tgz'
            r.local = 'localstuff5.tgz'
            r.compression = True
            
            # try downloading the resource
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-4])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-4])
            
            # try supplying invalid env
            r.compression = None
            try:
                iceprod.core.exe.downloadData({},r)
            except Exception as e:
                pass
            else:
                # was supposed to throw an exception
                raise Exception,'failed to throw exception on blank env'
            
        except Exception as e:
            logger.error('Error running downloadData test: %r',e)
            printer('Test exe.downloadData()',False)
            raise
        else:
            printer('Test exe.downloadData()')
    
    def test_03_uploadData(self): 
        """Test uploading a data file"""
        try:
            # set download() return value
            self.download_return = 'the data'
            
            # create an environment
            data_url = iceprod.core.dataclasses.Parameter()
            data_url.name = 'data_url'
            data_url.value = 'http://x2100.icecube.wisc.edu/downloads'
            data_directory = iceprod.core.dataclasses.Parameter()
            data_directory.name = 'data_directory'
            data_directory.value = self.test_dir
            parameters = {'data_url': data_url,
                          'data_directory': data_directory}
            env = {'parameters':parameters}
            
            # create a resource object
            r = iceprod.core.dataclasses.Data()
            r.remote = 'stuff'
            r.local = 'localstuff'
            r.type = 'permanent'
            r.movement = 'both'
            
            # try downloading the data
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('downloadResource did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('downloadResource did not write to the '
                                'expected filename of %s'%r.local)
            # try uploading the data
            self.upload_return = 'a different value'
            iceprod.core.exe.uploadData(env,r)
            if ((not self.upload_return) or 
                self.upload_return != self.download_return):
                raise Exception('upload failed for regular data')
            
            # try a compressed object
            r.remote = 'stuff2.gz'
            r.local = 'localstuff2.gz'
            r.compression = True
            
            # try downloading the data
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-3])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-3])
            # try uploading the data
            self.upload_return = 'a different value'
            iceprod.core.exe.uploadData(env,r)
            if ((not self.upload_return) or 
                self.upload_return != self.download_return):
                raise Exception('upload failed for compressed data')
            
            # try a tarred object
            r.remote = 'stuff3.tar'
            r.local = 'localstuff3.tar'
            r.compression = True
            
            # try downloading the data
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-4])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-4])
            # try uploading the data
            self.upload_return = 'a different value'
            iceprod.core.exe.uploadData(env,r)
            if ((not self.upload_return) or 
                self.upload_return != self.download_return):
                raise Exception('upload failed for tarred data')
            
            # try a tarred compressed object
            r.remote = 'stuff4.tar.bz2'
            r.local = 'localstuff4.tar.bz2'
            r.compression = True
            
            # try downloading the data
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-8])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-8])
            # try uploading the data
            self.upload_return = 'a different value'
            iceprod.core.exe.uploadData(env,r)
            if ((not self.upload_return) or 
                self.upload_return != self.download_return):
                raise Exception('upload failed for tarred compressed data')
            
            # try a tarred compressed object
            r.remote = 'stuff5.tgz'
            r.local = 'localstuff5.tgz'
            r.compression = True
            
            # try downloading the data
            iceprod.core.exe.downloadData(env,r)
            # check for record of file in env
            if r.local not in env['files']:
                raise Exception('did not add the file '
                                '%s to the env'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local)):
                raise Exception('did not write to the '
                                'expected filename of %s'%r.local)
            if not os.path.isfile(os.path.join(self.test_dir,r.local[:-4])):
                raise Exception('did not uncompress to the '
                                'expected filename of %s'%r.local[:-4])
            # try uploading the data
            self.upload_return = 'a different value'
            iceprod.core.exe.uploadData(env,r)
            if ((not self.upload_return) or 
                self.upload_return != self.download_return):
                raise Exception('upload failed for tarred compressed ' +
                                'data (tgz)')
            
            # try supplying invalid env
            r.compression = None
            try:
                iceprod.core.exe.uploadData({},r)
            except Exception as e:
                pass
            else:
                # was supposed to throw an exception
                raise Exception,'failed to throw exception on blank env'
            
        except Exception as e:
            logger.error('Error running uploadData test: %r',e)
            printer('Test exe.uploadData()',False)
            raise
        else:
            printer('Test exe.uploadData()')
    
    def test_04_setupClass(self):    
        """Test setting up a class"""
        try:
            # set download() return value
            self.download_return = 'class GridFTP(): pass'
            
            # create a class object
            r = iceprod.core.dataclasses.Class()
            r.name = 'datatransfer.py'
            r.src = 'datatransfer.py'
            
            # create an env
            local_temp = iceprod.core.dataclasses.Parameter()
            local_temp.name = 'local_temp'
            local_temp.value = os.path.join(self.test_dir,'classes')
            env = {'parameters':{'local_temp':local_temp}}
            
            # try setting up the class
            try:
                iceprod.core.exe.setupClass(env,r)
            except:
                logger.error('exe.setupClass() error')
                raise
            # check for record of file in env
            if r.name not in env['classes']:
                raise Exception('setupClass did not add the src file' +
                                ' %s to the env'%r.local)
            # check for ability to use class
            try:
                import datatransfer
                gftp = datatransfer.GridFTP()
            except:
                raise Exception('setupClass did not make class available' +
                                ' for import')
            
        except Exception as e:
            logger.error('Error running setupClass test: %s',str(e))
            printer('Test exe.setupClass()',False)
            raise
        else:
            printer('Test exe.setupClass()')    
    
    def test_05_setupClass_Env(self):    
        """Test setting up a class with an environment"""
        try:
            # set download() return value
            self.download_return = 'class GridFTP(): pass'
            
            # create a class object
            r = iceprod.core.dataclasses.Class()
            r.name = 'datatransfer.py'
            r.src = 'datatransfer.py'
            r.env_vars = 'I3_BUILD=$CLASS'
            
            # create an env
            local_temp = iceprod.core.dataclasses.Parameter()
            local_temp.name = 'local_temp'
            local_temp.value = os.path.join(self.test_dir,'classes')
            env = {'parameters':{'local_temp':local_temp}}
            
            # try setting up the class
            try:
                iceprod.core.exe.setupClass(env,r)
            except:
                logger.error('exe.setupClass() error')
                raise
            # check for record of file in env
            if r.name not in env['classes']:
                raise Exception('setupClass did not add the src file' +
                                ' %s to the env'%r.local)
            # check for ability to use class
            try:
                import datatransfer
                gftp = datatransfer.GridFTP()
            except:
                raise Exception('setupClass did not make class available' +
                                ' for import')
            # test env
            if 'I3_BUILD' not in os.environ or os.environ['I3_BUILD'] == '$CLASS':
                raise Exception('I3_BUILD not in environment')
            
            
            # create a class object
            r = iceprod.core.dataclasses.Class()
            r.name = 'datatransfer.py'
            r.src = 'datatransfer.py'
            r.env_vars = 'tester=1:2:3;PATH=$PWD;PYTHONPATH=$PWD/test'
            
            # create an env
            local_temp = iceprod.core.dataclasses.Parameter()
            local_temp.name = 'local_temp'
            local_temp.value = os.path.join(self.test_dir,'classes')
            env = {'parameters':{'local_temp':local_temp}}
            
            # try setting up the class
            try:
                iceprod.core.exe.setupClass(env,r)
            except:
                logger.error('exe.setupClass() error')
                raise
            # check for record of file in env
            if r.name not in env['classes']:
                raise Exception('setupClass did not add the src file' +
                                ' %s to the env'%r.local)
            # check for ability to use class
            try:
                import datatransfer
                gftp = datatransfer.GridFTP()
            except:
                raise Exception('setupClass did not make class available' +
                                ' for import')
            # test env
            if 'tester' not in os.environ or os.environ['tester'] != '1:2:3':
                raise Exception('tester not in environment')
            if 'PATH' not in os.environ or '$PWD' not in os.environ['PATH']:
                raise Exception('PATH not in environment')
            if ('PATH' not in os.environ or 
                '$PWD/test' not in os.environ['PYTHONPATH'] or
                '$PWD/test' not in sys.path):
                raise Exception('PYTHONPATH not in environment')
            
        except Exception as e:
            logger.error('Error running setupClass test2: %s',str(e))
            printer('Test exe.setupClass() with env',False)
            raise
        else:
            printer('Test exe.setupClass() with env') 
    
    def test_06_setupProject(self):    
        """Test setting up a project"""
        try:
            # mock a test module
            def tester(*args,**kwargs):
                tester.here = True
            import iceprod.modules.ipmodule
            if 'Test' in [x for x in dir(iceprod.modules.ipmodule)
                          if x[0] != '_']:
                ipmodule = flexmock(iceprod.modules.ipmodule)
                ipmodule.should_receive('Test').replace_with(tester)
            else:
                iceprod.modules.ipmodule.Test = tester
            
            # create a project object based on full path
            r = iceprod.core.dataclasses.Project()
            r.name = 'ipmodule'
            r.class_name = 'iceprod.modules.ipmodule'
            
            # create an env          
            env = {}
            
            # try setting up the project
            try:
                iceprod.core.exe.setupProject(env,r)
            except:
                logger.error('exe.setupProject() error')
                raise
            # check for record of file in env
            if r.name not in env['projects']:
                raise Exception('setupProject did not add the project %s' +
                                ' to the env'%r.name)
            # check for ability to use project
            tester.here = False
            try:
                x = env['projects'][r.name].Test()
            except Exception as e:
                logger.error('%r',e,exc_info=True)
                raise Exception('setupProject did not make project ' +
                                'available for use')
            if tester.here is False:
                raise Exception('mocked test function failed to be called')
                
            # create a project object from iceprod.modules
            r = iceprod.core.dataclasses.Project()
            r.name = 'ipmodule2'
            r.class_name = 'ipmodule'
            
            # try setting up the project
            try:
                iceprod.core.exe.setupProject(env,r)
            except:
                logger.error('exe.setupProject() error')
                raise
            # check for record of file in env
            if r.name not in env['projects']:
                raise Exception('setupProject did not add the project %s' +
                                ' to the env'%r.name)
            # check for ability to use project
            tester.here = False
            try:
                x = env['projects'][r.name].Test()
            except Exception as e:
                logger.error('%r',e,exc_info=True)
                raise Exception('setupProject did not make project ' +
                                'available for use')
            if tester.here is False:
                raise Exception('mocked test function failed to be called')
            
            
            # try setting up a duplicate project
            try:
                iceprod.core.exe.setupProject(env,r)
            except:
                logger.error('exe.setupProject() error')
                raise
            # check for record of file in env
            if r.name not in env['projects']:
                raise Exception('setupProject did not add the project %s' +
                                ' to the env 2'%r.name)          
            # check for ability to use project
            tester.here = False
            try:
                x = env['projects'][r.name].Test()
            except Exception as e:
                logger.error('%r',e,exc_info=True)
                raise Exception('setupProject did not make project ' +
                                'available for use 2')
            if tester.here is False:
                raise Exception('mocked test function failed to be called 2')
            
            
            # create a bad project object
            r = iceprod.core.dataclasses.Project()
            r.name = 'ipmodule3'
            r.class_name = 'badmodule'
            
            # try setting up the project
            try:
                iceprod.core.exe.setupProject(env,r)
            except:
                logger.error('exe.setupProject() error')
                raise
            # check for record of file in env
            if r.name in env['projects']:
                raise Exception('setupProject added the project %s to the'+
                                ' env when it wasn\'t supposed to'%r.name)
            
            
            # try setting up an empty project
            try:
                iceprod.core.exe.setupProject(env,None)
            except:
                pass
            else:
                raise Exception('setupProject added the project %s to the'+
                                ' env when it wasn\'t supposed to'%r.name)
            
        except Exception as e:
            logger.error('Error running setupProject test: %s',str(e))
            printer('Test exe.setupProject()',False)
            raise
        else:
            printer('Test exe.setupProject()')
    
    def test_10_setupenv_basic(self):    
        """Test basic setupenv functionality"""
        try:
            obj = iceprod.core.dataclasses.Steering()
            # create an empty env
            try:
                empty_env = iceprod.core.exe.setupenv(obj)
            except:
                logger.error('creating empty env failed')
                raise
                
            # create secondary env
            try:
                env2 = iceprod.core.exe.setupenv(obj,empty_env)
            except:
                logger.error('creating secondary env failed')
                raise
            
            # create something in env2, and check it's not in empty_env
            env2['test'] = 'testing'
            if 'test' in empty_env:
                raise Exception('env2 is a direct link to empty_env')
            
            # make new env from env2, and check it has that value
            try:
                env3 = iceprod.core.exe.setupenv(obj,env2)
            except:
                logger.error('creating env3 failed')
                raise
            if 'test' not in env3 or env3['test'] != 'testing':
                raise Exception('env3 does not have test value')
                
            # check that modifying a value in env3 has no effect on env2
            env3['test'] = 'abcd'
            if env2['test'] != 'testing':
                raise Exception, 'env3 is a direct link to env2'
                
            # check that modifying a value in env2 has no effect on env3
            env2['test'] = 'dcba'
            if env3['test'] != 'abcd':
                raise Exception('env2 is a direct link to env3')
            
            # do second level checks, like dealing with parameters
            obj.parameters = {}
            try:
                env4 = iceprod.core.exe.setupenv(obj)
            except:
                logger.error('creating empty env failed')
                raise
            try:
                env5 = iceprod.core.exe.setupenv(obj,env4)
            except:
                logger.error('creating empty env failed')
                raise
            env5['parameters']['test'] = 1
            if 'test' in env4['parameters']:
                raise Exception('adding a parameter in env5 adds it to env4')
            try:
                env6 = iceprod.core.exe.setupenv(obj,env5)
            except:
                logger.error('creating empty env failed')
                raise
            env6['parameters']['test'] = 2
            if env5['parameters']['test'] == 2:
                raise Exception('modifying a parameter in env6 modifies ' +
                                'it in env5')
            
        except Exception as e:
            logger.error('Error running basic setupenv test: %s',str(e))
            printer('Test exe.setupenv(): basic',False)
            raise
        else:
            printer('Test exe.setupenv(): basic')
    
    def test_11_setupenv_steering(self):    
        """Test setupenv with steering object"""
        try:
            # create the steering object
            steering = iceprod.core.dataclasses.Steering()
            
            # create a resource object
            r = iceprod.core.dataclasses.Resource()
            r.remote = 'globus.tar.gz'
            r.local = 'globus.tar.gz'
            steering.resources.append(r)
            
            # create some parameters
            test_param = iceprod.core.dataclasses.Parameter()
            test_param.name = 'test_param'
            test_param.value = 'value'
            steering.parameters = {'test_param':test_param}
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing resource directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'resource_directory'
            o.value = os.path.join(self.test_dir,'resources')
            options['resource_directory'] = o 
            
            # set download() return value
            self.download_return = 'the data'
            
            # create the env
            try:
                env = iceprod.core.exe.setupenv(steering,
                                                {'parameters':options})
            except:
                logger.error('creating env failed')
                raise
            
            # test parameters
            for p in steering.parameters:
                if p not in env['parameters']:
                    raise Exception('Parameters were not applied ' +
                                    'correctly: missing %r'%p)
            
            # test options
            for p in options:
                if p not in env['parameters']:
                    raise Exception('Options were not applied ' +
                                    'correctly: missing %r'%p)
            
            # test resource
            if r.local not in env['files']:
                raise Exception('downloadResource did not add the file ' +
                                '%s to the env'%r.local)
            if (env['files'][r.local] != 
                os.path.join(self.test_dir,'resources',r.local)):
                raise Exception('downloadResource did not return the ' +
                                'expected filename of %s' % 
                                os.path.join(self.test_dir,'resources',
                                             r.local))
            if not os.path.isfile(env['files'][r.local]):
                raise Exception('downloadResource did not write to the ' +
                                'expected filename of %s' % 
                                env['files'][r.local])
            
        except Exception as e:
            logger.error('Error running setupenv steering test: %s',str(e))
            printer('Test exe.setupenv(): steering',False)
            raise
        else:
            printer('Test exe.setupenv(): steering')

    def test_12_destroyenv_steering(self):    
        """Test destroyenv with steering object"""
        try:
            # create the steering object
            steering = iceprod.core.dataclasses.Steering()
            
            # create a data object
            r = iceprod.core.dataclasses.Data()
            r.remote = 'globus.tar.gz'
            r.local = 'globus.tar.gz'
            r.type = 'permanent'
            r.movement = 'both'
            steering.data.append(r)
            
            # create parameters
            test_param = iceprod.core.dataclasses.Parameter()
            test_param.name = 'test_param'
            test_param.value = 'value'
            steering.parameters = {'test_param':test_param}
                
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o
            
            # set download() return value
            self.download_return = 'the data'
            
            # create the env
            try:
                env = iceprod.core.exe.setupenv(steering,
                                                {'parameters':options})
            except:
                logger.error('creating env failed')
                raise
            
            # destroy the env
            try:
                iceprod.core.exe.destroyenv(env)
            except:
                logger.error('destroyenv error')
                raise
            
            # try a file deletion
            filename = os.path.join(self.test_dir,'test_file')
            with open(filename,'w') as f:
                f.write('this is a test')
            
            # create the env
            try:
                env = iceprod.core.exe.setupenv(steering,
                                                {'parameters':options})
            except:
                logger.error('creating env failed')
                raise
            
            env['deletions'] = [filename]
            
            # destroy the env
            try:
                iceprod.core.exe.destroyenv(env)
            except:
                logger.error('destroyenv error')
                raise
            
            if os.path.exists(filename):
                raise Exception('failed to delete file')
                
            # try environment reset
            
            # create the env
            try:
                env = iceprod.core.exe.setupenv(steering,
                                                {'parameters':options,
                                                 'deletions':[filename]})
            except:
                logger.error('creating env failed')
                raise
            
            os.environ['MyTestVar'] = 'testing'
            
            # destroy the env
            try:
                iceprod.core.exe.destroyenv(env)
            except:
                logger.error('destroyenv error')
                raise
            
            if 'MyTestVar' in os.environ:
                raise Exception('failed to delete environment entry')
            
            if os.path.exists(filename):
                raise Exception('failed to delete file')
            
        except Exception as e:
            logger.error('Error running destroyenv steering test: %s',str(e))
            printer('Test exe.destroyenv(): steering',False)
            raise
        else:
            printer('Test exe.destroyenv(): steering')

    def test_20_runmodule_iceprod_nosrc(self):    
        """Test runmodule with iceprod module and no src"""
        try:
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'iceprod.modules.ipmodule.Test_old'
            
            # create parameters
            hello = iceprod.core.dataclasses.Parameter()
            hello.name = 'greeting'
            hello.value = 'new greeting'
            module.parameters = {hello.name: hello}
            
            # make project
            r = iceprod.core.dataclasses.Project()
            r.name = 'ipmodule'
            r.class_name = 'iceprod.modules.ipmodule'
            module.projects.append(r)
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the module
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runmodule(env,module)
                except:
                    logger.error('running the module failed')
                    raise
            if ret != 'Test_old IPBaseClass':
                raise Exception('failed to call Test_old.Execute()')
            
        except Exception as e:
            logger.error('Error running runmodule test for iceprod (project, long name): %s',str(e))
            printer('Test exe.runmodule(): iceprod module (project, long)',False)
            raise
        else:
            printer('Test exe.runmodule(): iceprod module (project, long)')

    def test_21_runmodule_iceprod_nosrc(self):    
        """Test runmodule with iceprod module and no src"""
        try:
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'ipmodule.Test_old'
            
            # create parameters
            hello = iceprod.core.dataclasses.Parameter()
            hello.name = 'greeting'
            hello.value = 'new greeting'
            module.parameters = {hello.name: hello}
            
            # make project
            r = iceprod.core.dataclasses.Project()
            r.name = 'ipmodule'
            r.class_name = 'iceprod.modules.ipmodule'
            module.projects.append(r)
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the module
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runmodule(env,module)
                except:
                    logger.error('running the module failed')
                    raise
            if ret != 'Test_old IPBaseClass':
                raise Exception('failed to call Test_old.Execute()')
            
        except Exception as e:
            logger.error('Error running runmodule test for (project, short name): %s',str(e))
            printer('Test exe.runmodule(): iceprod module (project, short)',False)
            raise
        else:
            printer('Test exe.runmodule(): iceprod module (project, short)')

    def test_22_runmodule_iceprod_src(self):    
        """Test runmodule with iceprod module and src"""
        try:
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.src = 'test.py'
            module.running_class = 'test.Test'
            
            # set download() return value
            def down():
                if self.download_args['url'].endswith('test.py'):
                    return """
from iceprod.modules.ipmodule import IPBaseClass
class Test(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 'Tester'
"""
            self.download_return = down
            
            # create parameters
            hello = iceprod.core.dataclasses.Parameter()
            hello.name = 'greeting'
            hello.value = 'new greeting'
            module.parameters = {hello.name: hello}
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the module
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runmodule(env,module)
                except:
                    logger.error('running the module failed')
                    raise
            if ret != 'Tester':
                raise Exception('failed to call Test.Execute()')
            
        except Exception as e:
            logger.error('Error running runmodule test for (src, long): %s',str(e))
            printer('Test exe.runmodule(): iceprod module (src, long)',False)
            raise
        else:
            printer('Test exe.runmodule(): iceprod module (src, long)')
    
    def test_23_runmodule_iceprod_src(self):    
        """Test runmodule with iceprod module and src"""
        try:
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.src = 'test.py'
            module.running_class = 'Test'
            
            # set download() return value
            def down():
                if self.download_args['url'].endswith('test.py'):
                    return """
from iceprod.modules.ipmodule import IPBaseClass
class Test(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 'Tester'
"""
            self.download_return = down
            
            # create parameters
            hello = iceprod.core.dataclasses.Parameter()
            hello.name = 'greeting'
            hello.value = 'new greeting'
            module.parameters = {hello.name: hello}
            
            # make project
            r = iceprod.core.dataclasses.Project()
            r.name = 'ipmodule'
            r.class_name = 'iceprod.modules.ipmodule'
            module.projects.append(r)
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the module
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runmodule(env,module)
                except:
                    logger.error('running the module failed')
                    raise
            if ret != 'Tester':
                raise Exception('failed to call Test.Execute()')
            
        except Exception as e:
            logger.error('Error running runmodule test for (src, short): %s',str(e))
            printer('Test exe.runmodule(): iceprod module (src, short)',False)
            raise
        else:
            printer('Test exe.runmodule(): iceprod module (src, short)')


    def test_30_runmodule_simple(self):    
        """Test runmodule with simple script"""
        try:
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.src = 'test.py'
            module.running_class = 'test.Test'
            
            # set download() return value
            def down():
                if self.download_args['url'].endswith('test.py'):
                    return """
def Test():
    return 'Tester'
"""
            self.download_return = down
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the module
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runmodule(env,module)
                except:
                    logger.error('running the module failed')
                    raise
            if ret != 'Tester':
                raise Exception('failed to call test.Test()')
        
            # try with short form of class
            module.running_class = 'Test'
            
            # run the module
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runmodule(env,module)
                except:
                    logger.error('running the module failed (short)')
                    raise
            if ret != 'Tester':
                raise Exception('failed to call Test()')
            
        except Exception as e:
            logger.error('Error running runmodule test for simple from src: %s',str(e))
            printer('Test exe.runmodule(): simple module from src',False)
            raise
        else:
            printer('Test exe.runmodule(): simple module from src')

    def test_31_runmodule_script(self):    
        """Test runmodule with raw python script"""
        try:
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.src = 'test.py'
            
            # set download() return value
            def down():
                if self.download_args['url'].endswith('test.py'):
                    return """
def Test():
    return 'Tester'
if __name__ == '__main__':
    Test()
"""
            self.download_return = down
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the module
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runmodule(env,module)
                except:
                    logger.error('running the module failed')
                    raise
            if ret != 0:
                raise Exception('failed to call script')
            
        except Exception as e:
            logger.error('Error running runmodule test for python script: %s',str(e))
            printer('Test exe.runmodule(): python script',False)
            raise
        else:
            printer('Test exe.runmodule(): python script')

    def test_32_runmodule_script(self):    
        """Test runmodule with raw bash script"""
        try:
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.src = 'test.sh'
            
            # set download() return value
            def down():
                if self.download_args['url'].endswith('test.sh'):
                    return """
uname -a
echo "test"
"""
            self.download_return = down
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the module
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runmodule(env,module)
                except:
                    logger.error('running the module failed')
                    raise
            if ret != 0:
                raise Exception('failed to call script')
            
        except Exception as e:
            logger.error('Error running runmodule test for shell script: %s',str(e))
            printer('Test exe.runmodule(): shell script',False)
            raise
        else:
            printer('Test exe.runmodule(): shell script')


    def test_40_runmodule_icetray(self):    
        """Test runmodule with linked libraries"""
        try:
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'iceprod_test.Test'
            
            c = iceprod.core.dataclasses.Class()
            c.name = 'test'
            c.src = 'test.tar.gz'
            module.classes.append(c)
            
            # make .so file
            so = self.make_shared_lib()
            
            # set download() return value
            self.download_return = {'iceprod_test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""",
                                    'hello.so':so}
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the module
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runmodule(env,module)
                except:
                    logger.error('running the module failed')
                    raise
            if ret != 'Tester':
                raise Exception('failed to call iceprod_test.Test()')
            
        except Exception as e:
            logger.error('Error running runmodule test with linked libraries: %s',str(e))
            printer('Test exe.runmodule(): with linked libraries',False)
            raise
        else:
            printer('Test exe.runmodule(): with linked libraries')


    def test_50_runtray(self):    
        """Test runtray"""
        try:
            # create the tray object
            tray = iceprod.core.dataclasses.Tray()
            tray.name = 'tray'
            
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'iceprod_test.Test'
            
            c = iceprod.core.dataclasses.Class()
            c.name = 'test'
            c.src = 'test.tar.gz'
            module.classes.append(c)
            tray.modules[module.name] = module
            
            # create another module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module2'
            module.running_class = 'test.Test'
            module.src = 'test.py'
            tray.modules[module.name] = module
            
            # make .so file
            so = self.make_shared_lib()
            
            # set download() return value
            def dw():
                if self.download_args['url'].endswith('test.tar.gz'):
                    return {'iceprod_test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""",
                                    'hello.so':so}
                else:
                    return """
def Test():
    return 'Tester2'
"""
            self.download_return = dw
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the tray
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runtray(env,tray)
                except:
                    logger.error('running the tray failed')
                    raise
            if ret != ['Tester','Tester2']:
                logger.info('ret=%r',ret)
                raise Exception('failed to call modules')
            
        except Exception as e:
            logger.error('Error running runtray test: %s',str(e))
            printer('Test exe.runtray()',False)
            raise
        else:
            printer('Test exe.runtray()')


    def test_51_runtray_iter(self):    
        """Test runtray iterations"""
        try:
            # create the tray object
            tray = iceprod.core.dataclasses.Tray()
            tray.name = 'tray'
            tray.iterations = 3
            
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'iceprod_test.Test'
            
            c = iceprod.core.dataclasses.Class()
            c.name = 'test'
            c.src = 'test.tar.gz'
            module.classes.append(c)
            tray.modules[module.name] = module
            
            # create another module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module2'
            module.running_class = 'test.Test'
            module.src = 'test.py'
            tray.modules[module.name] = module
            
            # make .so file
            so = self.make_shared_lib()
            
            # set download() return value
            def dw():
                if self.download_args['url'].endswith('test.tar.gz'):
                    return {'iceprod_test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""",
                                    'hello.so':so}
                else:
                    return """
def Test():
    return 'Tester2'
"""
            self.download_return = dw
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the tray
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runtray(env,tray)
                except:
                    logger.error('running the tray failed')
                    raise
            if ret != [['Tester','Tester2'],
                       ['Tester','Tester2'],
                       ['Tester','Tester2']]:
                logger.info('ret=%r',ret)
                raise Exception('failed to call modules')
            
        except Exception as e:
            logger.error('Error running runtray iterations test: %s',str(e))
            printer('Test exe.runtray(): iterations',False)
            raise
        else:
            printer('Test exe.runtray(): iterations')


    def test_60_runtask(self):    
        """Test runtask"""
        try:
            # create the task object
            task = iceprod.core.dataclasses.Task()
            task.name = 'task'
            
            # create the tray object
            tray = iceprod.core.dataclasses.Tray()
            tray.name = 'tray'
            
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'iceprod_test.Test'
            
            c = iceprod.core.dataclasses.Class()
            c.name = 'test'
            c.src = 'test.tar.gz'
            module.classes.append(c)
            tray.modules[module.name] = module
            
            # create another module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module2'
            module.running_class = 'test.Test'
            module.src = 'test.py'
            tray.modules[module.name] = module
            
            # add tray to task
            task.trays[tray.name] = tray
            
            # make .so file
            so = self.make_shared_lib()
            
            # set download() return value
            def dw():
                if self.download_args['url'].endswith('test.tar.gz'):
                    return {'iceprod_test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""",
                                    'hello.so':so}
                else:
                    return """
def Test():
    return 'Tester2'
"""
            self.download_return = dw
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the tray
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runtask(env,task)
                except:
                    logger.error('running the tray failed')
                    raise
            if ret != ['Tester','Tester2']:
                logger.info('ret=%r',ret)
                raise Exception('failed to call modules')
            
        except Exception as e:
            logger.error('Error running runtask test: %s',str(e))
            printer('Test exe.runtask()',False)
            raise
        else:
            printer('Test exe.runtask()')

    def test_61_runtask_multi(self):    
        """Test runtask with multiple trays"""
        try:
            # create the task object
            task = iceprod.core.dataclasses.Task()
            task.name = 'task'
            
            # create the tray object
            tray = iceprod.core.dataclasses.Tray()
            tray.name = 'tray'
            
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'iceprod_test.Test'
            
            c = iceprod.core.dataclasses.Class()
            c.name = 'test'
            c.src = 'test.tar.gz'
            module.classes.append(c)
            tray.modules[module.name] = module
            
            # create another module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module2'
            module.running_class = 'test.Test'
            module.src = 'test.py'
            tray.modules[module.name] = module
            
            # add tray to task
            task.trays[tray.name] = tray
            
            # create the tray object
            tray = iceprod.core.dataclasses.Tray()
            tray.name = 'tray2'
            
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'iceprod_test.Test'
            
            c = iceprod.core.dataclasses.Class()
            c.name = 'test'
            c.src = 'test.tar.gz'
            module.classes.append(c)
            tray.modules[module.name] = module
            
            # create another module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module2'
            module.running_class = 'test.Test'
            module.src = 'test.py'
            tray.modules[module.name] = module
            
            # add tray to task
            task.trays[tray.name] = tray
            
            # make .so file
            so = self.make_shared_lib()
            
            # set download() return value
            def dw():
                if self.download_args['url'].endswith('test.tar.gz'):
                    return {'iceprod_test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""",
                                    'hello.so':so}
                else:
                    return """
def Test():
    return 'Tester2'
"""
            self.download_return = dw
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the tray
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runtask(env,task)
                except:
                    logger.error('running the tray failed')
                    raise
            if ret != [['Tester','Tester2'],
                       ['Tester','Tester2']]:
                logger.info('ret=%r',ret)
                raise Exception('failed to call modules')
            
        except Exception as e:
            logger.error('Error running runtask multi test: %s',str(e))
            printer('Test exe.runtask(): multiple trays',False)
            raise
        else:
            printer('Test exe.runtask(): multiple trays')

    def test_62_runtask_multi_iter(self):    
        """Test runtask with multiple trays and iterations"""
        try:
            # create the task object
            task = iceprod.core.dataclasses.Task()
            task.name = 'task'
            
            # create the tray object
            tray = iceprod.core.dataclasses.Tray()
            tray.name = 'tray'
            
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'iceprod_test.Test'
            
            c = iceprod.core.dataclasses.Class()
            c.name = 'test'
            c.src = 'test.tar.gz'
            module.classes.append(c)
            tray.modules[module.name] = module
            
            # create another module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module2'
            module.running_class = 'test.Test'
            module.src = 'test.py'
            tray.modules[module.name] = module
            
            # add tray to task
            task.trays[tray.name] = tray
            
            # create the tray object
            tray = iceprod.core.dataclasses.Tray()
            tray.name = 'tray2'
            tray.iterations = 3
            
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module'
            module.running_class = 'iceprod_test.Test'
            
            c = iceprod.core.dataclasses.Class()
            c.name = 'test'
            c.src = 'test.tar.gz'
            module.classes.append(c)
            tray.modules[module.name] = module
            
            # create another module object
            module = iceprod.core.dataclasses.Module()
            module.name = 'module2'
            module.running_class = 'test.Test'
            module.src = 'test.py'
            tray.modules[module.name] = module
            
            # add tray to task
            task.trays[tray.name] = tray
            
            # make .so file
            so = self.make_shared_lib()
            
            # set download() return value
            def dw():
                if self.download_args['url'].endswith('test.tar.gz'):
                    return {'iceprod_test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""",
                                    'hello.so':so}
                else:
                    return """
def Test():
    return 'Tester2'
"""
            self.download_return = dw
            
            # check that validate, resource_url, debug are in options
            options = {}
            if 'validate' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'validate'
                o.value = str(True)
                options['validate'] = o
            if 'resource_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'resource_url'
                o.value = 'http://x2100.icecube.wisc.edu/downloads'
                options['resource_url'] = o
            if 'debug' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'debug'
                o.value = str(False)
                options['debug'] = o
            
            # make sure some basic options are set
            if 'data_url' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'data_url'
                o.value = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
                options['data_url'] = o
            if 'svn_repository' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'svn_repository'
                o.value = 'http://code.icecube.wisc.edu/svn/'
                options['svn_repository'] = o
            if 'job_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'job_temp'
                o.value = os.path.join(self.test_dir,'job_temp')
                options['job_temp'] = o
            if 'local_temp' not in options:
                o = iceprod.core.dataclasses.Parameter()
                o.name = 'local_temp'
                o.value = os.path.join(self.test_dir,'local_temp')
                options['local_temp'] = o
            
            # set testing data directory
            o = iceprod.core.dataclasses.Parameter()
            o.name = 'data_directory'
            o.value = os.path.join(self.test_dir,'data')
            options['data_directory'] = o 
            
            # set env
            env = {'parameters': options}
            
            # run the tray
            with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
                try:
                    ret = iceprod.core.exe.runtask(env,task)
                except:
                    logger.error('running the tray failed')
                    raise
            if ret != [['Tester','Tester2'],
                       [['Tester','Tester2'],
                        ['Tester','Tester2'],
                        ['Tester','Tester2']]]:
                logger.info('ret=%r',ret)
                raise Exception('failed to call modules')
            
        except Exception as e:
            logger.error('Error running runtask multi iter test: %s',str(e))
            printer('Test exe.runtask(): multiple trays and iterations',False)
            raise
        else:
            printer('Test exe.runtask(): multiple trays and iterations')
    
    
    def test_90_setupjsonRPC(self):
        """Test setupjsonRPC"""
        try:
            # mock the JSONRPC class
            def start(*args,**kwargs):
                start.args = args
                start.kwargs = kwargs
            start.args = None
            start.kwargs = None
            jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
            jsonrpc.should_receive('start').replace_with(start)
            def f(*args,**kwargs):
                if kwargs['func_name'] in f.returns:
                    ret = f.returns[kwargs['func_name']]
                else:
                    ret = Exception('jsonrpc error')
                logger.debug('f(func_name=%s) returns %r',kwargs['func_name'],ret)
                if 'callback' in kwargs:
                    kwargs['callback'](ret)
                else:
                    return ret
            jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))
            
            address = 'http://test:9080'
            passkey = 'ksdf8n4'
            #ssl_options = None
            f.returns = {'echo':'e'}
            try:
                iceprod.core.exe.setupjsonRPC(address,passkey)
            except:
                logger.error('running setupjsonRPC failed')
                raise
            if (('address' in start.kwargs and start.kwargs['address'] != address) or 
                ('address' not in start.kwargs and 
                 (len(start.args) < 1 or start.args[0] != address))):
                raise Exception('JSONRPC.start() does not have address')
            if 'address' not in start.kwargs:
                start.args = start.args[1:]
            if (('passkey' in start.kwargs and start.kwargs['passkey'] != passkey) or 
                ('passkey' not in start.kwargs and 
                 (len(start.args) < 1 or start.args[0] != passkey))):
                raise Exception('JSONRPC.start() does not have passkey')
            #if (('ssl_options' in start.kwargs and start.kwargs != ssl_options) or 
            #    args[0] != ssl_options):
            #    raise Exception('JSONRPC.start() does not have ssl_options')
        
        except Exception as e:
            logger.error('Error running setupjsonRPC test: %s',str(e))
            printer('Test exe.setupjsonRPC()',False)
            raise
        else:
            printer('Test exe.setupjsonRPC()')

    def test_91_downloadtask(self):
        """Test downloadtask"""
        try:
            # mock the JSONRPC class
            task = 'a task'
            def new_task(platform=None, hostname=None, ifaces=None,
                         gridspec=None,python_unicode=None):
                new_task.called = True
                new_task.platform = platform
                new_task.hostname = hostname
                new_task.ifaces = ifaces
                new_task.gridspec = gridspec
                new_task.python_unicode = python_unicode
                return task
            new_task.called = False
            def f(*args,**kwargs):
                name = kwargs.pop('func_name')
                if 'callback' in kwargs:
                    cb = kwargs.pop('callback')
                else:
                    cb = None
                if name == 'new_task':
                    ret = new_task(*args,**kwargs)
                else:
                    ret = Exception()
                if cb:
                    cb(ret)
                else:
                    return ret
            jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
            jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))
            
            if 'PLATFORM' not in os.environ:
                os.environ['PLATFORM'] = 'other'
            platform = os.environ['PLATFORM']
            hostname = iceprod.core.functions.gethostname()
            ifaces = iceprod.core.functions.getInterfaces()
            gridspec = 'thegrid'
            python_unicode = 'ucs4' if sys.maxunicode == 1114111 else 'ucs2'
            f.returns = {'echo':'e'}
            try:
                iceprod.core.exe.downloadtask(gridspec)
            except:
                logger.error('running downloadtask failed')
                raise
            if not new_task.called:
                raise Exception('JSONRPC.new_task() not called')
            if new_task.platform != platform:
                raise Exception('JSONRPC.new_task() platform !=')
            if new_task.hostname != hostname:
                raise Exception('JSONRPC.new_task() hostname !=')
            if new_task.ifaces != ifaces:
                raise Exception('JSONRPC.new_task() ifaces !=')
            if new_task.gridspec != gridspec:
                raise Exception('JSONRPC.new_task() gridspec !=')
            if new_task.python_unicode != python_unicode:
                raise Exception('JSONRPC.new_task() python_unicode !=')
        
        except Exception as e:
            logger.error('Error running downloadtask test: %s',str(e))
            printer('Test exe.downloadtask()',False)
            raise
        else:
            printer('Test exe.downloadtask()')

    def test_92_finishtask(self):
        """Test finishtask"""
        try:
            # mock the JSONRPC class
            task_id = 'a task'
            stats = {'test':True}
            def finish_task(task_id,stats={}):
                finish_task.called = True
                finish_task.task_id = task_id
                finish_task.stats = stats
                return None
            finish_task.called = False
            def f(*args,**kwargs):
                name = kwargs.pop('func_name')
                if 'callback' in kwargs:
                    cb = kwargs.pop('callback')
                else:
                    cb = None
                if name == 'finish_task':
                    ret = finish_task(*args,**kwargs)
                else:
                    ret = Exception()
                if cb:
                    cb(ret)
                else:
                    return ret
            jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
            jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))
            iceprod.core.exe.config.options['task_id'] = iceprod.core.dataclasses.Parameter('task_id',task_id)
            
            try:
                iceprod.core.exe.finishtask(stats)
            except:
                logger.error('running finishtask failed')
                raise
            if not finish_task.called:
                raise Exception('JSONRPC.finish_task() not called')
            if finish_task.task_id != task_id:
                raise Exception('JSONRPC.finish_task() task_id !=')
            if finish_task.stats != stats:
                raise Exception('JSONRPC.finish_task() stats !=')
        
        except Exception as e:
            logger.error('Error running finishtask test: %s',str(e))
            printer('Test exe.finishtask()',False)
            raise
        else:
            printer('Test exe.finishtask()')
    
    def test_93_stillrunning(self):
        """Test stillrunning"""
        try:
            # mock the JSONRPC class
            task_id = 'a task'
            def stillrunning(task_id):
                stillrunning.called = True
                stillrunning.task_id = task_id
                return stillrunning.ret
            stillrunning.called = False
            def f(*args,**kwargs):
                name = kwargs.pop('func_name')
                if 'callback' in kwargs:
                    cb = kwargs.pop('callback')
                else:
                    cb = None
                if name == 'stillrunning':
                    ret = stillrunning(*args,**kwargs)
                else:
                    ret = Exception()
                if cb:
                    cb(ret)
                else:
                    return ret
            jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
            jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))
            iceprod.core.exe.config.options['task_id'] = iceprod.core.dataclasses.Parameter('task_id',task_id)
            
            stillrunning.ret = True
            try:
                iceprod.core.exe.stillrunning()
            except:
                logger.error('exception when not supposed to')
                raise
            if not stillrunning.called:
                raise Exception('JSONRPC.stillrunning() not called')
            if stillrunning.task_id != task_id:
                raise Exception('JSONRPC.stillrunning() task_id !=')
                
            stillrunning.ret = False
            try:
                iceprod.core.exe.stillrunning()
            except:
                pass
            else:
                raise Exception('exception not thrown')
                raise
            if 'DBkill' not in iceprod.core.exe.config.options:
                raise Exception('DBkill not in config.options')
            
            stillrunning.ret = Exception('sql error')
            try:
                iceprod.core.exe.stillrunning()
            except:
                pass
            else:
                raise Exception('exception not thrown2')
                raise
        
        except Exception as e:
            logger.error('Error running stillrunning test: %s',str(e))
            printer('Test exe.stillrunning()',False)
            raise
        else:
            printer('Test exe.stillrunning()')
        finally:
            if 'DBkill' in iceprod.core.exe.config.options:
                del iceprod.core.exe.config.options['DBkill']
    
    def test_94_taskerror(self):
        """Test taskerror"""
        try:
            # mock the JSONRPC class
            task_id = 'a task'
            def task_error(task_id):
                task_error.called = True
                task_error.task_id = task_id
                return None
            task_error.called = False
            def f(*args,**kwargs):
                name = kwargs.pop('func_name')
                if 'callback' in kwargs:
                    cb = kwargs.pop('callback')
                else:
                    cb = None
                if name == 'task_error':
                    ret = task_error(*args,**kwargs)
                else:
                    ret = Exception()
                if cb:
                    cb(ret)
                else:
                    return ret
            jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
            jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(f,func_name=a))
            iceprod.core.exe.config.options['task_id'] = iceprod.core.dataclasses.Parameter('task_id',task_id)
            
            try:
                iceprod.core.exe.taskerror()
            except:
                logger.error('running taskerror failed')
                raise
            if not task_error.called:
                raise Exception('JSONRPC.task_error() not called')
            if task_error.task_id != task_id:
                raise Exception('JSONRPC.task_error() task_id !=')
        
        except Exception as e:
            logger.error('Error running taskerror test: %s',str(e))
            printer('Test exe.taskerror()',False)
            raise
        else:
            printer('Test exe.taskerror()')
    
    def test_95_uploadLogging(self):
        """Test uploading logfiles"""
        try:
            # mock the JSONRPC class
            task_id = 'a task'
            def uploader(task_id,name,data):
                uploader.called = True
                uploader.task_id = task_id
                uploader.data[name] = json_compressor.uncompress(data)
                return None
            def fun(*args,**kwargs):
                name = kwargs.pop('func_name')
                if 'callback' in kwargs:
                    cb = kwargs.pop('callback')
                else:
                    cb = None
                if name == 'upload_logfile':
                    ret = uploader(*args,**kwargs)
                else:
                    ret = Exception()
                if cb:
                    cb(ret)
                else:
                    return ret
            jsonrpc = flexmock(iceprod.core.jsonRPCclient.MetaJSONRPC)
            jsonrpc.should_receive('__getattr__').replace_with(lambda a:partial(fun,func_name=a))
            iceprod.core.exe.config.options['task_id'] = iceprod.core.dataclasses.Parameter('task_id',task_id)
            
            data = ''.join([str(random.randint(0,10000)) for _ in xrange(100)])
            
            filename = os.path.join(self.test_dir,str(random.randint(0,10000)))
            with open(filename,'w') as f:
                f.write(data)
            
            uploader.called = False
            uploader.task_id = None
            uploader.data = {}
            name = 'testing'
            try:
                iceprod.core.exe._upload_logfile(task_id,name,filename)
            except:
                logger.error('running _upload_logfile failed')
                raise
            if not uploader.called:
                raise Exception('JSONRPC._upload_logfile() not called')
            if uploader.task_id != task_id:
                raise Exception('JSONRPC._upload_logfile() task_id !=')
            if name not in uploader.data:
                raise Exception('JSONRPC._upload_logfile() invalid name: %r'%
                                uploader.data.keys())
            if uploader.data[name] != data:
                raise Exception('JSONRPC._upload_logfile() data !=')
            
            uploader.called = False
            uploader.task_id = None
            uploader.data = {}
            for f in constants.keys():
                if f in ('stderr','stdout','stdlog'):
                    with open(constants[f],'w') as f:
                        f.write(''.join([str(random.randint(0,10000))
                                         for _ in xrange(100)]))
            try:
                iceprod.core.exe.uploadLogging()
            except:
                logger.error('running uploadLogging failed')
                raise
            if not uploader.called:
                raise Exception('JSONRPC.uploadLogging() not called')
            if uploader.task_id != task_id:
                raise Exception('JSONRPC.uploadLogging() task_id !=')
            for name in ('stdlog','stderr','stdout'):
                if name not in uploader.data:
                    raise Exception('JSONRPC.uploadLogging(%s) invalid name: %r'%
                                    (name,uploader.data.keys()))
                if uploader.data[name] != open(constants[name]).read():
                    raise Exception('JSONRPC.uploadLogging(%s) data !='%name)
        
        except Exception as e:
            logger.error('Error running uploadLogging test: %s',str(e))
            printer('Test exe.uploadLogging()',False)
            raise
        else:
            printer('Test exe.uploadLogging()')



def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(exe_test))
    suite.addTests(loader.loadTestsFromNames(alltests,exe_test))
    return suite
