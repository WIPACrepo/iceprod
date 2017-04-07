"""
Test script for core exe
"""

from __future__ import absolute_import, division, print_function

from tests.util import unittest_reporter, glob_tests

import logging
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


import unittest
from iceprod.core import to_log,constants
import iceprod.core.dataclasses
import iceprod.core.functions
import iceprod.core.exe
from iceprod.core.jsonUtil import json_encode,json_decode

from flexmock import flexmock


class exe_test(unittest.TestCase):
    def setUp(self):
        super(exe_test,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        curdir = os.getcwd()
        os.symlink(os.path.join(curdir, 'iceprod'),
                   os.path.join(self.test_dir, 'iceprod'))
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

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
        self.config = iceprod.core.exe.Config()
        self.config.config['options']['offline'] = True

    def download(self,url,local,cache=False,options={}):
        """mocked iceprod.functions.download"""
        logger.info('mock download: %r %r', url, local)
        self.download_called = True
        self.download_args = {'url':url,'local':local,'cache':cache,
                              'options':options}
        if callable(self.download_return):
            data = self.download_return()
        elif self.download_return:
            data = self.download_return
        else:
            return False
        if os.path.isdir(local):
            local = os.path.join(local,os.path.basename(url))
        # remove tar or compress file extensions
        suffixes = ('.tar','.tgz','.gz','.tbz2','.tbz','.bz2','.bz',
                    '.lzma2','.lzma','.lz','.xz')
        local2 = reduce(lambda a,b:a.replace(b,''),suffixes,local)
        if isinstance(data,dict):
            # make directory of things
            if not os.path.exists(local2):
                os.mkdir(local2)
                for k in data:
                    with open(os.path.join(local2,k),'w') as f:
                        f.write(data[k])
        else:
            with open(local2,'w') as f:
                f.write(data)
        if (iceprod.core.functions.iscompressed(url) or
            iceprod.core.functions.istarred(url)):
            if '.tar.' in local:
                c = '.'.join(local.rsplit('.',2)[-2:])
            else:
                c = local.rsplit('.',1)[-1]
            output = iceprod.core.functions.compress(local2, c)
        if os.path.exists(local):
            return local
        else:
            raise Exception('Something went wrong when mocking download')

    def upload(self,local,remote,options={}):
        """mocked iceprod.functions.upload"""
        logger.info('mock upload: %r %r', local, remote)
        self.upload_called = True
        self.upload_args = {'local':local,'remote':remote,
                            'options':options}
        suffixes = ('.tar','.tgz','.gz','.tbz2','.tbz','.bz2','.bz',
                    '.lzma2','.lzma','.lz','.xz')
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
            f.write("""#include <python2.7/Python.h>

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


    @unittest_reporter
    def test_01_downloadResource(self):
        """Test downloading a resource"""
        # set download() return value
        self.download_return = 'the data'

        # create an environment
        options = {'resource_url': 'http://blah/downloads',
                   'resource_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Resource()
        r['remote'] = 'stuff'
        r['local'] = 'localstuff'

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('downloadResource did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('downloadResource did not write to the '
                            'expected filename of %s'%r['local'])

        # try a compressed object
        r['remote'] = 'stuff2.gz'
        r['local'] = 'localstuff2.gz'
        r['compression'] = True

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-3])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-3])

        # try a tarred object
        r['remote'] = 'stuff3.tar'
        r['local'] = 'localstuff3.tar'
        r['compression'] = True

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])

        # try a tarred compressed object
        r['remote'] = 'stuff4.tar.bz2'
        r['local'] = 'localstuff4.tar.bz2'
        r['compression'] = True

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-8])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-8])

        # try a tarred compressed object
        r['remote'] = 'stuff5.tgz'
        r['local'] = 'localstuff5.tgz'
        r['compression'] = True

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])

        # try supplying invalid env
        r['compression'] = None
        try:
            iceprod.core.exe.downloadResource({},r)
        except Exception:
            pass
        else:
            # was supposed to throw an exception
            raise Exception('failed to throw exception on blank env')

    @unittest_reporter
    def test_02_downloadData(self):
        """Test downloading a data file"""
        # set download() return value
        self.download_return = 'the data'

        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff'
        r['local'] = 'localstuff'
        r['type'] = 'permanent'
        r['movement'] = 'input'

        # try downloading the resource
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('downloadResource did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('downloadResource did not write to the '
                            'expected filename of %s'%r['local'])

        # try a compressed object
        r['remote'] = 'stuff2.gz'
        r['local'] = 'localstuff2.gz'
        r['compression'] = True

        # try downloading the resource
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-3])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-3])

        # try a tarred object
        r['remote'] = 'stuff3.tar'
        r['local'] = 'localstuff3.tar'
        r['compression'] = True

        # try downloading the resource
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])

        # try a tarred compressed object
        r['remote'] = 'stuff4.tar.bz2'
        r['local'] = 'localstuff4.tar.bz2'
        r['compression'] = True

        # try downloading the resource
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-8])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-8])

        # try a tarred compressed object
        r['remote'] = 'stuff5.tgz'
        r['local'] = 'localstuff5.tgz'
        r['compression'] = True

        # try downloading the resource
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])

        # try supplying invalid env
        r['compression'] = None
        try:
            iceprod.core.exe.downloadData({},r)
        except Exception:
            pass
        else:
            # was supposed to throw an exception
            raise Exception('failed to throw exception on blank env')

    @unittest_reporter
    def test_03_uploadData(self):
        """Test uploading a data file"""
        # set download() return value
        self.download_return = 'the data'

        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff'
        r['local'] = 'localstuff'
        r['type'] = 'permanent'
        r['movement'] = 'both'

        # try downloading the data
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('downloadResource did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('downloadResource did not write to the '
                            'expected filename of %s'%r['local'])
        # try uploading the data
        self.upload_return = 'a different value'
        iceprod.core.exe.uploadData(env,r)
        if ((not self.upload_return) or
            self.upload_return != self.download_return):
            raise Exception('upload failed for regular data')

        # try a compressed object
        r['remote'] = 'stuff2.gz'
        r['local'] = 'localstuff2.gz'
        r['compression'] = True

        # try downloading the data
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-3])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-3])
        # try uploading the data
        self.upload_return = 'a different value'
        iceprod.core.exe.uploadData(env,r)
        if ((not self.upload_return) or
            self.upload_return != self.download_return):
            raise Exception('upload failed for compressed data')

        # try a tarred object
        r['remote'] = 'stuff3.tar'
        r['local'] = 'localstuff3.tar'
        r['compression'] = True

        # try downloading the data
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])
        # try uploading the data
        self.upload_return = 'a different value'
        iceprod.core.exe.uploadData(env,r)
        if ((not self.upload_return) or
            self.upload_return != self.download_return):
            raise Exception('upload failed for tarred data')

        # try a tarred compressed object
        r['remote'] = 'stuff4.tar.bz2'
        r['local'] = 'localstuff4.tar.bz2'
        r['compression'] = True

        # try downloading the data
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-8])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-8])
        # try uploading the data
        self.upload_return = 'a different value'
        iceprod.core.exe.uploadData(env,r)
        if ((not self.upload_return) or
            self.upload_return != self.download_return):
            raise Exception('upload failed for tarred compressed data')

        # try a tarred compressed object
        r['remote'] = 'stuff5.tgz'
        r['local'] = 'localstuff5.tgz'
        r['compression'] = True

        # try downloading the data
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])
        # try uploading the data
        self.upload_return = 'a different value'
        iceprod.core.exe.uploadData(env,r)
        if ((not self.upload_return) or
            self.upload_return != self.download_return):
            raise Exception('upload failed for tarred compressed ' +
                            'data (tgz)')

        # try supplying invalid env
        r['compression'] = None
        try:
            iceprod.core.exe.uploadData({},r)
        except Exception:
            pass
        else:
            # was supposed to throw an exception
            raise Exception('failed to throw exception on blank env')

    @unittest_reporter
    def test_04_setupClass(self):
        """Test setting up a class"""
        # set download() return value
        self.download_return = 'class GridFTP(): pass'

        # create a class object
        r = iceprod.core.dataclasses.Class()
        r['name'] = 'datatransfer.py'
        r['src'] = 'datatransfer.py'

        # create an env
        env = {'options':{'local_temp':os.path.join(self.test_dir,'classes')}}

        # try setting up the class
        try:
            iceprod.core.exe.setupClass(env,r)
        except:
            logger.error('exe.setupClass() error')
            raise

        self.assertIn(r['name'], env['classes'])
        self.assertIn(os.path.dirname(env['classes'][r['name']]),
                      os.environ['PYTHONPATH'].split(':'))

    @unittest_reporter(name='setupClass() with env')
    def test_05_setupClass_Env(self):
        """Test setting up a class with an environment"""
        # set download() return value
        self.download_return = 'class GridFTP(): pass'

        # create a class object
        r = iceprod.core.dataclasses.Class()
        r['name'] = 'datatransfer.py'
        r['src'] = 'datatransfer.py'
        r['env_vars'] = 'I3_BUILD=$CLASS'

        # create an env
        env = {'options':{'local_temp':os.path.join(self.test_dir,'classes')}}

        # try setting up the class
        try:
            iceprod.core.exe.setupClass(env,r)
        except:
            logger.error('exe.setupClass() error')
            raise

        self.assertIn(r['name'], env['classes'])
        self.assertIn(os.path.dirname(env['classes'][r['name']]),
                      os.environ['PYTHONPATH'].split(':'))
        # test env
        if 'I3_BUILD' not in os.environ or os.environ['I3_BUILD'] == '$CLASS':
            raise Exception('I3_BUILD not in environment')


        # create a class object
        r = iceprod.core.dataclasses.Class()
        r['name'] = 'datatransfer.py'
        r['src'] = 'datatransfer.py'
        r['env_vars'] = 'tester=1:2:3;PATH=$PWD;PYTHONPATH=$PWD/test'

        # create an env
        env = {'options':{'local_temp':os.path.join(self.test_dir,'classes')}}

        # try setting up the class
        try:
            iceprod.core.exe.setupClass(env,r)
        except:
            logger.error('exe.setupClass() error')
            raise

        self.assertIn(r['name'], env['classes'])
        self.assertIn(os.path.dirname(env['classes'][r['name']]),
                      os.environ['PYTHONPATH'].split(':'))
        # test env
        if 'tester' not in os.environ or os.environ['tester'] != '1:2:3':
            raise Exception('tester not in environment')
        if 'PATH' not in os.environ or '$PWD' not in os.environ['PATH']:
            raise Exception('PATH not in environment')
        if ('PATH' not in os.environ or
            '$PWD/test' not in os.environ['PYTHONPATH']):
            raise Exception('PYTHONPATH not in environment')

    @unittest_reporter(name='setupenv(): basic')
    def test_10_setupenv_basic(self):
        """Test basic setupenv functionality"""
        obj = iceprod.core.dataclasses.Steering()
        # create an empty env
        with iceprod.core.exe.setupenv(self.config, obj) as empty_env:
            # create secondary env
            with iceprod.core.exe.setupenv(self.config, obj, empty_env) as env2:
                # create something in env2, and check it's not in empty_env
                env2['test'] = 'testing'
                if 'test' in empty_env:
                    raise Exception('env2 is a direct link to empty_env')

                # make new env from env2, and check it has that value
                with iceprod.core.exe.setupenv(self.config, obj, env2) as env3:
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
                    with iceprod.core.exe.setupenv(self.config, obj) as env4:
                        with iceprod.core.exe.setupenv(self.config, obj, env4) as env5:
                            env5['parameters']['test'] = 1
                            if 'test' in env4['parameters']:
                                raise Exception('adding a parameter in env5 adds it to env4')
                            with iceprod.core.exe.setupenv(self.config, obj, env5) as env6:
                                env6['parameters']['test'] = 2
                                if env5['parameters']['test'] == 2:
                                    raise Exception('modifying a parameter in env6 modifies ' +
                                                    'it in env5')

    @unittest_reporter(name='setupenv(): steering')
    def test_11_setupenv_steering(self):
        """Test setupenv with steering object"""
        # create the steering object
        steering = iceprod.core.dataclasses.Steering()

        # create a resource object
        r = iceprod.core.dataclasses.Resource()
        r['remote'] = 'globus.tar.gz'
        r['local'] = 'globus.tar.gz'
        steering['resources'].append(r)

        # create some parameters
        steering['parameters'] = {'test_param':'value'}

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing resource directory
        options['resource_directory'] = os.path.join(self.test_dir,'resources')

        # set download() return value
        self.download_return = 'the data'

        # create the env
        with iceprod.core.exe.setupenv(self.config, steering,
                                            {'options':options}) as env:

            # test parameters
            for p in steering['parameters']:
                if p not in env['parameters']:
                    raise Exception('Parameters were not applied ' +
                                    'correctly: missing %r'%p)

            # test options
            for p in options:
                if p not in env['options']:
                    raise Exception('Options were not applied ' +
                                    'correctly: missing %r'%p)

            # test resource
            if r['local'] not in env['files']:
                raise Exception('downloadResource did not add the file ' +
                                '%s to the env'%r['local'])
            if (env['files'][r['local']] !=
                os.path.join(self.test_dir,'resources',r['local'])):
                raise Exception('downloadResource did not return the ' +
                                'expected filename of %s' %
                                os.path.join(self.test_dir,'resources',
                                             r['local']))
            if not os.path.isfile(env['files'][r['local']]):
                raise Exception('downloadResource did not write to the ' +
                                'expected filename of %s' %
                                env['files'][r['local']])

    @unittest_reporter(name='destroyenv(): steering')
    def test_12_destroyenv_steering(self):
        """Test destroyenv with steering object"""
        # create the steering object
        steering = iceprod.core.dataclasses.Steering()

        # create a data object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'globus.tar.gz'
        r['local'] = 'globus.tar.gz'
        r['type'] = 'permanent'
        r['movement'] = 'both'
        steering['data'].append(r)

        # create parameters
        steering['parameters'] = {'test_param':'value'}

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set download() return value
        self.download_return = 'the data'

        

        # try a file deletion
        filename = os.path.join(self.test_dir,'test_file')
        with open(filename,'w') as f:
            f.write('this is a test')

        # create the env
        with iceprod.core.exe.setupenv(self.config, steering, {'options':options}) as env:
            env['deletions'] = [filename]

        if os.path.exists(filename):
            raise Exception('failed to delete file')

        # try environment reset

        # create the env
        with iceprod.core.exe.setupenv(self.config, steering,
                                            {'options':options,
                                             'deletions':[filename]}) as env:
            os.environ['MyTestVar'] = 'testing'

        if 'MyTestVar' in os.environ:
            raise Exception('failed to delete environment entry')

        if os.path.exists(filename):
            raise Exception('failed to delete file')

    @unittest_reporter(name='runmodule(): iceprod module (from src)')
    def test_22_runmodule_iceprod_src(self):
        """Test runmodule with iceprod module and src"""
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'
        module['running_class'] = 'Test'

        # set download() return value
        def down():
            if self.download_args['url'].endswith('test.py'):
                return """
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class Test(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 0
"""
        self.download_return = down

        # create parameters
        module['parameters'] = {'greeting': 'new greeting'}

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @unittest_reporter(name='runmodule(): iceprod module (clear env)')
    def test_23_runmodule_iceprod_env(self):
        """Test runmodule with iceprod module and src, clearing env"""
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'
        module['running_class'] = 'Test'
        module['env_clear'] = True

        # set download() return value
        def down():
            if self.download_args['url'].endswith('test.py'):
                return """
class IPBaseClass:
    def __init__(self):
        self.params = {}
    def AddParameter(self,p,h,d):
        self.params[p] = d
    def GetParameter(self,p):
        return self.params[p]
    def SetParameter(self,p,v):
        self.params[p] = v
class Test(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 0
"""
        self.download_return = down

        # create parameters
        module['parameters'] = {'greeting': 'new greeting'}

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @unittest_reporter(name='runmodule(): simple module from src')
    def test_30_runmodule_simple(self):
        """Test runmodule with simple script"""
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'
        module['running_class'] = 'Test'

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp-rr.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

        # try with short form of class
        module['running_class'] = 'Test'

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed (short)')
                raise

    @unittest_reporter(name='runmodule(): python script')
    def test_31_runmodule_script(self):
        """Test runmodule with raw python script"""
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @unittest_reporter(name='runmodule(): shell script')
    def test_32_runmodule_script(self):
        """Test runmodule with raw bash script"""
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.sh'

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @unittest_reporter(name='runmodule(): python script (clear env)')
    def test_33_runmodule_script(self):
        """Test runmodule with raw python script, clearing env"""
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'
        module['env_clear'] = True

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @unittest_reporter(name='runmodule(): python script (env_shell)')
    def test_34_runmodule_script(self):
        """Test runmodule with raw python script, with env_shell"""
        # create env_shell
        env_shell = os.path.join(self.test_dir,'env_shell.sh')
        with open(env_shell,'w') as f:
            f.write('#!/bin/sh\nfoo=bar $@\n')
        os.chmod(env_shell, 0o777)
        
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'
        module['env_shell'] = env_shell

        # set download() return value
        def down():
            if self.download_args['url'].endswith('test.py'):
                return """
import os
def Test():
    if os.environ['foo'] != 'bar':
        raise Exception('bad env_shell')
if __name__ == '__main__':
    Test()
"""
        self.download_return = down

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @unittest_reporter(name='runmodule(): with linked libraries')
    def test_40_runmodule_icetray(self):
        """Test runmodule with linked libraries"""
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'iceprod_test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @unittest_reporter
    def test_50_runtray(self):
        """Test runtray"""
        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray.name = 'tray'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'iceprod_test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'test.py'
        tray['modules'].append(module)

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the tray
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runtray(self.config, env, tray)
            except:
                logger.error('running the tray failed')
                raise

    @unittest_reporter(name='runtray(): iterations')
    def test_51_runtray_iter(self):
        """Test runtray iterations"""
        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        tray['iterations'] = 3

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'iceprod_test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'test.py'
        tray['modules'].append(module)

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options}

        # run the tray
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runtray(self.config, env, tray)
            except:
                logger.error('running the tray failed')
                raise

    @unittest_reporter
    def test_60_runtask(self):
        """Test runtask"""
        # create the task object
        task = iceprod.core.dataclasses.Task()
        task.name = 'task'

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray.name = 'tray'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'iceprod_test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'test.py'
        tray['modules'].append(module)

        # add tray to task
        task['trays'].append(tray)

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options,'stats':{'tasks':[]}}

        # run the tray
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runtask(self.config, env, task)
            except:
                logger.error('running the tray failed')
                raise

    @unittest_reporter(name='runtask(): multiple trays')
    def test_61_runtask_multi(self):
        """Test runtask with multiple trays"""
        # create the task object
        task = iceprod.core.dataclasses.Task()
        task.name = 'task'

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray.name = 'tray'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'iceprod_test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'test.py'
        tray['modules'].append(module)

        # add tray to task
        task['trays'].append(tray)

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray2'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'iceprod_test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'test.py'
        tray['modules'].append(module)

        # add tray to task
        task['trays'].append(tray)

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options,'stats':{'tasks':[]}}

        # run the tray
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runtask(self.config, env, task)
            except:
                logger.error('running the tray failed')
                raise

    @unittest_reporter(name='runtask(): multiple trays with iterations')
    def test_62_runtask_multi_iter(self):
        """Test runtask with multiple trays and iterations"""
        # create the task object
        task = iceprod.core.dataclasses.Task()
        task.name = 'task'

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray.name = 'tray'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'iceprod_test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'test.py'
        tray['modules'].append(module)

        # add tray to task
        task['trays'].append(tray)

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray2'
        tray['iterations'] = 3

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'iceprod_test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'test.py'
        tray['modules'].append(module)

        # add tray to task
        task['trays'].append(tray)

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
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://x2100.icecube.wisc.edu/downloads'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp.icecube.wisc.edu/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://code.icecube.wisc.edu/svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set env
        env = {'options': options,'stats':{'tasks':[]}}

        # run the tray
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runtask(self.config, env, task)
            except:
                logger.error('running the tray failed')
                raise


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(exe_test))
    suite.addTests(loader.loadTestsFromNames(alltests,exe_test))
    return suite
