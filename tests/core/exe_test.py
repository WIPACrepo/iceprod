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
from functools import partial, reduce

try:
    import cPickle as pickle
except:
    import pickle


import unittest
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
from iceprod.core import to_log,constants
import iceprod.core.dataclasses
import iceprod.core.functions
import iceprod.core.exe
from iceprod.core.jsonUtil import json_encode,json_decode


class DownloadTestCase(unittest.TestCase):
    def setUp(self):
        super(DownloadTestCase,self).setUp()

        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        curdir = os.getcwd()
        os.symlink(os.path.join(curdir, 'iceprod'),
                   os.path.join(self.test_dir, 'iceprod'))
        os.chdir(self.test_dir)
        def cleanup():
            os.chdir(curdir)
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

    def mk_files(self, path, data, compress=None, ext=False):
        orig_path = path
        if not ext:
            path,ext = os.path.splitext(path)
            while ext:
                path,ext = os.path.splitext(path)
        if isinstance(data,dict):
            # make directory of things
            if not os.path.exists(path):
                os.mkdir(path)
                for k in data:
                    with open(os.path.join(path,k),'w' if isinstance(data[k],str) else 'wb') as f:
                        f.write(data[k])
        else:
            with open(path,'w' if isinstance(data,str) else 'wb') as f:
                f.write(data)
        if compress:
            new_path = iceprod.core.functions.compress(path,compress)
            if new_path != orig_path:
                os.rename(new_path, orig_path)
            if orig_path != path:
                iceprod.core.functions.removedirs(path)

    def make_shared_lib(self):
        """Make a shared library file used for testing"""
        so_file = os.path.join(self.test_dir,'hello')[len(os.getcwd()):]
        if so_file[0] == '/':
            so_file = so_file[1:]
        if os.path.exists(so_file+'.so'):            
            with open(so_file+'.so','rb') as f:
                return f.read()

        from distutils import sysconfig
        pythondir = sysconfig.get_python_inc()
        logger.info('pythondir: %s', pythondir)
        pythonver = os.path.basename(pythondir)

        with open(so_file+'.c','w') as f:
            f.write('#include <'+pythonver+"""/Python.h>

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

#if PY_MAJOR_VERSION >= 3
    static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "hello",     /* m_name */
        "Hello world",  /* m_doc */
        -1,                  /* m_size */
        HelloMethods,    /* m_methods */
        NULL,                /* m_reload */
        NULL,                /* m_traverse */
        NULL,                /* m_clear */
        NULL,                /* m_free */
    };
    PyMODINIT_FUNC
    PyInit_hello(void)
    {
         (void) PyModule_Create(&moduledef);
    }
#else
    PyMODINIT_FUNC
    inithello(void)
    {
         (void) Py_InitModule("hello", HelloMethods);
    }
#endif
""")
        from distutils.ccompiler import new_compiler
        c = new_compiler()
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
          
        with open(so_file+'.so','rb') as f:
            return f.read()

class exe_test(DownloadTestCase):
    def setUp(self):
        super(exe_test,self).setUp()

        # set offline mode
        self.config = iceprod.core.exe.Config()
        self.config.config['options']['offline'] = True

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadResource')
    def test_001_downloadResource(self, download):
        # create an environment
        options = {'resource_url': 'http://blah/downloads',
                   'resource_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Resource()
        r['remote'] = 'stuff'
        r['local'] = 'localstuff'

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, 'the data')
            return path
        download.side_effect = create

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('downloadResource did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('downloadResource did not write to the '
                            'expected filename of %s'%r['local'])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadResource - gz')
    def test_002_downloadResource(self, download):
        # create an environment
        options = {'resource_url': 'http://blah/downloads',
                   'resource_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Resource()
        r['remote'] = 'stuff2.gz'
        r['local'] = 'localstuff2.gz'
        r['compression'] = True

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, 'the data', compress='gz')
            return path
        download.side_effect = create

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('downloadResource did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'][:-3])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-3])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadResource - tar')
    def test_003_downloadResource(self, download):
        # create an environment
        options = {'resource_url': 'http://blah/downloads',
                   'resource_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Resource()
        r['remote'] = 'stuff3.tar'
        r['local'] = 'localstuff3.tar'
        r['compression'] = True

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, {'f':'the data'}, compress='tar')
            return path
        download.side_effect = create

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('downloadResource did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isdir(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadResource - tar.bz2')
    def test_004_downloadResource(self, download):
        # create an environment
        options = {'resource_url': 'http://blah/downloads',
                   'resource_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Resource()
        r['remote'] = 'stuff4.tar.bz2'
        r['local'] = 'localstuff4.tar.bz2'
        r['compression'] = True

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, {'f':'the data'}, compress='bz2')
            return path
        download.side_effect = create

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('downloadResource did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isdir(os.path.join(self.test_dir,r['local'][:-8])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-8])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadResource - tgz')
    def test_005_downloadResource(self, download):
        # create an environment
        options = {'resource_url': 'http://blah/downloads',
                   'resource_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Resource()
        r['remote'] = 'stuff5.tgz'
        r['local'] = 'localstuff5.tgz'
        r['compression'] = True

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, {'f':'the data'}, compress='gz')
            return path
        download.side_effect = create

        # try downloading the resource
        iceprod.core.exe.downloadResource(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('downloadResource did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isdir(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadResource - invalid env')
    def test_006_downloadResource(self, download):
        path = os.path.join(self.test_dir, 'localstuff5')
        self.mk_files(path, 'the data')
        download.return_value = path

        r = iceprod.core.dataclasses.Resource()
        r['remote'] = 'stuff5.tgz'
        r['local'] = 'localstuff5.tgz'
        r['compression'] = None
        with self.assertRaises(Exception):
            iceprod.core.exe.downloadResource({},r)

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadData')
    def test_010_downloadData(self, download):
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

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, 'the data')
            return path
        download.side_effect = create

        # try downloading the resource
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('downloadResource did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('downloadResource did not write to the '
                            'expected filename of %s'%r['local'])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadData - gz')
    def test_011_downloadData(self, download):
        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff2.gz'
        r['local'] = 'localstuff2.gz'
        r['compression'] = True
        r['type'] = 'permanent'
        r['movement'] = 'input'

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, 'the data', compress='gz')
            return path
        download.side_effect = create

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

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadData - tar')
    def test_012_downloadData(self, download):
        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff3.tar'
        r['local'] = 'localstuff3.tar'
        r['compression'] = True
        r['type'] = 'permanent'
        r['movement'] = 'input'

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, {'f':'the data'}, compress='tar')
            return path
        download.side_effect = create

        # try downloading the resource
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isdir(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadData - tar.bz2')
    def test_013_downloadData(self, download):
        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff4.tar.bz2'
        r['local'] = 'localstuff4.tar.bz2'
        r['compression'] = True
        r['type'] = 'permanent'
        r['movement'] = 'input'

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, {'f':'the data'}, compress='bz2')
            return path
        download.side_effect = create

        # try downloading the resource
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isdir(os.path.join(self.test_dir,r['local'][:-8])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-8])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadData - tgz')
    def test_014_downloadData(self, download):
        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff4.tgz'
        r['local'] = 'localstuff4.tgz'
        r['compression'] = True
        r['type'] = 'permanent'
        r['movement'] = 'input'

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(self.test_dir, r['local'])
            self.mk_files(path, {'f':'the data'}, compress='gz')
            return path
        download.side_effect = create

        # try downloading the resource
        iceprod.core.exe.downloadData(env,r)
        # check for record of file in env
        if r['local'] not in env['files']:
            raise Exception('did not add the file '
                            '%s to the env'%r['local'])
        if not os.path.isfile(os.path.join(self.test_dir,r['local'])):
            raise Exception('did not write to the '
                            'expected filename of %s'%r['local'])
        if not os.path.isdir(os.path.join(self.test_dir,r['local'][:-4])):
            raise Exception('did not uncompress to the '
                            'expected filename of %s'%r['local'][:-4])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='downloadData - invalid env')
    def test_015_downloadData(self, download):
        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff4'
        r['local'] = 'localstuff4'
        r['compression'] = None
        r['type'] = 'permanent'
        r['movement'] = 'input'

        # try supplying invalid env
        with self.assertRaises(Exception):
            iceprod.core.exe.downloadData({},r)

    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='uploadData')
    def test_020_uploadData(self, upload):
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

        # create the downloaded file
        path = os.path.join(self.test_dir, r['local'])
        self.mk_files(path, 'the data')

        # try uploading the data
        iceprod.core.exe.uploadData(env,r)
        self.assertTrue(upload.called)
        self.assertEqual(upload.call_args[0][0],
            os.path.join(options['data_directory'],r['local']))
        self.assertEqual(upload.call_args[0][1],
            os.path.join(options['data_url'],r['remote']))

    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='uploadData - gz')
    def test_021_uploadData(self, upload):
        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff2.gz'
        r['local'] = 'localstuff2'
        r['compression'] = True
        r['type'] = 'permanent'
        r['movement'] = 'both'

        # create the downloaded file
        path = os.path.join(self.test_dir, r['local'])
        self.mk_files(path, 'the data')

        # try uploading the data
        iceprod.core.exe.uploadData(env,r)
        self.assertTrue(upload.called)
        self.assertEqual(upload.call_args[0][0],
            os.path.join(options['data_directory'],r['local']+'.gz'))
        self.assertEqual(upload.call_args[0][1],
            os.path.join(options['data_url'],r['remote']))

    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='uploadData - tar')
    def test_022_uploadData(self, upload):
        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff3.tar'
        r['local'] = 'localstuff3'
        r['compression'] = True
        r['type'] = 'permanent'
        r['movement'] = 'both'

        # create the downloaded file
        path = os.path.join(self.test_dir, r['local'])
        self.mk_files(path, {'f':'the data'})

        # try uploading the data
        iceprod.core.exe.uploadData(env,r)
        self.assertTrue(upload.called)
        self.assertEqual(upload.call_args[0][0],
            os.path.join(options['data_directory'],r['local']+'.tar'))
        self.assertEqual(upload.call_args[0][1],
            os.path.join(options['data_url'],r['remote']))

    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='uploadData - tar.bz2')
    def test_023_uploadData(self, upload):
        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff4.tar.bz2'
        r['local'] = 'localstuff4'
        r['compression'] = True
        r['type'] = 'permanent'
        r['movement'] = 'both'

        # create the downloaded file
        path = os.path.join(self.test_dir, r['local'])
        self.mk_files(path, {'f':'the data'})

        # try uploading the data
        iceprod.core.exe.uploadData(env,r)
        self.assertTrue(upload.called)
        self.assertEqual(upload.call_args[0][0],
            os.path.join(options['data_directory'],r['local']+'.tar.bz2'))
        self.assertEqual(upload.call_args[0][1],
            os.path.join(options['data_url'],r['remote']))

    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='uploadData - tgz')
    def test_024_uploadData(self, upload):
        # create an environment
        options = {'data_url': 'http://blah/downloads',
                   'data_directory': self.test_dir}
        env = {'options':options}

        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff5.tgz'
        r['local'] = 'localstuff5'
        r['compression'] = True
        r['type'] = 'permanent'
        r['movement'] = 'both'

        # create the downloaded file
        path = os.path.join(self.test_dir, r['local'])
        self.mk_files(path, {'f':'the data'})

        # try uploading the data
        iceprod.core.exe.uploadData(env,r)
        self.assertTrue(upload.called)
        self.assertEqual(upload.call_args[0][0],
            os.path.join(options['data_directory'],r['local']+'.tgz'))
        self.assertEqual(upload.call_args[0][1],
            os.path.join(options['data_url'],r['remote']))

    @patch('iceprod.core.exe.functions.upload')
    @unittest_reporter(name='uploadData - invalid env')
    def test_025_uploadData(self, upload):
        # create a resource object
        r = iceprod.core.dataclasses.Data()
        r['remote'] = 'stuff5.tgz'
        r['local'] = 'localstuff5'
        r['type'] = 'permanent'
        r['movement'] = 'both'

        with self.assertRaises(Exception):
            iceprod.core.exe.uploadData({},r)

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='setupClass')
    def test_030_setupClass(self, download):
        # create an env
        env = {'options':{'local_temp':os.path.join(self.test_dir,'classes')}}
        os.mkdir(env['options']['local_temp'])

        # create a class object
        r = iceprod.core.dataclasses.Class()
        r['name'] = 'datatransfer.py'
        r['src'] = 'datatransfer.py'

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(env['options']['local_temp'], r['name'])
            self.mk_files(path, 'class GridFTP(): pass', ext=True)
            return path
        download.side_effect = create

        # try setting up the class
        iceprod.core.exe.setupClass(env,r)

        self.assertIn(r['name'], env['classes'])
        self.assertIn(os.path.dirname(env['classes'][r['name']]),
                      os.environ['PYTHONPATH'].split(':'))

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='setupClass - env $CLASS')
    def test_031_setupClass(self, download):
        # create an env
        env = {'options':{'local_temp':os.path.join(self.test_dir,'classes')}}
        os.mkdir(env['options']['local_temp'])

        # create a class object
        r = iceprod.core.dataclasses.Class()
        r['name'] = 'datatransfer.py'
        r['src'] = 'datatransfer.py'
        r['env_vars'] = 'I3_BUILD=$CLASS'

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(env['options']['local_temp'], r['name'])
            self.mk_files(path, 'class GridFTP(): pass', ext=True)
            return path
        download.side_effect = create

        # try setting up the class
        iceprod.core.exe.setupClass(env,r)

        self.assertIn(r['name'], env['classes'])
        self.assertIn(os.path.dirname(env['classes'][r['name']]),
                      os.environ['PYTHONPATH'].split(':'))
        self.assertIn('I3_BUILD', os.environ)
        self.assertEqual(os.environ['I3_BUILD'], env['classes'][r['name']])

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='setupClass - env overload')
    def test_032_setupClass(self, download):
        # create an env
        env = {'options':{'local_temp':os.path.join(self.test_dir,'classes')}}
        os.mkdir(env['options']['local_temp'])

        # create a class object
        r = iceprod.core.dataclasses.Class()
        r['name'] = 'datatransfer.py'
        r['src'] = 'datatransfer.py'
        r['env_vars'] = 'tester=1:2:3;PATH=$PWD;PYTHONPATH=$PWD/test'

        # create the downloaded file
        def create(*args,**kwargs):
            path = os.path.join(env['options']['local_temp'], r['name'])
            self.mk_files(path, 'class GridFTP(): pass', ext=True)
            return path
        download.side_effect = create

        # try setting up the class
        iceprod.core.exe.setupClass(env,r)

        self.assertIn(r['name'], env['classes'])
        self.assertIn(os.path.dirname(env['classes'][r['name']]),
                      os.environ['PYTHONPATH'].split(':'))
        self.assertIn('tester', os.environ)
        self.assertEqual(os.environ['tester'], '1:2:3')
        self.assertIn('PATH', os.environ)
        self.assertIn('$PWD', os.environ['PATH'].split(':'))
        self.assertIn('PYTHONPATH', os.environ)
        self.assertIn('$PWD/test', os.environ['PYTHONPATH'].split(':'))

    @unittest_reporter(name='setupenv - basic')
    def test_100_setupenv_basic(self):
        """Test basic setupenv functionality"""
        obj = iceprod.core.dataclasses.Steering()
        # create an empty env
        with iceprod.core.exe.setupenv(self.config, obj) as empty_env:
            # create secondary env
            with iceprod.core.exe.setupenv(self.config, obj, empty_env) as env2:
                # create something in env2, and check it's not in empty_env
                env2['test'] = 'testing'
                self.assertNotIn('test', empty_env, 'env2 is a direct link to empty_env')

                # make new env from env2, and check it has that value
                with iceprod.core.exe.setupenv(self.config, obj, env2) as env3:
                    self.assertIn('test', env3, 'env3 does not have test value')
                    self.assertEqual(env3['test'], 'testing', 'env3 does not have test value')

                    # check that modifying a value in env3 has no effect on env2
                    env3['test'] = 'abcd'
                    self.assertEqual(env2['test'], 'testing', 'env3 is a direct link to env2')

                    # check that modifying a value in env2 has no effect on env3
                    env2['test'] = 'dcba'
                    self.assertEqual(env3['test'], 'abcd', 'env2 is a direct link to env3')

                    # do second level checks, like dealing with parameters
                    obj.parameters = {}
                    with iceprod.core.exe.setupenv(self.config, obj) as env4:
                        with iceprod.core.exe.setupenv(self.config, obj, env4) as env5:
                            env5['parameters']['test'] = 1
                            self.assertNotIn('test', env4['parameters'],
                                'adding a parameter in env5 adds it to env4')
                            with iceprod.core.exe.setupenv(self.config, obj, env5) as env6:
                                env6['parameters']['test'] = 2
                                self.assertNotEqual(env5['parameters']['test'], 2,
                                    'modifying a parameter in env6 modifies it in env5')

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='setupenv - steering')
    def test_101_setupenv_steering(self, download):
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
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing resource directory
        options['resource_directory'] = os.path.join(self.test_dir,'resources')

        # set download() return value
        def create(*args,**kwargs):
            path = os.path.join(options['resource_directory'],r['local'])
            self.mk_files(path, {'f':'blah'}, compress='gz')
            return path
        download.side_effect = create

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

    @patch('iceprod.core.exe.functions.upload')
    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='destroyenv - steering')
    def test_102_destroyenv_steering(self, download, upload):
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
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        # set testing data directory
        options['data_directory'] = os.path.join(self.test_dir,'data')

        # set download() return value
        def create(*args,**kwargs):
            path = os.path.join(options['data_directory'],r['local'])
            self.mk_files(path, {'f':'blah'}, compress='gz')
            return path
        download.side_effect = create

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

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runmodule - iceprod module (from src)')
    def test_200_runmodule_iceprod_src(self, download):
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'
        module['running_class'] = 'Test'

        # create parameters
        module['parameters'] = {'greeting': 'new greeting'}

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(*args, **kwargs):
            path = os.path.join(options['local_temp'], module['src'])
            self.mk_files(path, """
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
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runmodule - iceprod module (clear env)')
    def test_201_runmodule_iceprod_env(self, download):
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'
        module['running_class'] = 'Test'
        module['env_clear'] = True

        # create parameters
        module['parameters'] = {'greeting': 'new greeting'}

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(*args, **kwargs):
            path = os.path.join(options['local_temp'], module['src'])
            self.mk_files(path, """
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
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runmodule - simple module from src')
    def test_210_runmodule_simple(self, download):
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'
        module['running_class'] = 'Test'

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(*args, **kwargs):
            path = os.path.join(options['local_temp'], module['src'])
            self.mk_files(path, """
def Test():
    return 'Tester'
""", ext=True)
            return path
        download.side_effect = create

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

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runmodule - python script')
    def test_211_runmodule_script(self, download):
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'

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

        def create(*args, **kwargs):
            path = os.path.join(options['local_temp'], module['src'])
            self.mk_files(path, """
def Test():
    return 'Tester'
if __name__ == '__main__':
    Test()
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runmodule - shell script')
    def test_212_runmodule_script(self, download):
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.sh'

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(*args, **kwargs):
            path = os.path.join(options['local_temp'], module['src'])
            self.mk_files(path, """
uname -a
echo "test"
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runmodule - python script (clear env)')
    def test_220_runmodule_script(self, download):
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['src'] = 'test.py'
        module['env_clear'] = True

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(*args, **kwargs):
            path = os.path.join(options['local_temp'], module['src'])
            self.mk_files(path, """
def Test():
    return 'Tester'
if __name__ == '__main__':
    Test()
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runmodule - python script (env_shell)')
    def test_221_runmodule_script(self, download):
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

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(*args, **kwargs):
            path = os.path.join(options['local_temp'], module['src'])
            self.mk_files(path, """
import os
def Test():
    if os.environ['foo'] != 'bar':
        raise Exception('bad env_shell')
if __name__ == '__main__':
    Test()
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runmodule - with linked libraries')
    def test_230_runmodule_icetray(self, download):
        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)

        # make .so file
        so = self.make_shared_lib()

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(url, *args, **kwargs):
            path = os.path.join(options['local_temp'], c['src'])
            self.mk_files(path, {'test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""", 'hello.so':so}, compress='gz')
            return path
        download.side_effect = create

        # set env
        env = {'options': options}

        # run the module
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runmodule(self.config, env, module)
            except:
                logger.error('running the module failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runtray')
    def test_300_runtray(self, download):
        """Test runtray"""
        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray.name = 'tray'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'test.Test'

        c = iceprod.core.dataclasses.Class()
        c['name'] = 'test'
        c['src'] = 'test.tar.gz'
        module['classes'].append(c)
        tray['modules'].append(module)

        # create another module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module2'
        module['running_class'] = 'Test'
        module['src'] = 'test2.py'
        tray['modules'].append(module)

        # make .so file
        so = self.make_shared_lib()

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(url, *args, **kwargs):
            if url.endswith(c['src']):
                path = os.path.join(options['local_temp'], c['src'])
                self.mk_files(path, {'test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""", 'hello.so':so}, compress='gz')
            else:
                path = os.path.join(options['local_temp'], module['src'])
                self.mk_files(path, """
def Test():
    return 'Tester2'
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options}

        # run the tray
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runtray(self.config, env, tray)
            except:
                logger.error('running the tray failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runtray - iterations')
    def test_310_runtray_iter(self, download):
        """Test runtray iterations"""
        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray['name'] = 'tray'
        tray['iterations'] = 3

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'test.Test'

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

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(url, *args, **kwargs):
            if url.endswith(c['src']):
                path = os.path.join(options['local_temp'], c['src'])
                self.mk_files(path, {'test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""", 'hello.so':so}, compress='gz')
            else:
                path = os.path.join(options['local_temp'], module['src'])
                self.mk_files(path, """
def Test():
    return 'Tester2'
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options}

        # run the tray
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runtray(self.config, env, tray)
            except:
                logger.error('running the tray failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runtask')
    def test_400_runtask(self, download):
        # create the task object
        task = iceprod.core.dataclasses.Task()
        task.name = 'task'

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray.name = 'tray'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'test.Test'

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

        # check that validate, resource_url, debug are in options
        options = {}
        if 'validate' not in options:
            options['validate'] = True
        if 'resource_url' not in options:
            options['resource_url'] = 'http://foo/'
        if 'debug' not in options:
            options['debug'] = False

        # make sure some basic options are set
        if 'data_url' not in options:
            options['data_url'] = 'gsiftp://gridftp/'
        if 'svn_repository' not in options:
            options['svn_repository'] = 'http://svn/'
        if 'job_temp' not in options:
            options['job_temp'] = os.path.join(self.test_dir,'job_temp')
        if 'local_temp' not in options:
            options['local_temp'] = os.path.join(self.test_dir,'local_temp')

        def create(url, *args, **kwargs):
            if url.endswith(c['src']):
                path = os.path.join(options['local_temp'], c['src'])
                self.mk_files(path, {'test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""", 'hello.so':so}, compress='gz')
            else:
                path = os.path.join(options['local_temp'], module['src'])
                self.mk_files(path, """
def Test():
    return 'Tester2'
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options,'stats':{'tasks':[]}}

        # run the tray
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runtask(self.config, env, task)
            except:
                logger.error('running the tray failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runtask - multiple trays')
    def test_410_runtask_multi(self, download):
        # create the task object
        task = iceprod.core.dataclasses.Task()
        task.name = 'task'

        # create the tray object
        tray = iceprod.core.dataclasses.Tray()
        tray.name = 'tray'

        # create the module object
        module = iceprod.core.dataclasses.Module()
        module['name'] = 'module'
        module['running_class'] = 'test.Test'

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
        module['running_class'] = 'test.Test'

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

        def create(url, *args, **kwargs):
            if url.endswith(c['src']):
                path = os.path.join(options['local_temp'], c['src'])
                self.mk_files(path, {'test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""", 'hello.so':so}, compress='gz')
            else:
                path = os.path.join(options['local_temp'], module['src'])
                self.mk_files(path, """
def Test():
    return 'Tester2'
""", ext=True)
            return path
        download.side_effect = create

        # set env
        env = {'options': options,'stats':{'tasks':[]}}

        # run the tray
        with to_log(sys.stdout,'stdout'),to_log(sys.stderr,'stderr'):
            try:
                iceprod.core.exe.runtask(self.config, env, task)
            except:
                logger.error('running the tray failed')
                raise

    @patch('iceprod.core.exe.functions.download')
    @unittest_reporter(name='runtask - multiple trays with iterations')
    def test_420_runtask_multi_iter(self, download):
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
        module['running_class'] = 'test.Test'

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
        module['running_class'] = 'test.Test'

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

        def create(url, *args, **kwargs):
            if url.endswith(c['src']):
                path = os.path.join(options['local_temp'], c['src'])
                self.mk_files(path, {'test.py':"""
import hello
def Test():
    return hello.say_hello('Tester')
""", 'hello.so':so}, compress='gz')
            else:
                path = os.path.join(options['local_temp'], module['src'])
                self.mk_files(path, """
def Test():
    return 'Tester2'
""", ext=True)
            return path
        download.side_effect = create

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
