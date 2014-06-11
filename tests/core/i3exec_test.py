"""
Test script for i3exec
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('i3exec')

import os, sys, time
import shutil
import tempfile
import random
import string
import subprocess
import threading

try:
    import unittest2 as unittest
except ImportError:
    import unittest

from iceprod.core import to_log
import iceprod.core.dataclasses
import iceprod.core.functions
import iceprod.core.serialization
import iceprod.core.logger
from iceprod.core import jsonUtil

# mock the logger so we don't overwrite the root logger
#from logging import getLogger as realLogger
#class fakeLogger:
#    def __init__(self):
#        self.handlers = []
#def log(*args,**kwargs):
#    if not args and not kwargs:
#        # return fake root logger
#        return fakeLogger()
#    else:
#        # passthrough
#        return realLogger(*args,**kwargs)
#logging.getLogger = log
#def log2(*args,**kwargs):
#    pass
#iceprod.core.logger.setlogger = log2
from iceprod.core import i3exec

from flexmock import flexmock


# a simple server for testing the external process 
def server(port,cb):
    import BaseHTTPServer
    import SocketServer
    class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-type", "text")
            self.end_headers()
        def do_GET(self):
            logging.warn('got GET request %s'%self.path)
            self.send_response(200)
            self.end_headers()
            ret = cb(self.path)
            self.wfile.write(ret)
            self.wfile.close()
        def do_POST(self):
            logging.warn('got POST request %s'%self.path)
            self.send_response(200)
            self.end_headers()
            input = None
            varLen = 0
            try:
                varLen = int(self.headers['Content-Length'])
            except Exception as e:
                logger.info('error getting content-length: %r',e)
                pass
            if varLen:
                try:
                    input = self.rfile.read(varLen)
                except Exception as e:
                    logger.info('error getting input: %r',e)
                    pass
            logger.info('input: %r',input)
            try:
                if input:
                    ret = cb(self.path,input=input)
                else:
                    ret = cb(self.path)
            except Exception as e:
                logger.error('Error running callback function: %r',e)
                ret = ''
            logger.info('ret: %r',ret)
            self.wfile.write(ret)
            self.wfile.close()
            
    httpd = SocketServer.TCPServer(("localhost", port), Handler)
    def run():
        with to_log(stream=sys.stderr,level='warn'),to_log(stream=sys.stdout):
            httpd.serve_forever()
    threading.Thread(target=run).start()
    time.sleep(1)
    logging.info('test server started at localhost:%d'%port)
    return httpd

def online_rpc(url,input=''):
    if not input:
        logger.info('no input for url %s',url)
        return ''
    
    data = jsonUtil.json_decode(input)
    logger.info('data: %r',data)
    out = {}
    if 'jsonrpc' not in data or float(data['jsonrpc']) < 2:
        online_rpc.error = 'jsonrpc tag error'
        out['error'] = online_rpc.error
    elif 'id' not in data:
        online_rpc.error = 'id error'
        out['error'] = online_rpc.error
    elif 'method' not in data:
        online_rpc.error = 'method error'
        out['error'] = online_rpc.error
    elif 'params' not in data:
        online_rpc.error = 'params error'
        out['error'] = online_rpc.error
    else:
        method = data['method']
        params = data['params']
        online_rpc.called_methods.append(method)
        
        if method == 'echo':
            if isinstance(params,dict):
                out['result'] = params['args'][0]
            elif isinstance(params,(list,tuple)):
                out['result'] = params[0]
            else:
                online_rpc.error = 'echo without proper args'
                out['error'] = online_rpc.error
        elif method == 'start':
            online_rpc.error = 'called start on server instead of client'
            out['error'] = online_rpc.error
        elif method == 'new_task':
            if online_rpc.called_methods.count('new_task') > 3:
                # only run 3 times
                out['result'] = None
            else:
                out['result'] = online_rpc.config
        elif method == 'set_processing':
            out['result'] = True
        elif method == 'stillrunning':
            out['result'] = True
        elif method == 'finish_task':
            out['result'] = True
        elif method == 'task_error':
            out['result'] = True
        elif method == 'upload_logfile':
            out['result'] = True
        else:
            online_rpc.error = 'called unknown method'
            out['error'] = online_rpc.error
    
    return jsonUtil.json_encode(out)


class i3exec_test(unittest.TestCase):
    def setUp(self):
        super(i3exec_test,self).setUp()
        
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
            
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
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(i3exec_test,self).tearDown()
    
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
    
    def make_config(self):
        # create basic config file
        config = iceprod.core.dataclasses.Job()
        config['options']['job_temp'] = os.path.join(self.test_dir,'job_temp')
        config['options']['local_temp'] = os.path.join(self.test_dir,'local_temp')
        config['options']['data_directory'] = os.path.join(self.test_dir,'data')
        config['options']['loglevel'] = 'info'
        config['steering'] = iceprod.core.dataclasses.Steering()
        return config
    
    def test_01(self):
        """Test basic i3exec functionality"""
        try:
            # create basic config file
            cfgfile = os.path.join(self.test_dir,'test_steering.json')
            config = self.make_config()
            task = iceprod.core.dataclasses.Task()
            task['name'] = 'task'
            config['tasks'].append(task)
            tray = iceprod.core.dataclasses.Tray()
            tray['name'] = 'tray'
            task['trays'].append(tray)
            mod = iceprod.core.dataclasses.Module()
            mod['name'] = 'mod'
            mod['running_class'] = 'MyTest'
            mod['src'] = 'mytest.py'
            tray['modules'].append(mod)
                
            # set download() return value
            def down():
                if self.download_args['url'].endswith('mytest.py'):
                    return """
from iceprod.modules.ipmodule import IPBaseClass
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 'Tester'
"""
            self.download_return = down
                
            # write configuration to file
            iceprod.core.serialization.serialize_json.dump(config,cfgfile)
            
            # set some default values
            validate = False
            url = 'http://x2100.icecube.wisc.edu/downloads'
            debug = False
            passkey = 'pass'
            offline = True
            
            # try to run the config
            try:
                i3exec.main(cfgfile,validate,url,debug,passkey,offline)
            except:
                raise
                
        except Exception, e:
            logger.error('Error running basic i3exec test: %s',str(e))
            printer('Test i3exec.main() basic',False)
            raise
        else:
            printer('Test i3exec.main() basic')
    
    def test_02(self):
        """Test not providing a steering file"""
        try:              
            # set some default values
            cfgfile = None
            validate = False
            url = 'http://x2100.icecube.wisc.edu/downloads'
            debug = True
            passkey = 'pass'
            offline = True
                
            # try to run the config
            try:
                i3exec.main(cfgfile,validate,url,debug,passkey,offline)
            except:
                pass
            else:
                raise Exception, 'Bad config did not raise an exception'
            
        except Exception, e:
            logger.error('Error running i3exec with bad config test: %s',str(e))
            printer('Test i3exec.main() with bad config',False)
            raise
        else:
            printer('Test i3exec.main() with bad config')
    
    def test_03(self):
        """Test debug mode"""
        try:
            # create basic config file
            cfgfile = os.path.join(self.test_dir,'test_steering.json')
            config = self.make_config()
            task = iceprod.core.dataclasses.Task()
            task['name'] = 'task'
            config['tasks'].append(task)
            tray = iceprod.core.dataclasses.Tray()
            tray['name'] = 'tray'
            task['trays'].append(tray)
            mod = iceprod.core.dataclasses.Module()
            mod['name'] = 'mod'
            mod['running_class'] = 'MyTest'
            mod['src'] = 'mytest.py'
            tray['modules'].append(mod)
                
            # set download() return value
            def down():
                if self.download_args['url'].endswith('mytest.py'):
                    return """
from iceprod.modules.ipmodule import IPBaseClass
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 'Tester'
"""
            self.download_return = down
                
            # write configuration to file
            iceprod.core.serialization.serialize_json.dump(config,cfgfile)
            
            # set some default values
            validate = False
            url = 'http://x2100.icecube.wisc.edu/downloads'
            debug = True
            passkey = 'pass'
            offline = True
            
            # try to run the config
            try:
                i3exec.main(cfgfile,validate,url,debug,passkey,offline)
            except:
                raise
                
        except Exception, e:
            logger.error('Error running i3exec debug test: %s',str(e))
            printer('Test i3exec.main() debug',False)
            raise
        else:
            printer('Test i3exec.main() debug')
    
    def test_10(self):
        """Test specifying tasks to run"""
        try:
            # create basic config file
            cfgfile = os.path.join(self.test_dir,'test_steering.json')
            config = self.make_config()
            task = iceprod.core.dataclasses.Task()
            task['name'] = 'task'
            config['tasks'].append(task)
            tray = iceprod.core.dataclasses.Tray()
            tray['name'] = 'tray'
            task['trays'].append(tray)
            mod = iceprod.core.dataclasses.Module()
            mod['name'] = 'mod'
            mod['running_class'] = 'MyTest'
            mod['src'] = 'mytest.py'
            tray['modules'].append(mod)
                
            # set download() return value
            def down():
                if self.download_args['url'].endswith('mytest.py'):
                    return """
from iceprod.modules.ipmodule import IPBaseClass
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 'Tester'
"""
            self.download_return = down
                
            # write configuration to file
            iceprod.core.serialization.serialize_json.dump(config,cfgfile)
            
            # set some default values
            validate = False
            url = 'http://x2100.icecube.wisc.edu/downloads'
            debug = False
            passkey = 'pass'
            offline = True
            
            # try to run the config
            try:
                i3exec.main(cfgfile,validate,url,debug,passkey,offline)
            except:
                raise
                
        except Exception, e:
            logger.error('Error running i3exec test: %s',str(e))
            printer('Test i3exec.main() specific task',False)
            raise
        else:
            printer('Test i3exec.main() specific task')
    
    def test_11(self):
        """Test multiple tasks"""
        try:
            # create basic config file
            cfgfile = os.path.join(self.test_dir,'test_steering.json')
            config = self.make_config()
                
            # create the task object
            task = iceprod.core.dataclasses.Task()
            task['name'] = 'task'
            
            config['tasks'].append(task)
            
            # create the tray object
            tray = iceprod.core.dataclasses.Tray()
            tray['name'] = 'tray'
            
            # create the module object
            module = iceprod.core.dataclasses.Module()
            module['name'] = 'module'
            module['running_class'] = 'iceprod_test.MyTest'
            
            c = iceprod.core.dataclasses.Class()
            c['name'] = 'test'
            c['src'] = 'test.tar.gz'
            module['classes'].append(c)
            tray['modules'].append(module)
            
            # create another module object
            module = iceprod.core.dataclasses.Module()
            module['name'] = 'module2'
            module['running_class'] = 'mytest.MyTest'
            module['src'] = 'mytest.py'
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
            module['running_class'] = 'iceprod_test.MyTest'
            
            c = iceprod.core.dataclasses.Class()
            c['name'] = 'test'
            c['src'] = 'test.tar.gz'
            module['classes'].append(c)
            tray['modules'].append(module)
            
            # create another module object
            module = iceprod.core.dataclasses.Module()
            module['name'] = 'module2'
            module['running_class'] = 'mytest.MyTest'
            module['src'] = 'mytest.py'
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
def MyTest():
    return hello.say_hello('Tester')
""",
                                    'hello.so':so}
                else:
                    return """
def MyTest():
    return 'Tester2'
"""
            self.download_return = dw
            
            # write configuration to file
            iceprod.core.serialization.serialize_json.dump(config,cfgfile)
            
            
            # set some default values
            validate = False
            url = 'http://x2100.icecube.wisc.edu/downloads'
            debug = False
            passkey = 'pass'
            offline = True
            
            # try to run the config
            try:
                i3exec.main(cfgfile,validate,url,debug,passkey,offline)
            except:
                raise
                
        except Exception, e:
            logger.error('Error running .so lib i3exec test: %s',str(e))
            printer('Test i3exec.main() .so lib',False)
            raise
        else:
            printer('Test i3exec.main() .so lib')
    
    
    def test_20(self):
        """Test failing task i3exec functionality"""
        try:
            # create basic config file
            cfgfile = os.path.join(self.test_dir,'test_steering.json')
            config = self.make_config()
            task = iceprod.core.dataclasses.Task()
            task['name'] = 'task'
            config['tasks'].append(task)
            tray = iceprod.core.dataclasses.Tray()
            tray['name'] = 'tray'
            task['trays'].append(tray)
            mod = iceprod.core.dataclasses.Module()
            mod['name'] = 'mod'
            mod['running_class'] = 'MyTest'
            mod['src'] = 'mytest.py'
            tray['modules'].append(mod)
                
            # set download() return value
            def down():
                if self.download_args['url'].endswith('mytest.py'):
                    return """
from iceprod.modules.ipmodule import IPBaseClass
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        raise Exception()
"""
            self.download_return = down
                
            # write configuration to file
            iceprod.core.serialization.serialize_json.dump(config,cfgfile)
            
            # set some default values
            validate = False
            url = 'http://x2100.icecube.wisc.edu/downloads'
            debug = False
            passkey = 'pass'
            offline = True
            
            # try to run the config
            try:
                i3exec.main(cfgfile,validate,url,debug,passkey,offline)
            except:
                pass
            else:
                raise Exception('failure was not detected')
                
        except Exception, e:
            logger.error('Error running failing task i3exec test: %s',str(e))
            printer('Test i3exec failing task',False)
            raise
        else:
            printer('Test i3exec failing task')
    
    
    def test_30(self):
        """Test online i3exec functionality"""
        try:
            # create basic config file
            cfgfile = os.path.join(self.test_dir,'test_steering.json')
            config = self.make_config()
            config['options']['task_id'] = 'task_id'
            task = iceprod.core.dataclasses.Task()
            task['name'] = 'task'
            config['tasks'].append(task)
            tray = iceprod.core.dataclasses.Tray()
            tray['name'] = 'tray'
            task['trays'].append(tray)
            mod = iceprod.core.dataclasses.Module()
            mod['name'] = 'mod'
            mod['running_class'] = 'MyTest'
            mod['src'] = 'mytest.py'
            tray['modules'].append(mod)
                
            # set download() return value
            def down():
                if self.download_args['url'].endswith('mytest.py'):
                    return """
from iceprod.modules.ipmodule import IPBaseClass
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 'Tester'
"""
            self.download_return = down
            
            # set up online server
            online_rpc.config = config
            online_rpc.error = None
            online_rpc.called_methods = []
            
            port = random.randint(16000,32000)
            http = server(port,online_rpc)
            
            try:
                # write configuration to file
                iceprod.core.serialization.serialize_json.dump(config,cfgfile)
                
                # set some default values
                validate = False
                url = 'http://localhost:%d'%port
                debug = False
                passkey = 'pass'
                offline = False
                
                # try to run the config
                try:
                    i3exec.main(cfgfile,validate,url,debug,passkey,offline)
                except:
                    raise
                if 'new_task' in online_rpc.called_methods:
                    raise Exception('tried to download a cfg, but was given '
                                    'one manually')
                if online_rpc.error:
                    raise Exception('error in online_rpc: %r',online_rpc.error)
            finally:
                http.shutdown()
                time.sleep(0.5)
                
        except Exception, e:
            logger.error('Error running online i3exec test: %s',str(e))
            printer('Test i3exec.main() online',False)
            raise
        else:
            printer('Test i3exec.main() online')
    
    def test_31(self):
        """Test online pilot i3exec functionality"""
        try:
            # create basic config file
            cfgfile = os.path.join(self.test_dir,'test_steering.json')
            config = self.make_config()
            task = iceprod.core.dataclasses.Task()
            task['name'] = 'task'
            config['tasks'].append(task)
            config['options']['task_id'] = 'task_id'
            tray = iceprod.core.dataclasses.Tray()
            tray['name'] = 'tray'
            task['trays'].append(tray)
            mod = iceprod.core.dataclasses.Module()
            mod['name'] = 'mod'
            mod['running_class'] = 'MyTest'
            mod['src'] = 'mytest.py'
            tray['modules'].append(mod)
                
            # set download() return value
            def down():
                if self.download_args['url'].endswith('mytest.py'):
                    return """
from iceprod.modules.ipmodule import IPBaseClass
class MyTest(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return 'Tester'
"""
            self.download_return = down
            
            # set up online server
            online_rpc.config = config
            online_rpc.error = None
            online_rpc.called_methods = []
            
            port = random.randint(16000,32000)
            http = server(port,online_rpc)
            
            try:
                # set some default values
                validate = False
                url = 'http://localhost:%d'%port
                debug = False
                passkey = 'pass'
                offline = False
                gridspec = 'testgrid'
                
                # try to run the config
                try:
                    i3exec.main(None,validate,url,debug,passkey,offline,
                                gridspec=gridspec)
                except:
                    raise
                if 'new_task' not in online_rpc.called_methods:
                    raise Exception('did not try to download a cfg')
                if 'finish_task' not in online_rpc.called_methods:
                    raise Exception('did not try to finish task')
                if online_rpc.error:
                    raise Exception('error in online_rpc: %r',online_rpc.error)
            finally:
                http.shutdown()
                time.sleep(0.5)
                
        except Exception, e:
            logger.error('Error running online pilot i3exec test: %s',str(e))
            printer('Test i3exec.main() online pilot',False)
            raise
        else:
            printer('Test i3exec.main() online pilot')
    
    
    def test_90(self):
        """Test calling externally"""
        try:
            # create basic config file
            cfgfile = os.path.join(self.test_dir,'test_steering.json')
            config = self.make_config()
            task = iceprod.core.dataclasses.Task()
            task['name'] = 'task'
            config['tasks'].append(task)
            tray = iceprod.core.dataclasses.Tray()
            tray['name'] = 'tray'
            task['trays'].append(tray)
            mod = iceprod.core.dataclasses.Module()
            mod['name'] = 'mod'
            mod['running_class'] = 'Test'
            mod['src'] = 'test.py'
            tray['modules'].append(mod)
                
            # set download() return value
            def down(url):
                if url.endswith('test.py'):
                    return """
from iceprod.modules.ipmodule import IPBaseClass
class Test(IPBaseClass):
    def __init__(self):
        IPBaseClass.__init__(self)
    def Execute(self,stats):
        return True
"""
            port = random.randint(16000,32000)
            http = server(port,down)
            
            try:
                # write configuration to file
                iceprod.core.serialization.serialize_json.dump(config,cfgfile)
                
                # set some default values
                validate = False
                url = 'http://localhost:%d'%port
                debug = False
                passkey = 'pass'
                offline = True
                
                # try to run the config
                if subprocess.call('coverage run -p bin/i3exec.py '
                                   '--cfgfile=%s --validate=%s --url=%s '
                                   '--passkey=%s --offline'%(
                                       cfgfile, str(validate), url, passkey),
                                   shell=True):
                    raise Exception('Error returned')
            
            finally:
                http.shutdown()
                time.sleep(0.5)
                
        except Exception, e:
            logger.error('Error running external i3exec test: %s',str(e))
            printer('Test i3exec externally',False)
            raise
        else:
            printer('Test i3exec externally')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(i3exec_test))
    suite.addTests(loader.loadTestsFromNames(alltests,i3exec_test))
    return suite